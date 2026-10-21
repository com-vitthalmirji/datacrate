//! One owner for byte reservations and budgeted count/rendezvous waits.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use super::{CancellationToken, PipelineError, PipelineIoError};
use arrow::array::RecordBatch;

#[derive(Debug)]
struct State {
    queue: VecDeque<Result<BatchLease, PipelineIoError>>,
    bytes: usize,
    peak: usize,
    receiver_waiting: bool,
    receiver_alive: bool,
    sender_alive: bool,
    cancelled: bool,
    #[cfg(test)]
    sender_waiting: bool,
}

#[derive(Debug)]
pub(super) struct Control {
    state: Mutex<State>,
    changed: Condvar,
    capacity: usize,
    limit: usize,
}

impl Control {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(super) fn cancel(&self) {
        let queued = {
            let mut state = self.lock();
            state.cancelled = true;
            let queued = std::mem::take(&mut state.queue);
            self.changed.notify_all();
            queued
        };
        // Lease destruction acquires this control again; never drop it under its lock.
        drop(queued);
    }

    pub(super) fn metrics(&self) -> (usize, usize) {
        let state = self.lock();
        (state.bytes, state.peak)
    }
}

pub(super) fn channel(
    capacity: usize,
    limit: usize,
    token: &CancellationToken,
) -> (Sender, Receiver, Arc<Control>) {
    let control = Arc::new(Control {
        state: Mutex::new(State {
            queue: VecDeque::new(),
            bytes: 0,
            peak: 0,
            receiver_waiting: false,
            receiver_alive: true,
            sender_alive: true,
            cancelled: false,
            #[cfg(test)]
            sender_waiting: false,
        }),
        changed: Condvar::new(),
        capacity,
        limit,
    });
    token.register(&control);
    (
        Sender(Arc::clone(&control)),
        Receiver(Arc::clone(&control)),
        control,
    )
}

pub(super) fn checked_charge(
    sizes: impl IntoIterator<Item = usize>,
) -> Result<usize, PipelineError> {
    sizes.into_iter().try_fold(0usize, |sum, next| {
        sum.checked_add(next)
            .ok_or(PipelineError::ByteChargeOverflow)
    })
}

fn charge(batch: &RecordBatch) -> Result<usize, PipelineError> {
    checked_charge(
        batch
            .columns()
            .iter()
            .map(|array| array.get_array_memory_size()),
    )
}

pub(super) struct Sender(Arc<Control>);
pub(super) struct Receiver(Arc<Control>);

impl Sender {
    pub(super) fn send(
        &self,
        batch: Result<RecordBatch, PipelineIoError>,
    ) -> Result<(), PipelineError> {
        let bytes = match &batch {
            Ok(batch) => charge(batch)?,
            Err(_) => 0,
        };
        if bytes > self.0.limit {
            return Err(PipelineError::BatchTooLarge {
                bytes,
                limit: self.0.limit,
            });
        }
        let mut state = self.0.lock();
        loop {
            if state.cancelled || !state.receiver_alive {
                return Err(PipelineError::Cancelled);
            }
            let count_available = if self.0.capacity == 0 {
                state.receiver_waiting && state.queue.is_empty()
            } else {
                state.queue.len() < self.0.capacity
            };
            if count_available && bytes <= self.0.limit - state.bytes {
                break;
            }
            #[cfg(test)]
            {
                state.sender_waiting = true;
                self.0.changed.notify_all();
            }
            state = self
                .0
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            #[cfg(test)]
            {
                state.sender_waiting = false;
            }
        }
        state.bytes += bytes;
        state.peak = state.peak.max(state.bytes);
        let message = batch.map(|batch| BatchLease {
            batch,
            reservation: Reservation(Some((Arc::clone(&self.0), bytes))),
        });
        state.queue.push_back(message);
        self.0.changed.notify_all();
        // An empty rendezvous slot alone is not an acknowledgment of delivery.
        while self.0.capacity == 0
            && !state.queue.is_empty()
            && !state.cancelled
            && state.receiver_alive
        {
            #[cfg(test)]
            {
                state.sender_waiting = true;
                self.0.changed.notify_all();
            }
            state = self
                .0
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            #[cfg(test)]
            {
                state.sender_waiting = false;
            }
        }
        if state.cancelled || !state.receiver_alive {
            Err(PipelineError::Cancelled)
        } else {
            Ok(())
        }
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        state.sender_alive = false;
        self.0.changed.notify_all();
    }
}

impl Receiver {
    pub(super) fn recv(&self) -> Result<Option<BatchLease>, PipelineError> {
        let mut state = self.0.lock();
        loop {
            if state.cancelled {
                return Err(PipelineError::Cancelled);
            }
            if let Some(message) = state.queue.pop_front() {
                state.receiver_waiting = false;
                self.0.changed.notify_all();
                return message.map(Some).map_err(PipelineError::Io);
            }
            if !state.sender_alive {
                return Ok(None);
            }
            state.receiver_waiting = true;
            self.0.changed.notify_all();
            state = self
                .0
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        let queued = {
            let mut state = self.0.lock();
            state.receiver_alive = false;
            state.receiver_waiting = false;
            let queued = std::mem::take(&mut state.queue);
            self.0.changed.notify_all();
            queued
        };
        drop(queued);
    }
}

/// Owns one batch and retains its reservation through transformation and writer use.
#[derive(Debug)]
pub(super) struct BatchLease {
    batch: RecordBatch,
    reservation: Reservation,
}

#[derive(Debug)]
pub(super) struct Reservation(Option<(Arc<Control>, usize)>);

impl BatchLease {
    pub(super) fn uncharged(batch: RecordBatch) -> Self {
        Self {
            batch,
            reservation: Reservation(None),
        }
    }

    pub(super) fn into_parts(self) -> (RecordBatch, Reservation) {
        (self.batch, self.reservation)
    }

    pub(super) fn transform(
        self,
        f: impl FnOnce(RecordBatch) -> Result<RecordBatch, PipelineIoError>,
    ) -> Result<Self, PipelineError> {
        let Self {
            batch,
            mut reservation,
        } = self;
        let batch = f(batch)?;
        if let Some((control, retained)) = &mut reservation.0 {
            let bytes = charge(&batch)?;
            if bytes > *retained {
                let growth = bytes - *retained;
                let mut state = control.lock();
                let available = control.limit - state.bytes;
                if growth > available {
                    return Err(PipelineError::BatchGrowthExceedsBudget {
                        bytes,
                        available: *retained + available,
                    });
                }
                state.bytes += growth;
                state.peak = state.peak.max(state.bytes);
                *retained = bytes;
            }
        }
        Ok(Self { batch, reservation })
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if let Some((control, bytes)) = &self.0 {
            let mut state = control.lock();
            state.bytes -= bytes;
            control.changed.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use std::time::Duration;

    fn batch(note: &str) -> RecordBatch {
        RecordBatch::try_new(
            crate::schema(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["name"])),
                Arc::new(StringArray::from(vec![Some(note)])),
            ],
        )
        .expect("batch")
    }

    fn wait_for_send(control: &Control) {
        let mut state = control.lock();
        while !state.sender_waiting {
            let (next, timeout) = control
                .changed
                .wait_timeout(state, Duration::from_secs(5))
                .expect("wait");
            state = next;
            assert!(
                !timeout.timed_out(),
                "producer never entered a transport wait"
            );
        }
    }

    #[test]
    fn byte_admission_equal_budget_and_drop_release() {
        let input = batch("note");
        let bytes = charge(&input).expect("charge");
        let (sender, receiver, control) = channel(1, bytes, &CancellationToken::new());
        sender.send(Ok(input)).expect("equal budget");
        assert_eq!(control.metrics(), (bytes, bytes));
        let lease = receiver.recv().expect("receive").expect("batch");
        assert_eq!(
            control.metrics(),
            (bytes, bytes),
            "receiving must retain the charge"
        );
        drop(lease);
        assert_eq!(control.metrics(), (0, bytes));
    }

    #[test]
    fn byte_admission_oversized_fails_without_waiting() {
        let input = batch("note");
        let bytes = charge(&input).expect("charge");
        let (sender, _receiver, control) = channel(1, bytes - 1, &CancellationToken::new());
        assert!(
            matches!(sender.send(Ok(input)), Err(PipelineError::BatchTooLarge { bytes: b, limit }) if b == bytes && limit == bytes-1)
        );
        assert_eq!(control.metrics(), (0, 0));
    }

    #[test]
    fn byte_admission_cancel_wakes_count_byte_and_rendezvous_waits() {
        for capacity in [0, 1, 4] {
            let token = CancellationToken::new();
            let bytes = charge(&batch("note")).expect("charge");
            let (sender, _receiver, control) = channel(capacity, bytes, &token);
            if capacity > 0 {
                sender.send(Ok(batch("note"))).expect("first send");
            }
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| sender.send(Ok(batch("note"))));
                wait_for_send(&control);
                token.cancel();
                assert!(matches!(
                    worker.join().expect("sender thread"),
                    Err(PipelineError::Cancelled)
                ));
            });
            assert_eq!(control.metrics().0, 0);
        }
    }

    #[test]
    fn byte_admission_receiver_failure_wakes_outstanding_send() {
        for capacity in [0, 1] {
            let (sender, receiver, control) =
                channel(capacity, usize::MAX, &CancellationToken::new());
            if capacity > 0 {
                sender.send(Ok(batch("queued"))).expect("first send");
            }
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| sender.send(Ok(batch("blocked"))));
                wait_for_send(&control);
                drop(receiver);
                assert!(worker.join().expect("sender thread").is_err());
            });
            assert_eq!(control.metrics().0, 0);
        }
    }

    #[test]
    fn byte_admission_transform_growth_does_not_wait_on_queued_bytes() {
        let bytes = charge(&batch("a")).expect("charge");
        let (sender, receiver, control) = channel(2, bytes * 2, &CancellationToken::new());
        sender.send(Ok(batch("a"))).expect("first");
        sender.send(Ok(batch("b"))).expect("second");
        let lease = receiver.recv().expect("receive").expect("batch");
        let error = lease
            .transform(|_| Ok(batch(&"x".repeat(bytes * 2))))
            .expect_err("growth cannot wait");
        assert!(matches!(
            error,
            PipelineError::BatchGrowthExceedsBudget { .. }
        ));
        assert_eq!(control.metrics().0, bytes);
        drop(receiver);
        assert_eq!(control.metrics().0, 0);
    }

    #[test]
    fn byte_admission_transform_failure_and_shrink_keep_accounting() {
        let bytes = charge(&batch(&"x".repeat(1000))).expect("charge");
        let (sender, receiver, control) = channel(1, bytes, &CancellationToken::new());
        sender.send(Ok(batch(&"x".repeat(1000)))).expect("first");
        let lease = receiver.recv().expect("receive").expect("batch");
        let lease = lease.transform(|_| Ok(batch(""))).expect("shrink");
        assert_eq!(
            control.metrics().0,
            bytes,
            "shrink retains original conservative charge"
        );
        let failure = lease.transform(|_| {
            Err(PipelineIoError::InvalidId {
                row: 0,
                value: "callback".into(),
            })
        });
        assert!(matches!(
            failure,
            Err(PipelineError::Io(PipelineIoError::InvalidId { .. }))
        ));
        assert_eq!(control.metrics().0, 0);
    }

    #[test]
    fn byte_admission_rendezvous_completes_only_after_receive() {
        let (sender, receiver, control) = channel(0, usize::MAX, &CancellationToken::new());
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| sender.send(Ok(batch("a"))));
            wait_for_send(&control);
            let lease = receiver.recv().expect("receive").expect("batch");
            worker.join().expect("sender thread").expect("acknowledged");
            drop(lease);
        });
        assert_eq!(control.metrics().0, 0);
    }

    #[test]
    fn byte_admission_successful_growth_releases_adjusted_charge_and_wakes_sender() {
        let bytes = charge(&batch(&"x".repeat(1000))).expect("grown charge");
        let (sender, receiver, control) = channel(2, bytes, &CancellationToken::new());
        sender.send(Ok(batch("small"))).expect("first");
        let lease = receiver
            .recv()
            .expect("receive")
            .expect("first batch")
            .transform(|_| Ok(batch(&"x".repeat(1000))))
            .expect("available growth");
        assert_eq!(control.metrics(), (bytes, bytes));
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| sender.send(Ok(batch("next"))));
            wait_for_send(&control);
            drop(lease);
            worker
                .join()
                .expect("sender thread")
                .expect("released grown charge");
        });
        drop(receiver.recv().expect("receive").expect("next batch"));
        assert_eq!(control.metrics(), (0, bytes));
    }

    #[test]
    fn byte_admission_cancel_wakes_waiting_receiver() {
        let token = CancellationToken::new();
        let (_sender, receiver, control) = channel(0, usize::MAX, &token);
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| receiver.recv());
            let mut state = control.lock();
            while !state.receiver_waiting {
                let (next, timeout) = control
                    .changed
                    .wait_timeout(state, Duration::from_secs(5))
                    .expect("wait");
                state = next;
                assert!(!timeout.timed_out(), "receiver did not register its wait");
            }
            drop(state);
            token.cancel();
            assert!(matches!(
                worker.join().expect("receiver thread"),
                Err(PipelineError::Cancelled)
            ));
        });
        assert_eq!(control.metrics().0, 0);
    }

    #[test]
    fn byte_admission_cancel_queued_rendezvous_before_acknowledgment() {
        let token = CancellationToken::new();
        let (sender, _receiver, control) = channel(0, usize::MAX, &token);
        // Model a receiver paused after registering readiness but before taking
        // the delivered message. This is the real protocol's second wait state.
        control.lock().receiver_waiting = true;
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| sender.send(Ok(batch("queued"))));
            wait_for_send(&control);
            assert_eq!(control.lock().queue.len(), 1);
            assert!(control.metrics().0 > 0);
            token.cancel();
            assert!(matches!(
                worker.join().expect("sender thread"),
                Err(PipelineError::Cancelled)
            ));
        });
        assert_eq!(control.metrics().0, 0);
    }

    #[test]
    fn byte_admission_count_wait_and_registration_race() {
        let token = CancellationToken::new();
        let (sender, _receiver, control) = channel(1, usize::MAX, &token);
        sender.send(Ok(batch("first"))).expect("first");
        std::thread::scope(|scope| {
            let worker = scope.spawn(|| sender.send(Ok(batch("second"))));
            wait_for_send(&control);
            token.cancel();
            assert!(matches!(
                worker.join().expect("sender"),
                Err(PipelineError::Cancelled)
            ));
        });
        assert_eq!(control.metrics().0, 0);
        for _ in 0..100 {
            let token = CancellationToken::new();
            let barrier = std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                let worker = scope.spawn(|| {
                    barrier.wait();
                    channel(1, usize::MAX, &token)
                });
                barrier.wait();
                token.cancel();
                let (sender, _receiver, control) = worker.join().expect("registration");
                assert!(matches!(
                    sender.send(Ok(batch("raced"))),
                    Err(PipelineError::Cancelled)
                ));
                assert_eq!(control.metrics().0, 0);
            });
        }
    }

    #[test]
    fn byte_admission_cancel_before_registration_and_shared_token() {
        let token = CancellationToken::new();
        let (one, _r1, c1) = channel(0, usize::MAX, &token);
        let (two, _r2, c2) = channel(0, usize::MAX, &token);
        std::thread::scope(|scope| {
            let first = scope.spawn(|| one.send(Ok(batch("a"))));
            let second = scope.spawn(|| two.send(Ok(batch("b"))));
            wait_for_send(&c1);
            wait_for_send(&c2);
            token.cancel();
            assert!(first.join().expect("first").is_err());
            assert!(second.join().expect("second").is_err());
        });
        let (third, _r3, c3) = channel(1, usize::MAX, &token);
        assert!(matches!(
            third.send(Ok(batch("c"))),
            Err(PipelineError::Cancelled)
        ));
        assert_eq!(c1.metrics().0 + c2.metrics().0 + c3.metrics().0, 0);
    }

    #[test]
    fn byte_admission_shared_sliced_and_empty_arrays_follow_arrow_charge() {
        let input = batch(&"x".repeat(1000));
        let sliced = input.slice(0, 0);
        assert_eq!(
            charge(&sliced).expect("slice charge"),
            sliced.get_array_memory_size()
        );
        let repeated = RecordBatch::try_new(
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Utf8, false),
                arrow::datatypes::Field::new("b", arrow::datatypes::DataType::Utf8, false),
            ])),
            vec![Arc::clone(input.column(1)), Arc::clone(input.column(1))],
        )
        .expect("shared arrays");
        assert_eq!(
            charge(&repeated).expect("shared charge"),
            repeated.get_array_memory_size()
        );
        assert!(matches!(
            checked_charge([usize::MAX, 1]),
            Err(PipelineError::ByteChargeOverflow)
        ));
    }
}
