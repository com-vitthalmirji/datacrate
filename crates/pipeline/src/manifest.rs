//! The completion manifest [`run_bounded_pipeline_s3`](crate::bounded::run_bounded_pipeline_s3)
//! publishes once its output object is fully written: proof of *what* was
//! written (row count, a logical-content digest) and *from what* (a snapshot
//! of the input object), without ever buffering more than one row's hash at
//! a time.
//!
//! Order independence: `accumulate_batch_digest` combines each row's
//! individual hash into a running total with wrapping (mod 2^64) addition,
//! not XOR — XOR cancels on duplicate rows (`x ^ x == 0`), which would let a
//! dataset with dropped duplicate rows hash identically to the original. The
//! resulting digest is an AdHash-style incremental multiset hash
//! (Bellare-Micciancio): a commutative, associative combiner, so the result
//! only depends on the multiset of rows, never on batch boundaries or
//! arrival order. This isn't a security boundary, so a fast non-cryptographic
//! per-row hash (`ahash`, already in the dependency tree via datafusion) is
//! enough — the digest exists to catch accidental content drift, not an
//! adversary.

use std::hash::{BuildHasher, Hasher};

use ahash::RandomState;
use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use object_store::ObjectMeta;
use object_store::path::Path as ObjectPath;
use serde::{Deserialize, Serialize};

/// Fixed, arbitrary seeds for [`hash_row`]'s `RandomState`. `ahash`'s default
/// seeds are randomised per process — meant to resist hash-flooding denial of
/// service in hash maps — which would make the digest differ across runs of
/// the *same* content. A content fingerprint needs the opposite: identical
/// content must hash identically no matter which process computed it.
const DIGEST_SEED_1: u64 = 0x517c_c1b7_2722_0a95;
const DIGEST_SEED_2: u64 = 0x2545_f491_4f6c_dd1d;
const DIGEST_SEED_3: u64 = 0x9e37_79b9_7f4a_7c15;
const DIGEST_SEED_4: u64 = 0xbf58_476d_1ce4_e5b9;

fn hash_row(id: i64, name: &str, note: Option<&str>) -> u64 {
    let build_hasher =
        RandomState::with_seeds(DIGEST_SEED_1, DIGEST_SEED_2, DIGEST_SEED_3, DIGEST_SEED_4);
    let mut hasher = build_hasher.build_hasher();
    hasher.write_i64(id);
    hasher.write(name.as_bytes());
    match note {
        Some(note) => {
            hasher.write_u8(1);
            hasher.write(note.as_bytes());
        }
        None => hasher.write_u8(0),
    }
    hasher.finish()
}

/// Folds every row of `batch` into `running` via wrapping addition. Call
/// once per batch, in any order, over however many batches make up the
/// output — the result only depends on the multiset of rows, not on how
/// they were chunked or in what order the batches arrived.
pub(crate) fn accumulate_batch_digest(running: u64, batch: &RecordBatch) -> u64 {
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("pipeline batches always have an Int64 id column at index 0");
    let names = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("pipeline batches always have a Utf8 name column at index 1");
    let notes = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("pipeline batches always have a Utf8 note column at index 2");

    (0..batch.num_rows()).fold(running, |acc, row| {
        let row_hash = hash_row(
            ids.value(row),
            names.value(row),
            (!notes.is_null(row)).then(|| notes.value(row)),
        );
        acc.wrapping_add(row_hash)
    })
}

/// Proof that an object-store output was fully and correctly written:
/// row count, an order-independent content digest, and a snapshot of the
/// input object it was built from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionManifest {
    /// Bumped whenever this struct's field set changes shape.
    pub schema_version: u32,
    /// The input object's key, as given to `run_bounded_pipeline_s3`.
    pub input_key: String,
    /// The input object's size in bytes, at the moment it was read.
    pub input_size: u64,
    /// The input object's ETag, if the store reports one — lets a reader
    /// confirm the input hasn't changed since this manifest was written.
    pub input_e_tag: Option<String>,
    /// The input object's last-modified time, RFC 3339.
    pub input_last_modified: String,
    /// Rows written to the output object.
    pub row_count: usize,
    /// `accumulate_batch_digest`'s final value, as lowercase hex —
    /// identical for the same row multiset regardless of batch or row order.
    pub content_digest: String,
}

impl CompletionManifest {
    pub(crate) fn new(
        input_key: &ObjectPath,
        input_meta: &ObjectMeta,
        row_count: usize,
        content_digest: u64,
    ) -> Self {
        Self {
            schema_version: 1,
            input_key: input_key.to_string(),
            input_size: input_meta.size,
            input_e_tag: input_meta.e_tag.clone(),
            input_last_modified: input_meta.last_modified.to_rfc3339(),
            row_count,
            content_digest: format!("{content_digest:016x}"),
        }
    }

    /// The manifest's own key for a given output key: a `.manifest.json`
    /// sibling, published only after `output_key` itself is fully written —
    /// its presence is the completion signal a reader should wait for.
    pub fn key_for(output_key: &ObjectPath) -> ObjectPath {
        ObjectPath::from(format!("{output_key}.manifest.json"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema;
    use arrow::array::ArrayRef;
    use std::sync::Arc;

    fn batch(rows: &[(i64, &str, Option<&str>)]) -> RecordBatch {
        let ids: ArrayRef = Arc::new(Int64Array::from(
            rows.iter().map(|r| r.0).collect::<Vec<_>>(),
        ));
        let names: ArrayRef = Arc::new(StringArray::from(
            rows.iter().map(|r| Some(r.1)).collect::<Vec<_>>(),
        ));
        let notes: ArrayRef = Arc::new(StringArray::from(
            rows.iter().map(|r| r.2).collect::<Vec<_>>(),
        ));
        RecordBatch::try_new(schema(), vec![ids, names, notes]).expect("test batch should build")
    }

    #[test]
    fn digest_is_independent_of_batch_boundaries() {
        let rows = [(1, "Ada", None), (2, "Zoë", Some("note"))];
        let whole = accumulate_batch_digest(0, &batch(&rows));

        let split = rows.iter().fold(0u64, |acc, row| {
            accumulate_batch_digest(acc, &batch(&[*row]))
        });

        assert_eq!(whole, split);
    }

    #[test]
    fn digest_is_independent_of_row_order() {
        let forward = batch(&[(1, "Ada", None), (2, "Zoë", Some("note"))]);
        let backward = batch(&[(2, "Zoë", Some("note")), (1, "Ada", None)]);

        assert_eq!(
            accumulate_batch_digest(0, &forward),
            accumulate_batch_digest(0, &backward)
        );
    }

    #[test]
    fn digest_changes_when_content_changes() {
        let a = batch(&[(1, "Ada", None)]);
        let b = batch(&[(1, "Ada", Some("different"))]);

        assert_ne!(
            accumulate_batch_digest(0, &a),
            accumulate_batch_digest(0, &b)
        );
    }

    #[test]
    fn duplicate_rows_do_not_cancel_out() {
        let one = batch(&[(1, "Ada", None)]);
        let two = batch(&[(1, "Ada", None), (1, "Ada", None)]);

        assert_ne!(
            accumulate_batch_digest(0, &one),
            accumulate_batch_digest(0, &two),
            "a combiner where duplicates cancel (e.g. XOR) would wrongly make \
             these equal"
        );
    }

    #[test]
    fn manifest_key_is_a_sibling_of_the_output_key() {
        let output = ObjectPath::from("prefix/output.parquet");
        assert_eq!(
            CompletionManifest::key_for(&output).as_ref(),
            "prefix/output.parquet.manifest.json"
        );
    }
}
