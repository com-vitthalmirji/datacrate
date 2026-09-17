# The story

Every data pipeline starts the same way: a file lands somewhere, and something
has to turn it into rows a program can trust. The first version is always
simple — read a line, split it on commas, do something with the fields. It
works, right up until it doesn't: a column gets renamed upstream, a field that
used to always be present starts showing up empty, and the failure shows up
three stages downstream, at 2am, as a stack trace with no clue which producer
actually changed.

`datacrate` is what happens when you refuse to accept that as the cost of
doing business, and you have a language that will actually let you refuse.

## Reading without copying

The first problem is the most boring-sounding one: reading a CSV file. Not
"read the whole file into memory and iterate" — that's the version every
scripting language nudges you toward, and it's also the version that falls
over the moment the input is bigger than the machine's RAM. The interesting
version streams: one record in, one record processed, one record out, and the
underlying bytes never get copied more times than they have to.

This sounds like an implementation detail. It isn't. It's the first place
where owning your data — deciding precisely which piece of code is
responsible for a value, and for how long — stops being an academic exercise
and starts being the reason the pipeline can handle a file that doesn't fit
in memory. A borrowed slice into the original buffer costs nothing to create
and nothing to destroy. A cloned string costs an allocation every single row.
Multiply that by a few hundred million rows and the difference isn't
stylistic anymore.

## Columns that don't lie about their shape

Once rows are flowing, the next question is what shape they're in — and
whether that shape is something you can trust or something you have to keep
re-checking. A columnar, Arrow-backed representation makes a specific promise:
slicing a batch doesn't copy it. Selecting a window of rows out of a million-row
batch is a pointer and a length, not a new allocation. That promise is
provable, not just claimed — take two views into the same batch and compare
the pointers to their underlying allocation: they're identical, because
slicing only ever adjusts an offset and a length, never the bytes underneath.
When memory usage matters, "provable" beats "documented" every time.

## The pipeline that won't finish half-built

Somewhere around the third or fourth pipeline stage, a familiar bug starts to
recur: someone wires up a source and a sink, forgets the transform in the
middle, and finds out only when the program runs and throws at 2am — again.
The fix isn't a better runtime check. It's making the incomplete pipeline
impossible to construct in the first place. A builder whose required stages
are tracked in its own type signature simply doesn't have a `.build()` method
until every stage has been supplied. Forget one, and the compiler says so —
not with a runtime panic buried in a log file, but with an ordinary "method
not found" error at the exact line where the mistake was made. The bug still
exists as a possibility; it just never survives long enough to run.

## Schemas that argue with each other before you ship

The last and sharpest form of the same problem is schema drift between two
independently-evolving parts of a pipeline: a struct produced by one stage,
a struct expected by the next, agreeing by convention and nothing else.
Nothing stops them from drifting apart — a field renamed here, a type
loosened from `String` to `Option<String>` there — and the only sign is data
silently going missing downstream, or a panic that surfaces the mismatch far
from its cause.

Compile-time contracts turn that convention into an enforced one. A producer's
shape and a contract's shape get compared structurally, at compile time, under
a policy that says exactly what kind of drift is tolerable — exact agreement,
backward-compatible, forward-compatible, or both. If the shapes don't conform,
the crate doesn't compile. Not "the tests fail once someone remembers to run
them" — it doesn't build. The disagreement between two parts of the pipeline
becomes visible at the moment it's introduced, to the person who introduced
it, instead of becoming a production incident weeks later with no clear owner.

## The thread running through it

None of these four things — streaming without copying, provably zero-copy
slicing, a builder that can't be half-built, a schema check that runs at
compile time — are separate tricks. They're the same idea applied at four
different layers: push the failure as early as it can possibly go, and prefer
a check the compiler can make over a check a human has to remember to run.
That's the whole shape of `datacrate`. Everything else in this guide is just
the mechanics of how.
