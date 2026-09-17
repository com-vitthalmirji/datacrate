set shell := ["zsh", "-cu"]

fmt:
    cargo fmt --all --check

lint:
    cargo clippy --all-targets --all-features -- -D warnings

test:
    cargo test --all-targets --all-features --locked

release:
    cargo build --release --locked

docs:
    RUSTDOCFLAGS="-D warnings" cargo doc --no-deps -p dtl-core -p csv-select-cli
    mdbook build book

verify: fmt lint test release
    git diff --check

preflight:
    scripts/preflight.sh

install-hooks:
    git config core.hooksPath .githooks

minio-up:
    docker compose -f docker-compose.minio.yml up -d

minio-down:
    docker compose -f docker-compose.minio.yml down

minio-reset:
    docker compose -f docker-compose.minio.yml down -v

# Pinned by commit, not branch — see docs/internals/notes/decisions.md, 2026-09-15 entry.
ballista-install:
    cargo install --git https://github.com/apache/datafusion-ballista --rev ae9bff026ffd18b256d39b10890a2ca96e1fc836 ballista-scheduler ballista-executor

ballista-scheduler:
    ballista-scheduler --bind-port 50050

# --memory-pool-size set explicitly (64GB host, 2 executors, ~16GB headroom
# for OS/scheduler/client) — see docs/internals/notes/decisions.md, M3.8
# executor-topology entry: unset defaults to 70% of host memory *per executor*
# with vcores=all-cores, which oversubscribes a shared box badly once more
# than one executor is running.
ballista-executor-1:
    ballista-executor --scheduler-port 50050 --bind-port 50061 --bind-grpc-port 50062 --bind-health-port 50063 --work-dir /tmp/ballista-executor-1 --memory-pool-size 24GB

ballista-executor-2:
    ballista-executor --scheduler-port 50050 --bind-port 50071 --bind-grpc-port 50072 --bind-health-port 50073 --work-dir /tmp/ballista-executor-2 --memory-pool-size 24GB

# M3.8-only: same topology as ballista-executor-1/-2, but built from
# ballista-executor-scale (crates/pipeline/src/bin/ballista-executor-scale.rs)
# so the on-disk spill quota can be raised past the upstream 100GB default.
# 150GB per executor (300GB combined) leaves headroom under the ~377GB free
# on this box after the ~140GB M3.8 dataset. See
# docs/internals/notes/decisions.md, M3.8 disk-spill-limit entry.
ballista-executor-1-scale:
    cargo run --release -p pipeline --bin ballista-executor-scale -- --scheduler-port 50050 --bind-port 50061 --bind-grpc-port 50062 --bind-health-port 50063 --work-dir /tmp/ballista-executor-1 --memory-pool-size 24GB --max-temp-directory-size 150GB

ballista-executor-2-scale:
    cargo run --release -p pipeline --bin ballista-executor-scale -- --scheduler-port 50050 --bind-port 50071 --bind-grpc-port 50072 --bind-health-port 50073 --work-dir /tmp/ballista-executor-2 --memory-pool-size 24GB --max-temp-directory-size 150GB

# One executor per physical core (12 on this machine) — M3.8 local[*]-equivalent
# topology, see docs/internals/notes/decisions.md, "M3.8 Ballista local[*]" entry.
ballista-executor-3:
    ballista-executor --scheduler-port 50050 --bind-port 50081 --bind-grpc-port 50082 --bind-health-port 50083 --work-dir /tmp/ballista-executor-3

ballista-executor-4:
    ballista-executor --scheduler-port 50050 --bind-port 50091 --bind-grpc-port 50092 --bind-health-port 50093 --work-dir /tmp/ballista-executor-4

ballista-executor-5:
    ballista-executor --scheduler-port 50050 --bind-port 50101 --bind-grpc-port 50102 --bind-health-port 50103 --work-dir /tmp/ballista-executor-5

ballista-executor-6:
    ballista-executor --scheduler-port 50050 --bind-port 50111 --bind-grpc-port 50112 --bind-health-port 50113 --work-dir /tmp/ballista-executor-6

ballista-executor-7:
    ballista-executor --scheduler-port 50050 --bind-port 50121 --bind-grpc-port 50122 --bind-health-port 50123 --work-dir /tmp/ballista-executor-7

ballista-executor-8:
    ballista-executor --scheduler-port 50050 --bind-port 50131 --bind-grpc-port 50132 --bind-health-port 50133 --work-dir /tmp/ballista-executor-8

ballista-executor-9:
    ballista-executor --scheduler-port 50050 --bind-port 50141 --bind-grpc-port 50142 --bind-health-port 50143 --work-dir /tmp/ballista-executor-9

ballista-executor-10:
    ballista-executor --scheduler-port 50050 --bind-port 50151 --bind-grpc-port 50152 --bind-health-port 50153 --work-dir /tmp/ballista-executor-10

ballista-executor-11:
    ballista-executor --scheduler-port 50050 --bind-port 50161 --bind-grpc-port 50162 --bind-health-port 50163 --work-dir /tmp/ballista-executor-11

ballista-executor-12:
    ballista-executor --scheduler-port 50050 --bind-port 50171 --bind-grpc-port 50172 --bind-health-port 50173 --work-dir /tmp/ballista-executor-12

# Pinned by version, checksum-verified against Maven Central — see
# docs/internals/notes/decisions.md for the M3.5 Comet entry.
comet-jar-file := "comet-spark-spark4.1_2.13-0.16.0.jar"
comet-maven-path := "org/apache/datafusion/comet-spark-spark4.1_2.13/0.16.0/comet-spark-spark4.1_2.13-0.16.0.jar"

spark-comet-up:
    docker compose -f docker-compose.spark-comet.yml up -d

spark-comet-down:
    docker compose -f docker-compose.spark-comet.yml down

spark-comet-jar:
    #!/usr/bin/env zsh
    set -euo pipefail
    jar="benchmark/spark-comet/{{comet-jar-file}}"
    if [ -f "$jar" ]; then
        echo "already present: $jar"
        exit 0
    fi
    curl -fsSL -o "$jar" "https://repo1.maven.org/maven2/{{comet-maven-path}}"
    curl -fsSL -o "$jar.sha1" "https://repo1.maven.org/maven2/{{comet-maven-path}}.sha1"
    expected=$(cat "$jar.sha1")
    actual=$(shasum -a 1 "$jar" | cut -d' ' -f1)
    if [ "$expected" != "$actual" ]; then
        echo "checksum mismatch for $jar: expected $expected, got $actual" >&2
        rm -f "$jar" "$jar.sha1"
        exit 1
    fi
    echo "downloaded and verified: $jar"

comet-dataset:
    cargo run --release -p pipeline --example comet_benchmark_dataset -- --output benchmark/spark-comet/orders.parquet

comet-vanilla:
    docker compose -f docker-compose.spark-comet.yml exec spark \
        bash -c 'time /opt/spark/bin/spark-sql -f /spark-comet/query.sql'

comet-accelerated:
    docker compose -f docker-compose.spark-comet.yml exec spark \
        bash -c 'time /opt/spark/bin/spark-sql \
        --jars /spark-comet/{{comet-jar-file}} \
        --conf spark.plugins=org.apache.spark.CometPlugin \
        --conf spark.comet.enabled=true \
        --conf spark.memory.offHeap.enabled=true \
        --conf spark.memory.offHeap.size=2g \
        --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
        -f /spark-comet/query.sql'

comet-datafusion:
    cargo run --release -p pipeline --bin comet-aggregate-datafusion -- --input benchmark/spark-comet/orders.parquet

# M3.7 joins/shuffle comparison — see docs/internals/notes/decisions.md
# and docs/internals/notes/risks.md for scope. autoBroadcastJoinThreshold=-1
# forces a real shuffle (sort-merge/shuffle-hash) join on both Spark legs
# instead of silently broadcasting the smaller side.
join-shuffle-dataset:
    cargo run --release -p pipeline --example join_shuffle_benchmark_dataset -- --output-dir benchmark/join-shuffle

join-shuffle-vanilla:
    docker compose -f docker-compose.spark-comet.yml exec spark \
        bash -c 'time /opt/spark/bin/spark-sql \
        --conf spark.sql.autoBroadcastJoinThreshold=-1 \
        -f /join-shuffle/query.sql'

join-shuffle-accelerated:
    docker compose -f docker-compose.spark-comet.yml exec spark \
        bash -c 'time /opt/spark/bin/spark-sql \
        --jars /spark-comet/{{comet-jar-file}} \
        --conf spark.plugins=org.apache.spark.CometPlugin \
        --conf spark.comet.enabled=true \
        --conf spark.memory.offHeap.enabled=true \
        --conf spark.memory.offHeap.size=2g \
        --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
        --conf spark.sql.autoBroadcastJoinThreshold=-1 \
        -f /join-shuffle/query.sql'

join-shuffle-datafusion:
    cargo run --release -p pipeline --bin join-shuffle-datafusion -- \
        --orders benchmark/join-shuffle/orders_join.parquet \
        --shipments benchmark/join-shuffle/shipments_join.parquet

# Deferred Ballista shuffle leg of M3.7 — reuses the same dataset and the
# existing 2-executor cluster (just ballista-scheduler/-executor-1/-executor-2
# must already be running). See docs/internals/notes/risks.md row 25.
join-shuffle-ballista:
    cargo run --release -p pipeline --bin join-shuffle-ballista -- \
        --orders benchmark/join-shuffle/orders_join.parquet \
        --shipments benchmark/join-shuffle/shipments_join.parquet

# M3.6 scale comparison — Spark-vs-Rust-ecosystem retest + DataFusion-vs-Polars.
# See docs/internals/notes/decisions.md, 2026-09-15 "M3.6 scoped" entry.
# Small first pass for correctness-proving before the ~100GB timed run.
m36-dataset:
    cargo run --release -p pipeline --example scale_benchmark_dataset -- \
        --output benchmark/m3.6/orders --rows 5000000 --partitions 8

# Row count derived from m36-dataset's measured 16.18 bytes/row
# (5M rows -> 80922363 bytes), not guessed, targeting ~100GB on disk —
# see docs/internals/notes/decisions.md, "M3.6 scoped" entry.
m36-dataset-full:
    cargo run --release -p pipeline --example scale_benchmark_dataset -- \
        --output benchmark/m3.6/orders --rows 6600000000 --partitions 8

m36-datafusion:
    cargo run --release -p pipeline --bin scale-aggregate-datafusion -- --input benchmark/m3.6/orders

# Reuses the existing 2-executor local cluster (just ballista-scheduler /
# ballista-executor-1 / ballista-executor-2 must already be running).
m36-ballista:
    cargo run --release -p pipeline --bin scale-aggregate-ballista -- --input benchmark/m3.6/orders

m36-polars:
    cargo run --release -p pipeline --bin scale-aggregate-polars -- --input benchmark/m3.6/orders

m36-vanilla:
    docker compose -f docker-compose.spark-comet.yml exec spark \
        bash -c 'time /opt/spark/bin/spark-sql -f /m3.6/query.sql'

m36-accelerated:
    docker compose -f docker-compose.spark-comet.yml exec spark \
        bash -c 'time /opt/spark/bin/spark-sql \
        --jars /spark-comet/{{comet-jar-file}} \
        --conf spark.plugins=org.apache.spark.CometPlugin \
        --conf spark.comet.enabled=true \
        --conf spark.memory.offHeap.enabled=true \
        --conf spark.memory.offHeap.size=2g \
        --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
        -f /m3.6/query.sql'

# M3.8 — join-at-scale + high-cardinality group-by + multi-predicate filter,
# built in parallel with the M3 rehearsal checklist per explicit exception.
# See docs/internals/notes/decisions.md, "M3.8 scoped" entry, and
# docs/internals/notes/risks.md row 26. Polars is excluded from the join leg
# (comet_ballista_framing house rule: Polars comparisons stay DataFusion-
# internal, never against Spark). Small first pass for correctness-proving
# before the 91GB-scale timed run.
m38-dataset:
    cargo run --release -p pipeline --example scale_benchmark_dataset -- \
        --output benchmark/m3.8/orders --rows 5000000 --partitions 8
    cargo run --release -p pipeline --example scale_benchmark_shipments -- \
        --output benchmark/m3.8/shipments --order-rows 5000000 --partitions 8

# Row counts match M3.6's 91GB-scale run (6,600,000,000 orders, 8 partitions)
# plus shipments covering the same order-id range.
m38-dataset-full:
    cargo run --release -p pipeline --example scale_benchmark_dataset -- \
        --output benchmark/m3.8/orders --rows 6600000000 --partitions 8
    cargo run --release -p pipeline --example scale_benchmark_shipments -- \
        --output benchmark/m3.8/shipments --order-rows 6600000000 --partitions 8

m38-datafusion-aggregate:
    cargo run --release -p pipeline --bin scale-aggregate-datafusion -- \
        --input benchmark/m3.8/orders --query aggregate

m38-datafusion-groupby:
    cargo run --release -p pipeline --bin scale-aggregate-datafusion -- \
        --input benchmark/m3.8/orders --query group-by-bucket

m38-datafusion-filter:
    cargo run --release -p pipeline --bin scale-aggregate-datafusion -- \
        --input benchmark/m3.8/orders --query multi-predicate

# Reuses the existing 2-executor local cluster (just ballista-scheduler /
# ballista-executor-1 / ballista-executor-2 must already be running).
m38-ballista-aggregate:
    cargo run --release -p pipeline --bin scale-aggregate-ballista -- \
        --input benchmark/m3.8/orders --query aggregate

m38-ballista-groupby:
    cargo run --release -p pipeline --bin scale-aggregate-ballista -- \
        --input benchmark/m3.8/orders --query group-by-bucket

m38-ballista-filter:
    cargo run --release -p pipeline --bin scale-aggregate-ballista -- \
        --input benchmark/m3.8/orders --query multi-predicate

m38-vanilla-aggregate:
    docker compose -f docker-compose.spark-comet.yml exec spark \
        bash -c 'time /opt/spark/bin/spark-sql -f /m3.8/query-aggregate.sql'

m38-vanilla-groupby:
    docker compose -f docker-compose.spark-comet.yml exec spark \
        bash -c 'time /opt/spark/bin/spark-sql -f /m3.8/query-groupby.sql'

m38-vanilla-filter:
    docker compose -f docker-compose.spark-comet.yml exec spark \
        bash -c 'time /opt/spark/bin/spark-sql -f /m3.8/query-filter.sql'

m38-accelerated-aggregate:
    docker compose -f docker-compose.spark-comet.yml exec spark \
        bash -c 'time /opt/spark/bin/spark-sql \
        --jars /spark-comet/{{comet-jar-file}} \
        --conf spark.plugins=org.apache.spark.CometPlugin \
        --conf spark.comet.enabled=true \
        --conf spark.memory.offHeap.enabled=true \
        --conf spark.memory.offHeap.size=2g \
        --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
        -f /m3.8/query-aggregate.sql'

m38-accelerated-groupby:
    docker compose -f docker-compose.spark-comet.yml exec spark \
        bash -c 'time /opt/spark/bin/spark-sql \
        --jars /spark-comet/{{comet-jar-file}} \
        --conf spark.plugins=org.apache.spark.CometPlugin \
        --conf spark.comet.enabled=true \
        --conf spark.memory.offHeap.enabled=true \
        --conf spark.memory.offHeap.size=2g \
        --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
        -f /m3.8/query-groupby.sql'

m38-accelerated-filter:
    docker compose -f docker-compose.spark-comet.yml exec spark \
        bash -c 'time /opt/spark/bin/spark-sql \
        --jars /spark-comet/{{comet-jar-file}} \
        --conf spark.plugins=org.apache.spark.CometPlugin \
        --conf spark.comet.enabled=true \
        --conf spark.memory.offHeap.enabled=true \
        --conf spark.memory.offHeap.size=2g \
        --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
        -f /m3.8/query-filter.sql'

m38-datafusion-join:
    cargo run --release -p pipeline --bin join-shuffle-datafusion -- \
        --orders benchmark/m3.8/orders --shipments benchmark/m3.8/shipments

# Requires ballista-scheduler + ballista-executor-1-scale + ballista-executor-2-scale
# running (not the plain ballista-executor-1/-2), so the raised spill quota is in effect.
m38-ballista-join:
    cargo run --release -p pipeline --bin join-shuffle-ballista-scale -- \
        --orders benchmark/m3.8/orders --shipments benchmark/m3.8/shipments

m38-vanilla-join:
    docker compose -f docker-compose.spark-comet.yml exec spark \
        bash -c 'time /opt/spark/bin/spark-sql \
        --conf spark.sql.autoBroadcastJoinThreshold=-1 \
        -f /m3.8/query-join.sql'

m38-accelerated-join:
    docker compose -f docker-compose.spark-comet.yml exec spark \
        bash -c 'time /opt/spark/bin/spark-sql \
        --jars /spark-comet/{{comet-jar-file}} \
        --conf spark.plugins=org.apache.spark.CometPlugin \
        --conf spark.comet.enabled=true \
        --conf spark.memory.offHeap.enabled=true \
        --conf spark.memory.offHeap.size=2g \
        --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
        --conf spark.sql.autoBroadcastJoinThreshold=-1 \
        -f /m3.8/query-join.sql'

# M3-gate parity probe: timestamp/null/ordering/window semantics, Spark vs. DataFusion.
# Small fixture (fixtures/m3/orders.csv, 8 rows) - a correctness proof, not a scale benchmark.
parity-dataset:
    cargo run --release -p pipeline --example parity_fixture -- \
        --input fixtures/m3/orders.csv --output benchmark/parity/orders.parquet

parity-datafusion:
    cargo run --release -p pipeline --bin parity-datafusion -- \
        --input benchmark/parity/orders.parquet

parity-spark:
    docker compose -f docker-compose.spark-comet.yml exec spark \
        bash -c '/opt/spark/bin/spark-sql -f /parity/query.sql'
