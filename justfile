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

ballista-executor-1:
    ballista-executor --scheduler-port 50050 --bind-port 50061 --bind-grpc-port 50062 --bind-health-port 50063 --work-dir /tmp/ballista-executor-1

ballista-executor-2:
    ballista-executor --scheduler-port 50050 --bind-port 50071 --bind-grpc-port 50072 --bind-health-port 50073 --work-dir /tmp/ballista-executor-2

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
