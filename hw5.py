import statistics
import time
from pathlib import Path
from typing import Literal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LOG = "data/NASA_access_log_Jul95"
LOG_RE = r'^\S+ \S+ \S+ \[[^\]]+\] "[^"]*" \d{3} \S+$'
RUNS = 5
USES = 3
Variant = Literal["base", "cache", "coalesce_2", "repartition_16"]
VARIANTS: tuple[Variant, ...] = ("base", "cache", "coalesce_2", "repartition_16")

spark = (SparkSession.builder.master("local[*]").appName("hw5")
         .config("spark.driver.memory", "4g")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def hosts() -> DataFrame:
    return (spark.read.text(LOG)
            .filter(F.col("value").rlike(LOG_RE))
            .select(F.substring_index("value", " ", 1).alias("host")))


def collect_counts(df: DataFrame) -> dict[str, int]:
    rows = df.groupBy("host").count().collect()
    return {str(row["host"]): int(row["count"]) for row in rows}


def prepare(variant: Variant) -> DataFrame:
    df = hosts()
    if variant == "cache":
        return df.cache()
    if variant == "coalesce_2":
        return df.coalesce(2)
    if variant == "repartition_16":
        return df.repartition(16)
    return df


try:
    print(f"Spark {spark.version}, {spark.sparkContext.master}, "
          f"defaultParallelism={spark.sparkContext.defaultParallelism}")
    print(f"file bytes: {Path(LOG).stat().st_size}")
    print(f"AQE={spark.conf.get('spark.sql.adaptive.enabled')}, "
          f"shuffle.partitions={spark.conf.get('spark.sql.shuffle.partitions')}")
    lines = spark.read.text(LOG).count()
    print(f"lines: {lines}")

    expected: dict[str, int] = {}
    print("\n--- warmup and partitions (outside timing)")
    for variant in VARIANTS:
        df = prepare(variant)
        print(f"{variant}: partitions={df.rdd.getNumPartitions()}, "
              f"storage={df.storageLevel}")
        result = collect_counts(df)
        if variant == "base":
            expected = result
        assert result == expected, variant
        df.unpersist(blocking=True)

    assert len(expected) == 81982
    assert sum(expected.values()) == 1891700
    assert lines - sum(expected.values()) == 15
    print(f"hosts={len(expected)}, requests={sum(expected.values())}, "
          f"dropped={lines - sum(expected.values())}")
    print("\n--- top-10")
    for host, count in sorted(expected.items(), key=lambda kv: (-kv[1], kv[0]))[:10]:
        print(f"{host:<32} {count}")

    samples: dict[Variant, list[tuple[float, ...]]] = {v: [] for v in VARIANTS}
    print(f"\n--- {RUNS} rounds, {USES} actions per variant, seconds")
    for run in range(RUNS):
        offset = run % len(VARIANTS)
        for variant in VARIANTS[offset:] + VARIANTS[:offset]:
            df = prepare(variant)
            elapsed = []
            try:
                for _ in range(USES):
                    start = time.perf_counter()
                    rows = df.groupBy("host").count().collect()
                    elapsed.append(time.perf_counter() - start)
                    result = {str(row["host"]): int(row["count"]) for row in rows}
                    assert result == expected, (run + 1, variant)
                samples[variant].append(tuple(elapsed))
                print(f"round={run + 1} {variant:<15} "
                      f"actions={','.join(f'{t:.3f}' for t in elapsed)} "
                      f"total={sum(elapsed):.3f}", flush=True)
                if run == 0 and variant == "cache":
                    print("\n--- cached aggregation plan (outside timing)")
                    df.groupBy("host").count().explain()
            finally:
                df.unpersist(blocking=True)

    print("\n--- medians, seconds")
    print("variant          first    second   third    total    speedup")
    base = statistics.median(sum(ts) for ts in samples["base"])
    for variant in VARIANTS:
        measured = samples[variant]
        medians = [statistics.median(ts[i] for ts in measured) for i in range(USES)]
        total = statistics.median(sum(ts) for ts in measured)
        print(f"{variant:<16} " + " ".join(f"{t:8.3f}" for t in medians)
              + f" {total:8.3f} {base / total:8.2f}x")
    print("\nfull result identical in every action: True")
finally:
    spark.stop()
