"""
Prefect 3 Demo Flow
Showcases: tasks, subflows, retries, artifacts, logging,
           variables, tags, caching, task state, flow state
"""

import random
import time
from datetime import datetime

from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact, create_table_artifact
from prefect.cache_policies import TASK_SOURCE
from prefect.tasks import task_input_hash


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(
    name="Generate Sales Data",
    description="Generates fake daily sales records",
    tags=["data-generation", "input"],
)
def generate_sales_data(n_records: int = 20) -> list[dict]:
    logger = get_run_logger()
    logger.info(f"Generating {n_records} sales records...")

    regions = ["Baghdad", "Basra", "Mosul", "Erbil", "Wasit"]
    products = ["Fiber 10M", "Fiber 50M", "Fiber 100M", "Fiber 500M"]

    records = []
    for i in range(n_records):
        records.append({
            "id": i + 1,
            "region": random.choice(regions),
            "product": random.choice(products),
            "revenue": round(random.uniform(10_000, 500_000), 2),
            "subscribers": random.randint(10, 500),
            "churn_rate": round(random.uniform(0.01, 0.15), 4),
        })

    logger.info(f"Generated {len(records)} records across {len(regions)} regions")
    return records


@task(
    name="Validate Data",
    description="Checks for nulls and out-of-range values",
    tags=["validation"],
    retries=2,
    retry_delay_seconds=2,
)
def validate_data(records: list[dict]) -> list[dict]:
    logger = get_run_logger()

    # Simulate an occasional transient failure to show retry in UI
    if random.random() < 0.3:
        logger.warning("Transient validation error — will retry")
        raise ValueError("Simulated transient validation failure")

    invalid = [r for r in records if r["revenue"] <= 0 or r["subscribers"] <= 0]
    if invalid:
        logger.warning(f"Found {len(invalid)} invalid records, dropping them")

    valid = [r for r in records if r not in invalid]
    logger.info(f"Validation passed: {len(valid)} valid records")
    return valid


@task(
    name="Compute Region Summary",
    description="Aggregates revenue and subscribers by region",
    tags=["aggregation"],
    cache_policy=TASK_SOURCE,   # cache based on task source code — re-use if unchanged
)
def compute_region_summary(records: list[dict]) -> list[dict]:
    logger = get_run_logger()

    summary = {}
    for r in records:
        region = r["region"]
        if region not in summary:
            summary[region] = {"region": region, "total_revenue": 0, "total_subscribers": 0, "record_count": 0}
        summary[region]["total_revenue"] += r["revenue"]
        summary[region]["total_subscribers"] += r["subscribers"]
        summary[region]["record_count"] += 1

    result = []
    for region, data in summary.items():
        data["avg_revenue_per_sub"] = round(data["total_revenue"] / data["total_subscribers"], 2)
        data["total_revenue"] = round(data["total_revenue"], 2)
        result.append(data)

    result.sort(key=lambda x: x["total_revenue"], reverse=True)
    logger.info(f"Summarized {len(result)} regions")
    return result


@task(
    name="Detect High Churn",
    description="Flags records where churn rate exceeds threshold",
    tags=["analysis", "alerts"],
)
def detect_high_churn(records: list[dict], threshold: float = 0.10) -> list[dict]:
    logger = get_run_logger()

    high_churn = [r for r in records if r["churn_rate"] > threshold]
    logger.info(f"Found {len(high_churn)} high-churn records (threshold={threshold})")

    if high_churn:
        logger.warning(f"HIGH CHURN ALERT: {len(high_churn)} records exceed {threshold*100:.0f}% churn")

    return high_churn


@task(
    name="Compute Product Mix",
    description="Revenue share by product",
    tags=["aggregation"],
)
def compute_product_mix(records: list[dict]) -> list[dict]:
    logger = get_run_logger()

    product_rev = {}
    total = 0
    for r in records:
        p = r["product"]
        product_rev[p] = product_rev.get(p, 0) + r["revenue"]
        total += r["revenue"]

    result = [
        {
            "product": p,
            "revenue": round(rev, 2),
            "share_pct": round(rev / total * 100, 1),
        }
        for p, rev in product_rev.items()
    ]
    result.sort(key=lambda x: x["revenue"], reverse=True)
    logger.info(f"Product mix computed across {len(result)} products")
    return result


@task(
    name="Publish Region Artifact",
    description="Creates a markdown artifact visible in the Prefect UI",
    tags=["reporting"],
)
def publish_region_artifact(summary: list[dict]) -> None:
    rows = "\n".join(
        f"| {r['region']} | {r['total_revenue']:,.2f} | {r['total_subscribers']:,} | {r['avg_revenue_per_sub']:,.2f} | {r['record_count']} |"
        for r in summary
    )

    markdown = f"""# Regional Revenue Summary

Generated at: `{datetime.utcnow().isoformat()} UTC`

| Region | Total Revenue | Subscribers | Avg Rev/Sub | Records |
|--------|--------------|-------------|-------------|---------|
{rows}

> Top region: **{summary[0]['region']}** with revenue {summary[0]['total_revenue']:,.2f}
"""

    create_markdown_artifact(
        key="region-summary",
        markdown=markdown,
        description="Regional revenue breakdown",
    )


@task(
    name="Publish Churn Artifact",
    description="Creates a table artifact for high-churn records",
    tags=["reporting", "alerts"],
)
def publish_churn_artifact(high_churn: list[dict]) -> None:
    if not high_churn:
        create_markdown_artifact(
            key="churn-alert",
            markdown="# Churn Alert\n\nNo high-churn records detected. All good.",
        )
        return

    create_table_artifact(
        key="churn-alert",
        table=high_churn,
        description=f"High churn records — {len(high_churn)} flagged",
    )


@task(
    name="Slow Task",
    description="Simulates a slow operation so you can see running state in the UI",
    tags=["demo"],
)
def slow_task(seconds: int = 5) -> str:
    logger = get_run_logger()
    logger.info(f"Sleeping for {seconds}s — watch the UI show this task as Running")
    time.sleep(seconds)
    logger.info("Slow task complete")
    return f"Slept for {seconds}s"


# ---------------------------------------------------------------------------
# Subflow
# ---------------------------------------------------------------------------

@flow(
    name="Product Analysis Subflow",
    description="Computes product mix — runs as a nested subflow",
)
def product_analysis_subflow(records: list[dict]) -> list[dict]:
    logger = get_run_logger()
    logger.info("Starting product analysis subflow")
    mix = compute_product_mix(records)
    logger.info(f"Top product: {mix[0]['product']} at {mix[0]['share_pct']}% revenue share")
    return mix


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(
    name="FiberX Demo Pipeline",
    description="Demonstrates Prefect 3 features: tasks, subflows, retries, caching, artifacts, logging",
    version="1.0.0",
    log_prints=True,
)
def fiberx_demo_pipeline(
    n_records: int = 30,
    churn_threshold: float = 0.10,
    slow_seconds: int = 4,
):
    logger = get_run_logger()
    logger.info("=" * 50)
    logger.info("FiberX Demo Pipeline starting")
    logger.info(f"  records={n_records}, churn_threshold={churn_threshold}")
    logger.info("=" * 50)

    # Step 1 — generate data
    raw = generate_sales_data(n_records)

    # Step 2 — validate (has retries, may fail once or twice — watch the UI)
    valid = validate_data(raw)

    # Step 3 — run analysis tasks concurrently using .submit()
    region_future = compute_region_summary.submit(valid)
    churn_future = detect_high_churn.submit(valid, churn_threshold)

    # Step 4 — slow task runs while the above are in flight (visible in UI timeline)
    slow_task(slow_seconds)

    # Step 5 — collect results
    region_summary = region_future.result()
    high_churn = churn_future.result()

    # Step 6 — nested subflow (shows up separately in UI under this run)
    product_mix = product_analysis_subflow(valid)

    # Step 7 — publish artifacts (visible in UI under Artifacts tab)
    publish_region_artifact(region_summary)
    publish_churn_artifact(high_churn)

    # Step 8 — final summary log
    logger.info("=" * 50)
    logger.info("Pipeline complete")
    logger.info(f"  Valid records : {len(valid)}")
    logger.info(f"  Regions found : {len(region_summary)}")
    logger.info(f"  High churn    : {len(high_churn)}")
    logger.info(f"  Top region    : {region_summary[0]['region']}")
    logger.info(f"  Top product   : {product_mix[0]['product']} ({product_mix[0]['share_pct']}%)")
    logger.info("=" * 50)

    return {
        "valid_records": len(valid),
        "regions": len(region_summary),
        "high_churn_count": len(high_churn),
        "top_region": region_summary[0]["region"],
        "top_product": product_mix[0]["product"],
    }


if __name__ == "__main__":
    fiberx_demo_pipeline()