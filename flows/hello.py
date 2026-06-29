import random
from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact


@task(name="Generate Numbers", retries=1, retry_delay_seconds=2)
def generate_numbers(count: int) -> list[int]:
    logger = get_run_logger()
    numbers = [random.randint(1, 100) for _ in range(count)]
    logger.info(f"Generated: {numbers}")
    return numbers


@task(name="Compute Stats")
def compute_stats(numbers: list[int]) -> dict:
    logger = get_run_logger()
    stats = {
        "count": len(numbers),
        "total": sum(numbers),
        "average": round(sum(numbers) / len(numbers), 2),
        "min": min(numbers),
        "max": max(numbers),
    }
    logger.info(f"Stats: {stats}")
    return stats




@flow(name="Simple Demo", log_prints=True)
def simple_demo(count: int = 10):
    numbers = generate_numbers(count)
    stats = compute_stats(numbers)
    print(f"Done! Average was {stats['average']}")


if __name__ == "__main__":
    simple_demo()