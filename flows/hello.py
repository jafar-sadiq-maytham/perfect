from prefect import flow, task
import httpx

@task
def fetch_data(url: str) -> dict:
    response = httpx.get(url)
    return response.json()

@flow(log_prints=True)
def hello_flow(name: str = "world"):
    print(f"Hello, {name}!")
    data = fetch_data("https://httpbin.org/get")
    print(f"Got response: {data['url']}")

if __name__ == "__main__":
    hello_flow()
