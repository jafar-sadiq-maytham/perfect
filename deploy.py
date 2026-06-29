from prefect.deployments import Deployment
from prefect.runner.storage import GitRepository
from prefect.blocks.system import Secret

if __name__ == "__main__":
    # For private repos, use your token
    #source = GitRepository(
      #  url="https://github.com/your-org/your-repo.git",
     #   credentials={"access_token": "ghp_your_token_here"},  # or use a Prefect Secret block
    #)

    # For public repos
    source = GitRepository(url="https://github.com/jafar-sadiq-maytham/perfect")

    Deployment.build_from_flow(
        name="hello-flow-deployment",
        flow_name="hello-flow",           # matches @flow name (hyphenated)
        work_pool_name="default-process-pool",
        storage=source,
        entrypoint="flows/hello.py:hello_flow",   # path:function
        schedule={"cron": "0 6 * * *"},   # 6am daily, or None for manual only
        parameters={"name": "Prefect"},
    ).apply()
