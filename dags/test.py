from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import time

ws = WorkspaceClient(
    host="your_databricks_host",
    token="your_databricks_token")

job_trigger = ws.jobs.run_now(job_id = 412792872408110)

while True:
    job_status = ws.jobs.get_run(
        run_id=job_trigger.run_id
    )

    print(
        f"Job run status: {job_status.state.life_cycle_state}, "
        f"result state: {job_status.state.result_state}"
    )
    
    if job_status.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
        if job_status.state.result_state == RunResultState.SUCCESS:
            print("Job completed successfully.")
            break
        else:
            raise Exception(f"Job failed with state: {job_status.state.result_state}")

    time.sleep(5)  # Wait for 10 seconds before checking the status again

