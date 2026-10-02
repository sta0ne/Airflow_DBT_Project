from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import pendulum
import time


@dag
def orchestrate():

    @task
    def ingest_cdc():
        @dag(
        dag_id="orchestrate",
        schedule="0 11 * * *",
        catchup=False,
        start_date=pendulum.datetime(year=2026, month=9, day=14, tz="Asia/Kolkata")
)
        def orchestrate():

                @task
                def ingest_cdc():
                    ws = WorkspaceClient(
                    host="your-databricks host",
                    token="your-databricks-token")

                    job_trigger = ws.jobs.run_now(job_id = "your-job-id")

                    while True:
                        job_status = ws.jobs.get_run(
                            run_id=job_trigger.run_id
                        )

                        
                        
                        if job_status.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                            if job_status.state.result_state == RunResultState.SUCCESS:
                                print("Job completed successfully.")
                                break
                            else:
                                raise Exception(f"Job failed with state: {job_status.state.result_state}")

                        time.sleep(5)  # Wait for 10 seconds before checking the status again

                    return "CDC ingestion completed successfully."

    @task.bash
    def source_freshness():
        return (
                "dbt source freshness "
                "--profiles-dir /opt/airflow/pharma_project "
                "--project-dir /opt/airflow/pharma_project"
    )
    
    silver_technical = BashOperator(
    task_id="silver_technical",
    cwd="/opt/airflow/pharma_project",
    bash_command="dbt run --select silver_t"
   )
    silver_tecnical_test = BashOperator(
        task_id="silver_tecnical_test",
        cwd="/opt/airflow/pharma_project",
        bash_command=
            "dbt test --select silver_t "
    )
    silver_business = BashOperator(
        task_id="silver_business",
        cwd="/opt/airflow/pharma_project",
        bash_command="dbt run --select silver_b"
    )

    silver_business_test = BashOperator(
        task_id="silver_business_test",
        cwd="/opt/airflow/pharma_project",
        bash_command="dbt test --select silver_b"
    )
    gold = BashOperator(
        task_id="gold",
        cwd="/opt/airflow/pharma_project",
        bash_command="dbt run --select gold"
    )
    gold_test = BashOperator(
        task_id="gold_test",
        cwd="/opt/airflow/pharma_project",
        bash_command="dbt test --select gold"
    )
    ingest_cdc() >> source_freshness() >> silver_technical >> silver_tecnical_test >> silver_business >> silver_business_test >> gold >> gold_test

orchestrate_dag = orchestrate()