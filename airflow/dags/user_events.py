from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="user_events",
    schedule_interval=None,
    start_date = days_ago(1),
    catchup=False,
    tags=["user events", "simulation"],
) as dag:

    simulate_user_events = BashOperator(
        task_id="simulate_user_events",
        bash_command=(
            "python3 /opt/project/scripts/streaming/user_events_simulation.py"
        ),
    )

    simulate_user_events
