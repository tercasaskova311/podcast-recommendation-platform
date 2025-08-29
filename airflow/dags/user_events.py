from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import yaml

with open('/opt/airflow/config/schedule_config.yaml') as f:
    config = yaml.safe_load(f)

with DAG(
    dag_id="user_events",
    schedule_interval=config['simulate_user_events_interval'],
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
