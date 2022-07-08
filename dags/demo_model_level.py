import datetime
import json
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta
import airflow.operators.bash


dag = DAG(  # instantiating a dag
    dag_id='dbt_dag',
    start_date=datetime(2022, 7, 1), # yyyy/mm/dd
    description='A dbt wrapper for Airflow', # for purpose of us using a dbt dag in airflow
    schedule_interval=timedelta(days=1),
)
def load_manifest(): # loading our dbt dag into the airflow environment
    local_filepath = "/home/keenan/code/integrating_dbt_and_airflow/manifest.json" # /usr/local/airflow/dags/dbt/target/manifest.json
    with open(local_filepath) as f:
        data = json.load(f)

    return data
# each model now maps to 2 different tasks
def make_dbt_task(node, dbt_verb):
    """Returns an Airflow operator either run and test an individual model"""
    DBT_DIR = "/home/keenan/code/integrating_dbt_and_airflow" # directory of the dbt database   /usr/local/airflow/dags/dbt
    GLOBAL_CLI_FLAGS = "--no-write-json"
    model = node.split(".")[-1] 

    if dbt_verb == "run":
        dbt_task = BashOperator(
            task_id=node,  # f string is formatted string which parses variables
            bash_command=f"""
            cd {DBT_DIR} &&
            dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target prod --models {model}
            """,
            dag=dag,
        )

    elif dbt_verb == "test":
        node_test = node.replace("model", "test")
        dbt_task = BashOperator(
            task_id=node_test,
            bash_command=f"""
            cd {DBT_DIR} &&
            dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target prod --models {model}
            """,
            dag=dag,
        )

    return dbt_task

data = load_manifest()

dbt_tasks = {} # creating a list of tasks
for node in data["nodes"].keys():
    if node.split(".")[0] == "model":
        node_test = node.replace("model", "test")

        dbt_tasks[node] = make_dbt_task(node, "run")  # this is where we assign the 2 tasks per node (node represents a table)
        dbt_tasks[node_test] = make_dbt_task(node, "test")

for node in data["nodes"].keys():
    if node.split(".")[0] == "model":

        # Set dependency to run tests on a model after model runs finishes
        node_test = node.replace("model", "test")
        dbt_tasks[node] >> dbt_tasks[node_test]  # it compiles the task first and then it runs tests on it

        # Set all model -> model dependencies
        for upstream_node in data["nodes"][node]["depends_on"]["nodes"]:

            upstream_node_type = upstream_node.split(".")[0]  # once we have set the tests and runs dependant of each other, we now chain all the runs to each other
            if upstream_node_type == "model":
                dbt_tasks[upstream_node] >> dbt_tasks[node]