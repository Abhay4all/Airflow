try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    print("All DAG Module are OK ......")

except exception as e:
    print("Error {} ".format(e))

def first_function_execute(**context):
    print("first function execute")
    context['ti'].xcom_push(key="mkey",value="first function execute says hello")

def second_function_execute(**context):
    instance=context.get("ti").xcom_pull(key="mkey")
    print("i am in second function got value :{} from function_1".format(instance))

with DAG(
    dag_id="first_dag",
    schedule_interval="@daily",
    default_args={
        "owner":"airflow",
        "retries":1,
        "retry_delay":timedelta(minutes=5),
        "start_date":datetime(2022,11,1),
    },
    catchup=False) as f:

    first_function_execute=PythonOperator(
        task_id="first_function_execute",
        python_callable=first_function_execute,
        op_kwargs={"name":"abhay pratap singh"},
        provide_context=True
    )

    second_function_execute=PythonOperator(
        task_id="second_function_execute",
        python_callable=second_function_execute,
        provide_context=True
    )
first_function_execute>>second_function_execute