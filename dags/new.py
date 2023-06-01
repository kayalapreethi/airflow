import airflow
from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

args = {
    'owner': airflow,
    'start_date': datetime(2023, 5, 30)
}
dag_psql = DAG(
    dag_id="new_dag",
    default_args=args,
    schedule_interval='0 11 * * *',
    description='use case of psql operator in airflow',
    start_date=datetime(2023, 5, 30)
)

emp_details = PostgresOperator(
    task_id='emp_details',
    sql='''CREATE TABLE new_emp(
           emp_id integer, 
           emp_name character varying(10)
           );
           INSERT INTO new_emp 
           Values (1,'Mohan'),(2,'Shyam'),(3,'Raj'),(4,'Virat'),(5,'Rohan');,
    postgres_conn_id = "postgres_local",
    dag = dag_psql'''
)

new = PostgresOperator(
    task_id='new',
    sql='''CREATE TABLE new(
            emp_id integer, 
            emp_name character varying(10)
            );
            INSERT INTO new_emp 
            select * from new_emp;''',
    postgres_conn_id="postgres_local",
    dag=dag_psql
)

emp_details >> new

