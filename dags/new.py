import airflow
from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="postgres_operator_dag",
    start_date=datetime(2023, 6, 1),
    schedule_interval="15 11 * * *",
    catchup=False,
) as dag:
    emp_details = PostgresOperator(
        task_id="emp_details",
        sql="""
            CREATE TABLE new_emp(
            emp_id integer, 
            emp_name character varying(10)
            );
            INSERT INTO new_emp 
            Values (1,'Mohan'),(2,'Shyam'),(3,'Raj'),(4,'Virat'),(5,'Rohan');
          """,
    )
    new = PostgresOperator(
        task_id='new',
        sql='''
            CREATE TABLE new(
            emp_id integer, 
            emp_name character varying(10)
            );
            INSERT INTO new_emp 
            select * from new_emp;
            '''
    )
    emp_details >> new

