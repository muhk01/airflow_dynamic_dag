# airflow_dynamic_dag
Edit for function call to execute spesific Script
```
dag_ingestion_table1 = generate_ingestion_dag('table_data1.sh')
dag_ingestion_table2 = generate_ingestion_dag('table_data2.sh')
dag_ingestion_table3 = generate_ingestion_dag('table_data3.sh', 'serving_data1.sh')
dag_ingestion_table4 = generate_ingestion_dag('table_data4.sh', 'serving_data2.sh')
```

Sample to Create Dynamic DAG with Airflow

![Preview](https://raw.githubusercontent.com/muhk01/airflow_dynamic_dag/main/img/dynamic_dag.PNG)
