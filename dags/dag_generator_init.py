import os 

from dag_generator import create_dag


dags_folder = '/home/ubuntu/airflow/dags'

for root, dirs, files in os.walk(dags_folder):
    for file in files:
        if file.endswith('.yml'):
            scrape_dir_path = root  
            dag_id = os.path.basename(scrape_dir_path)
            globals()[dag_id] = create_dag(scrape_dir_path)