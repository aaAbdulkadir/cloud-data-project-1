import os 

from dag_generator import create_dag


dags_folder = '/home/ubuntu/airflow/dags'

for root, dirs, files in os.walk(dags_folder):
    for file in files:
        if file.endswith('.yml'):
            scrape_dir_path = root
            yml_file_path = os.path.join(scrape_dir_path, file)
            dag_id = os.path.basename(scrape_dir_path)
            globals()[dag_id] = create_dag(yml_file_path)