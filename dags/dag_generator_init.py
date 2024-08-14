import os 
import logging

from dag_generator import create_dag

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

dags_folder = '/home/ubuntu/airflow/dags'

for root, dirs, files in os.walk(dags_folder):
    for file in files:
        if file.endswith('.yml'):
            scrape_dir_path = root
            yml_file_path = os.path.join(scrape_dir_path, file)
            dag_id = os.path.basename(scrape_dir_path)
            
            try:
                dag = create_dag(yml_file_path)
                globals()[dag_id] = dag
            except Exception as e:
                logger.error(f"Error loading DAG {dag_id}: {str(e)}")
            