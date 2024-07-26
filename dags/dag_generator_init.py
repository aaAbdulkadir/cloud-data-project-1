import os 

from custom_functions.dag_generator import create_dag


dags_folder = os.path.dirname(os.path.abspath(__file__))

for folder in os.listdir(dags_folder):
    folder_path = os.path.join(dags_folder, folder)
    
    if os.path.isdir(folder_path):
        yaml_file_path = os.path.join(folder_path, f'{folder}.yml')

        if os.path.isfile(yaml_file_path):
            dag = create_dag(folder_path)
            globals()[dag.dag_id] = dag