import yaml

def load_config(file_path = '/mnt/c/Users/Admin/Downloads/Project/Github/ETL_Project/internal/config/data_craw_web_config/data_craw_web_config.yaml'):
    with open(file_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    return config

