import yaml

def load_config(file_path:str):
    """Load configuration from a YAML file.
    Args:
        file_path (str): Path to the YAML configuration file.
    Returns:
        dict: Configuration dictionary.

        
    web_craw_config: 
        world_bank_yaml: /mnt/c/Users/Admin/Downloads/Project/Github/ETL_Project/internal/config/web_craw_config/world_bank_config.yaml
        cophieu68_yaml: /mnt/c/Users/Admin/Downloads/Project/Github/ETL_Project/internal/config/web_craw_config/cophieu68_config.yaml

    """
    with open(file_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    return config

# if __name__ == "__main__":
#     # Example usage
#     config_path = "./platforms/processing/prefect/config/cophieu68_config.yaml"
#     config = load_config(config_path)
#     print(config)