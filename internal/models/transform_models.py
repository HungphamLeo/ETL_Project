
from dataclasses import dataclass

@dataclass
class EconomyTransformDataFrameOutput:
    """
    Table dataframe economic
    'Country_Name', 'Aggregate', 'Longitude', 'Latitude', 'Region',
       'Admin_Region', 'Lending_Type', 'Income_Level', 'Capital_Of_Country',
       'Country_ID'
    """
    ECONOMY_DF_RULES = {
        "switch_pyspark": 100000,

        "transform_economy_dataframe": {
            "Longitude": {
                "round": 3,      
                "replace_null_with": ""
            },
            "Latitude": {
                "round": 3,
                "replace_null_with": ""
            },
            "drop_columns": ["Aggregate"]
        }

        
    }