
from dataclasses import dataclass

@dataclass
class EconomyTransformDataFrameOutput:
    ECONOMY_DF_RULES = {
        "switch_pyspark": 100000,
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