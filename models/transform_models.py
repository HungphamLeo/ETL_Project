
from dataclasses import dataclass

@dataclass
class EconomyTransformDataFrameOutput:
    ECONOMY_DF_RULES = {
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