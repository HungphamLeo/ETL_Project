from dataclasses import dataclass

@dataclass
class EconomyTransform:
    """
    Transform rules for Economy dataframe.
    """
    ECONOMY_DF_RULES = {
        "switch_pyspark": 100000,
        "transform_economy_dataframe": {
            "Generation_ID": True,
            "Longitude": {
                "round": 3,
                "replace_null_with": ""
            },
            "Latitude": {
                "round": 3,
                "replace_null_with": ""
            },
            "drop_columns": ["Aggregate"]
        },
        "transform_economy_metadata": {
            "Two_Alpha_Code": True, 
            "Balance_Of_Payments_Manual_In_Use": True,
            "Currency_Unit": True,
            "IMF_Data_Dissemination_Standard": True,
            "Income_Group": True,
            "Latest_Population_Census": True,
            "Latest_Trade_Data": True,
            "Long_Name": True,
            "drop_columns": []
        }
    }

@dataclass
class SeriesTransform:
    """
    Transform rules for Series dataframe.
    """
    SERIES_DF_RULES = {
        "switch_pyspark": 100000,
        "transform_series_dataframe": {
            "Series_ID": True,
            "Topic_Name": True,
            "drop_columns": []
        },
        "transform_series_metadata": {
            "Aggregationmethod": True,
            "Dataset": True,
            "Developmentrelevance": True,
            "Generalcomments": True,
            "IndicatorName": True,
            "License_Type": True,
            "License_URL": True,
            "Limitationsandexceptions": True,
            "Longdefinition": True,
            "Periodicity": True,
            "Source": True,
            "Statisticalconceptandmethodology": True,
            "Topic": True,
            "Unitofmeasure": True,
            "drop_columns": []
        }
    }

@dataclass
class TopicTransform:
    """
    Transform rules for Topic dataframe.
    """
    TOPIC_DF_RULES = {
        "switch_pyspark": 100000,
        "transform_topic_info_dataframe": {
            "Topic_ID": True,
            "Topic_Name": True,
            "Topic_Description": True,
            "drop_columns": []
        },
        "transform_topic_series_dataframe": {
            "Topic_Name": True,
            "Topic_ID": True,
            "drop_columns": []
        },
        "transform_topic_metadata_dataframe": {
            "Member_ID": True,
            "Series_ID": True,
            "drop_columns": []
        }
    }

@dataclass
class TimeTransform:
    """
    Transform rules for Time dataframe.
    """
    TIME_DF_RULES = {
        "switch_pyspark": 100000,
        "transform_time_dataframe": {
            "Year": True,
            "Year_Time_ID": True,
            "drop_columns": []
        }
    }

@dataclass
class SourceTransform:
    """
    Transform rules for Source dataframe.
    """
    SOURCE_DF_RULES = {
        "switch_pyspark": 100000,
        "transform_source_info_dataframe": {
            "Source_ID": True,
            "Last_Updated": True,
            "Source_Name": True,
            "Code": True,
            "Data_ID": True,
            "Description": True,
            "URL": True,
            "Data_Availability": True,
            "Metadata_Availability": True,
            "Source_Concept": True,
            "drop_columns": []
        },
        "transform_source_series_dataframe": {
            "Source_Name": True,
            "Source_ID": True,
            "drop_columns": []
        }
    }

@dataclass
class RegionTransform:
    """
    Transform rules for Region dataframe.
    """
    REGION_DF_RULES = {
        "switch_pyspark": 100000,
        "transform_region_dataframe": {
            "Region_Code": True,
            "Series_Region_Name": True,
            "drop_columns": []
        }
    }

@dataclass
class IncomeTransform:
    """
    Transform rules for Income dataframe.
    """
    INCOME_DF_RULES = {
        "switch_pyspark": 100000,
        "transform_income_dataframe": {
            "Income_Group_Name": True,
            "drop_columns": []
        }
    }

@dataclass
class LendingTransform:
    """
    Transform rules for Lending dataframe.
    """
    LENDING_DF_RULES = {
        "switch_pyspark": 100000,
        "transform_lending_dataframe": {
            "Lending_Type_Name": True,
            "drop_columns": []
        }
    }