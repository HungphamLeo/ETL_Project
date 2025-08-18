import pandas as pd
from dataclasses import dataclass, asdict
from typing import Optional
from decimal import Decimal




# Series
@dataclass
class SeriesFetchOutput:
    Series_ID: str
    Topic_Name: str

@dataclass
class SeriesMetadataOutput:
    Aggregationmethod: Optional[str]
    Dataset: Optional[str]
    Developmentrelevance: Optional[str]
    Generalcomments: Optional[str]
    IndicatorName: Optional[str]
    License_Type: Optional[str]
    License_URL: Optional[str]
    Limitationsandexceptions: Optional[str]
    Longdefinition: Optional[str]
    Periodicity: Optional[str]
    Source: Optional[str]
    Statisticalconceptandmethodology: Optional[str]
    Topic: Optional[str]
    Unitofmeasure: Optional[str]




# Economy
@dataclass
class EconomyDataFrameOuput:
    Country_Name: str
    Aggregate: bool
    Longitude: Decimal 
    Latitude: Decimal
    Region: str	
    Admin_Region: str	
    Lending_Type: str
    Income_Level : str
    Capital_Of_Country: str
    Country_ID: str
    


@dataclass
class EconomyMetadataOuput:
    Two_Alpha_Code: str
    Balance_Of_Payments_Manual_In_Use: str
    Currency_Unit: str
    IMF_Data_Dissemination_Standard: str
    Income_Group: str
    Latest_Population_Census: str
    Latest_Trade_Data: str
    Long_Name: str
    National_Accounts_Base_Year: str
    PPP_Survey_Year: str
    Region: str
    Short_Name: str
    SNA_Price_Valuation: str
    System_Of_National_Accounts: str
    System_Of_Trade: str
    Table_Name: str
    Vital_Registration_Complete: str
    WB_Two_Code: str


#topic
@dataclass
class TopicSeriesOutput:
    Topic_Name: str
    Topic_ID: int
    
@dataclass
class TopicInfoOutput:
    Topic_ID: int
    Topic_Name: str
    Topic_Description: str

@dataclass
class TopicMembersOutput:
    Member_ID: int
    Series_ID: int



#time
@dataclass
class TimeSeriesOutput:
    Year: int
    Year_Time_ID: str
    

#source
@dataclass
class SourceSeriesOutput:
    Source_Name: str
    Source_ID: int

@dataclass
class SourceInfoOutput:
    Source_ID: int
    Last_Updated: str
    Source_Name: str
    Code: str
    Data_ID:str
    Description: str
    URL: str
    Data_Availability:str
    Metadata_Availability:str
    Source_Concept:int
    


#region
@dataclass
class RegionSeriesOutput:
    Series_Region_Name: str
    Region_Code: str


#income
@dataclass
class IncomeSeriesOutput:
    Income_Group_Name: str
    Income_Group_ID: int


# lending

@dataclass
class LendingSeriesOutput:
    Lending_Type_Name: str
    Lending_Type_ID: str