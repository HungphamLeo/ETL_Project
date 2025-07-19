import wbgapi as wb
import os
print("PYTHONPATH =", os.environ.get("PYTHONPATH"))
from utils import dataframe_rename_by_dataclass
from internal.config.load_config import load_config
from src.logger import FastLogger
from internal.models import *

class wbapi_series:
    def __init__(self):
        self.series = wb.series
        self.logger = FastLogger(load_config()).get_logger()

    def get_series_metadata(self, input: SeriesMetadataInput):
        """
        Returns metadata for a specific series.

        Args:
            input (SeriesMetadataInput): object with id, economic, time and db
                attributes.

        Returns:
            a dictionary with metadata for the given series
        """

        try:
            res_temp =  self.series.metadata.get(
                id = input.id, economies=input.economies, time=input.time, db=input.db
            )
            meta_dict = res_temp.metadata
            res = pd.DataFrame([meta_dict])            
            return dataframe_rename_by_dataclass(res, SeriesMetadataOutput)
        except Exception as e:
            # print(e)
            self.logger.error(f"Error fetching metadata for series {input.id}: {e}")
            return None

    def get_info(self, input: SeriesInfoInput):
        """
        Retrieve information about a specific series.

        Args:
            input (SeriesInfoInput): object with id, q, topic, and db attributes.

        Returns:
            a dictionary with information about the specified series or None if there is an error
        """
        try:
            res_temp = self.series.info(input.id, input.q, input.topic, input.db)
            res = pd.DataFrame(res_temp.items)
            return dataframe_rename_by_dataclass(res, SeriesInfoOutput)
        except Exception as e:
            self.logger.error(f"Error fetching series info for {input.id}: {e}")
            return None

    def get_data(self, input: SeriesDataInput):
        """
        Retrieve data for a specific series.

        Arguments:
            input: A SeriesDataInput object containing the series ID and database.

        Returns:
            The data for the specified series or None if there is an error.
        """

        try:
            res_temp = self.series.get(input.id, input.db)
            res = pd.DataFrame([res_temp])
            return dataframe_rename_by_dataclass(res, SeriesDataOutput)
        except Exception as e:
            self.logger.error(f"Error fetching data for series {input.id}: {e}")
            return None

    def get_series(self, input: SeriesGetInput):
        """
        Retrieve information about a specific series.

        Args:
            input (SeriesFetchInput): object with id, q, topic, db, and name attributes.

        Returns:
            a dictionary with information about the specified series or None if there is an error
        """
        try:
            res = self.series.Series(input.id, input.q, input.topic, input.db, input.name).to_frame().reset_index()
            return dataframe_rename_by_dataclass(res, SeriesFetchOutput)
        except Exception as e:
            self.logger.error(f"Error fetching series info for {input.id}: {e}")
            return None


class wbapi_economy:
    def __init__(self):
        self.economy = wb.economy
        self.logger = FastLogger(load_config()).get_logger()
    
    def check_coder(self, input: EconomyCheckCoderInput):
        try:
            return self.economy.coder(input.name, input.summary,input.debug)
        except Exception as e:
            self.logger.error(f"Error fetching coder for {input.id}: {e}")
            return None

    
    def dataframe_display(self, input:EconomyDataFrameInput):
        """
        Retrieve a pandas DataFrame containing data for a specific economy.

        Args:
            input (EconomyDataFrameInput): An object containing id, labels, skipAggs, and db attributes.

        Returns:
            A pandas DataFrame with data for the specified economy or None if there is an error.
        """
        try:
            res = self.economy.DataFrame(input.id, input.labels, input.skipAggs, input.db)
            id_columns = list(res.index)
            res['id']= id_columns
            return dataframe_rename_by_dataclass(res.reset_index(drop=True), EconomyDataFrameOuput)
        except Exception as e:
            self.logger.error(f"Error fetching dataframe for {input.id}: {e}")
            return None


        
    def get_info(self, input: EconomyInfoInput):
        """
        Retrieve information about a specific economy.

        Args:
            input (EconomyInfoInput): An object containing id, q, skipAggs, and db attributes.

        Returns:
            A dictionary with information about the specified economy or None if there is an error.
        """

        try:
            res = self.economy.info(input.id, input.q, input.skipAggs, input.db)
            df = pd.DataFrame(res.items)
            return dataframe_rename_by_dataclass(df, EconomyInfoOutput)
            
        except Exception as e:
            self.logger.error(f"Error fetching economy info for {input.id}: {e}")
            return None

    def get_data(self, input: EconomyGetInput):
        """
        Retrieve data for a specific economy.

        Args:
            input (EconomyGetInput): An object containing id, labels, and db attributes.

        Returns:
            A dictionary with data for the specified economy or None if there is an error.
        """
        try:
            res = self.economy.get(input.id, input.labels, input.db)
            df = pd.DataFrame([res])
            return dataframe_rename_by_dataclass(df, EconomyGetOutput)
        except Exception as e:
            self.logger.error(f"Error fetching economy data for {input.id}: {e}")
            return None

    def get_series(self, input: EconomySeriesInput):
        """
        Retrieve a list of series for a specific economy.

        Args:
            input (EconomySeriesInput): An object containing id, q, skipAggs, db, and name attributes.

        Returns:
            A list of series matching the given criteria or None if there is an error.
        """

        try:
            res = self.economy.Series(input.id, input.q, input.skipAggs, input.db, input.name).to_frame()
            id_columns = list(res.index)
            res['id']= id_columns
            return dataframe_rename_by_dataclass(res.reset_index(drop=True), EconomySeriesOutput)
        except Exception as e:
            self.logger.error(f"Error fetching economy series for {input.id}: {e}")
            return None

    def get_metadata(self, input: EconomyMetadataInput):
        """
        Retrieve metadata for a specific economy.

        Args:
            input (EconomyMetadataInput): An object containing id, series, and db attributes.

        Returns:
            A dictionary with metadata for the specified economy or None if there is an error.
        """
        try:
            res = self.economy.metadata.get(input.id, series=input.series, db=input.db)
            df = pd.DataFrame([res.metadata])
            return dataframe_rename_by_dataclass(df, EconomyMetadataOuput)
            return 
        except Exception as e:
            self.logger.error(f"Error fetching metadata for economy {input.id}: {e}")
            return None

    
class wbapi_topic:
    def __init__(self):
        self.topic = wb.topic
        self.logger = FastLogger(load_config()).get_logger()


    def get_info(self, input: TopicInfoInput):
        """
        Retrieve information about a specific topic.

        Args:
            input (TopicInfoInput): An object containing id and q attributes.

        Returns:
            A dictionary with information about the specified topic or None if there is an error.
        """
        try:
            res = self.topic.info(input.id, input.q)
            df = pd.DataFrame(res.items)
            return dataframe_rename_by_dataclass(df, TopicInfoOutput)
        except Exception as e:
            self.logger.error(f"Error fetching topic info for {input.id}: {e}")
            return None

    def get_series(self, input: TopicSeriesInput):
        """
        Retrieve a list of series for a specific topic.

        Args:
            input (TopicSeriesInput): An object containing id, q, and name attributes.

        Returns:
            A list of series matching the given criteria or None if there is an error.
        """
        
        try:
            res = self.topic.Series(input.id, input.q, input.name).to_frame()
            id_columns = list(res.index)
            res['id']= id_columns
            return dataframe_rename_by_dataclass(res.reset_index(drop=True), TopicSeriesOutput)

        except Exception as e:
            self.logger.error(f"Error fetching topic series for {input.id}: {e}")
            return None

    def get_members(self, input: TopicMembersInput):
        """
        Retrieve a list of members for a specific topic.

        Args:
            input (TopicMembersInput): An object containing `id` and `maximum_member_id`.

        Returns:
            A pandas DataFrame of members or None if error.
        """
        try:
            from dataclasses import fields
            import pandas as pd

            # Lấy tên các field từ dataclass TopicMembersOutput
            columns = [f.name for f in fields(TopicMembersOutput)]
            data = []

            for member_id in range(input.maximum_member_id):
                try:
                    series_list = self.topic.members(member_id)
                    if not series_list:
                        break  # Ngắt nếu không còn series nào

                    for series_id in series_list:
                        data.append({
                            "Member_ID": member_id,
                            "Series_ID": series_id
                        })

                except Exception as inner_e:
                    self.logger.warning(f"Error fetching member {member_id}: {inner_e}")
                    continue  # Bỏ qua lỗi từng member

            df = pd.DataFrame(data, columns=columns)
            return df

        except Exception as e:
            self.logger.error(f"Error fetching topic members for {input.id}: {e}")
            return None

class wbapi_time:
    def __init__(self):
        self.time = wb.time
        self.logger = FastLogger(load_config()).get_logger()


    def get_time_periods_series(self, input: TimeSeriesInput):
        """
        Retrieve a list of series for a specific time period.

        Args:
            input (TimeSeriesInput): An object containing id, q, db, and name attributes.

        Returns:
            A list of series matching the given criteria or None if there is an error.
        """
        try:
            res = self.time.Series(input.id, input.q, input.db, input.name).to_frame()
            id_columns = list(res.index)
            res['id']= id_columns
            
            return dataframe_rename_by_dataclass(res.reset_index(drop=True), TimeSeriesOutput)  
            
        except Exception as e:
            self.logger.error(f"Error fetching time series for {input.id}: {e}")
            return None


class wbapi_source:
    def __init__(self):
        self.source = wb.source
        self.logger = FastLogger(load_config()).get_logger()

    def get_info(self, input: SourceInfoInput):
        """
        Retrieve information about a specific source.

        Args:
            input (SourceInfoInput): An object containing id and q attributes.

        Returns:
            A dictionary with information about the specified source or None if there is an error.
        """
        
        try:
            res = self.source.info(input.id, input.q)
            df = pd.DataFrame(res.items)
            s = dataframe_rename_by_dataclass(df, SourceInfoOutput)
            return s
        except Exception as e:
            self.logger.error(f"Error fetching source info for {input.id}: {e}")
            return None

    def get_series(self, input: SourceSeriesInput):
        """
        Retrieve a list of series for a specific source.

        Args:
            input (SourceSeriesInput): An object containing id, q, and name attributes.

        Returns:
            A list of series matching the given criteria or None if there is an error.
        """

        try:
            res = self.source.Series(input.id, input.q, input.name).to_frame()
            id_columns = list(res.index)
            res['id']= id_columns
            return dataframe_rename_by_dataclass(res.reset_index(drop=True), SourceSeriesOutput)
             
        except Exception as e:
            self.logger.error(f"Error fetching source series for {input.id}: {e}")
            return None


    # def get_concepts(self, input: SourceConceptsInput):
    #     """
    #     Retrieve concepts for a specific source.

    #     Args:
    #         input (SourceConceptsInput): An object containing the db attribute.

    #     Returns:
    #         A list of concepts from the specified source or None if there is an error.
    #     """

    #     try:
    #         return self.source.concepts(input.db)
    #     except Exception as e:
    #         self.logger.error(f"Error fetching source concepts: {e}")
    #         return None


class wbapi_region:
    def __init__(self):
        self.region = wb.region
        self.logger = FastLogger(load_config()).get_logger()

    def get_series(self, input: RegionSeriesInput):
        """
        Retrieve a list of series for a specific region.

        Args:
            input (RegionSeriesInput): An object containing id, q, group, and name attributes.

        Returns:
            A list of series matching the given criteria or None if there is an error.
        """

        try:
            res = self.region.Series(input.id, input.q, input.group, input.name).to_frame()
            id_columns = list(res.index)
            res['id']= id_columns
            return dataframe_rename_by_dataclass(res.reset_index(drop=True), RegionSeriesOutput)
            
        except Exception as e:
            self.logger.error(f"Error fetching region series for {input.id}: {e}")
            return None


class wbapi_income:
    def __init__(self):
        self.income = wb.income
        self.logger = FastLogger(load_config()).get_logger()

    def get_series(self, input: IncomeSeriesInput):
        """
        Retrieve a list of series for a specific income level.

        Args:
            input (IncomeSeriesInput): An object containing id, q, and name attributes.

        Returns:
            A list of series matching the given criteria or None if there is an error.
        """

        try:
            res = self.income.Series(input.id, input.q, input.name).to_frame()
            id_columns = list(res.index)
            res['id']= id_columns
            return dataframe_rename_by_dataclass(res.reset_index(drop=True), IncomeSeriesOutput)
        except Exception as e:
            self.logger.error(f"Error fetching income series for {input.id}: {e}")
            return None

class wbapi_lending:
    def __init__(self):
        self.lending = wb.lending
        self.logger = FastLogger(load_config()).get_logger()

    def get_series(self, input: LendingSeriesInput):
        """
        Retrieve a list of series for a specific lending type.

        Args:
            input (LendingSeriesInput): An object containing id, q, and name attributes.

        Returns:
            A list of series matching the given criteria or None if there is an error.
        """

        try:
            res = self.lending.Series(input.id, input.q, input.name).to_frame()
            id_columns = list(res.index)
            res['id']= id_columns
            return dataframe_rename_by_dataclass(res.reset_index(drop=True), LendingSeriesOutput)
        except Exception as e:
            self.logger.error(f"Error fetching lending series for {input.id}: {e}")
            return None
