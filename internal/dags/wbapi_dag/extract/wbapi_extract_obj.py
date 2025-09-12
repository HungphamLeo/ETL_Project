import wbgapi as wb
import os
# print("PYTHONPATH =", os.environ.get("PYTHONPATH"))
from src.utils import dataframe_rename_by_dataclass
from internal.models.wbgapi_model import *



class base_extract_logger_obj:
    def __init__(self, pipeline_logger):
        self.logger = pipeline_logger


class wbapi_series(base_extract_logger_obj):
    def __init__(self, pipeline_logger):
        super().__init__(pipeline_logger)
        self.series = wb.series

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


class wbapi_economy(base_extract_logger_obj):
    def __init__(self, pipeline_logger):
        super().__init__(pipeline_logger)
        self.economy = wb.economy
    
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
             
        except Exception as e:
            self.logger.error(f"Error fetching metadata for economy {input.id}: {e}")
            return None

    
class wbapi_topic(base_extract_logger_obj):
    def __init__(self, pipeline_logger):
        super().__init__(pipeline_logger)
        self.topic = wb.topic

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

class wbapi_time(base_extract_logger_obj):
    def __init__(self, pipeline_logger):
        super().__init__(pipeline_logger)
        self.time = wb.time

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


class wbapi_source(base_extract_logger_obj):
    def __init__(self, pipeline_logger):
        super().__init__(pipeline_logger)
        self.source = wb.source

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


class wbapi_region(base_extract_logger_obj):
    def __init__(self, pipeline_logger):
        super().__init__(pipeline_logger)
        self.region = wb.region

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


class wbapi_income(base_extract_logger_obj):
    def __init__(self, pipeline_logger):
        super().__init__(pipeline_logger)
        self.income = wb.income

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

class wbapi_lending(base_extract_logger_obj):
    def __init__(self, pipeline_logger):
        super().__init__(pipeline_logger)
        self.lending = wb.lending

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
