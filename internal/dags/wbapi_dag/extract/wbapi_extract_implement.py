from cmd_.load_config import load_config
from src.logger import FastLogger
from .wbapi_extract_obj import (
    wbapi_series, wbapi_economy, wbapi_topic,
    wbapi_time, wbapi_source, wbapi_region,
    wbapi_income, wbapi_lending
)
from datetime import datetime
from internal.models import *
from internal.dags.wbapi_dag.extract import wbapi_extract
import airflow.operators as ops


class wbapi_extract:
    def __init__(self):
        self.series = wbapi_series()
        self.economy = wbapi_economy()
        self.topic = wbapi_topic()
        self.time = wbapi_time()
        self.source = wbapi_source()
        self.region = wbapi_region()
        self.income = wbapi_income()
        self.lending = wbapi_lending()
        self.logger = FastLogger(load_config()).get_logger()


class WorldBankExtractOperator(ops.python.PythonOperator):
    """Custom operator cho World Bank API data extraction"""
    
    def __init__(self, pipeline_config, extract_type: str, **kwargs):
        self.extract_type = extract_type
        self.pipeline_config = pipeline_config
        self.params = kwargs.get('params', {})
        super().__init__(python_callable=self._extract_data, **kwargs)
    
    def _extract_data(self, **context):
        """Extract data từ World Bank API"""
        logger = self.pipeline_config.logger
        
        try:
            extract_obj = wbapi_extract()
            execution_date = context['execution_date']
            
            # Dynamic extraction based on type
            extraction_map = {
                'economy': self._extract_economy_data,
                'series': self._extract_series_data,
                'topic': self._extract_topic_data,
                'time': self._extract_time_data,
                'source': self._extract_source_data,
                'region': self._extract_region_data,
                'income': self._extract_income_data,
                'lending': self._extract_lending_data
            }
            
            if self.extract_type not in extraction_map:
                raise ValueError(f"Unknown extract type: {self.extract_type}")
            
            data = extraction_map[self.extract_type](extract_obj)
            
            return {
                'extract_type': self.extract_type,
                'record_count': len(data) if isinstance(data, list) else data.count() if hasattr(data, 'count') else 0,
                'extraction_timestamp': datetime.now().isoformat(),
                'data': data
            }
            
        except Exception as e:
            logger.error(f"Extraction failed for {self.extract_type}: {str(e)}")
            raise
    
    def _get_param(self, key: str, default_value='all'):
        """Get parameter from self.params with fallback to default"""
        return self.params.get(key, default_value)
    
    def _extract_economy_data(self, extract_obj: wbapi_extract):
        """Extract economy-related data"""
        from internal.models.wbgapi_model.extract_input_models import EconomyDataFrameInput, EconomyMetadataInput
        
        dataframe_id = self._get_param('economy_dataframe_id')
        metadata_id = self._get_param('economy_metadata_id')
        
        return {
            'dataframe': extract_obj.economy.dataframe_display(EconomyDataFrameInput(id=dataframe_id)),
            'metadata': extract_obj.economy.get_metadata(EconomyMetadataInput(id=metadata_id))
        }
    
    def _extract_series_data(self, extract_obj: wbapi_extract):
        """Extract series-related data"""
        from internal.models.wbgapi_model.extract_input_models import SeriesMetadataInput, SeriesGetInput
        
        series_id = self._get_param('series_id')
        series_metadata_id = self._get_param('series_metadata_id')
        
        return {
            'dataframe': extract_obj.series.get_series(SeriesGetInput(id=series_id)),
            'metadata': extract_obj.series.get_series_metadata(SeriesMetadataInput(id=series_metadata_id))
        }

    def _extract_topic_data(self, extract_obj: wbapi_extract):
        from internal.models.wbgapi_model.extract_input_models import TopicInfoInput, TopicSeriesInput, TopicMembersInput
        
        topic_id = self._get_param('topic_id')
        
        return {
            'info': extract_obj.topic.get_info(TopicInfoInput(id=topic_id)),
            'series': extract_obj.topic.get_series(TopicSeriesInput(id=topic_id)),
            'members': extract_obj.topic.get_members(TopicMembersInput(id=topic_id))
        }

    def _extract_time_data(self, extract_obj: wbapi_extract):
        from internal.models.wbgapi_model.extract_input_models import TimeSeriesInput
        
        time_id = self._get_param('time_id')
        return extract_obj.time.get_time_periods_series(TimeSeriesInput(id=time_id))
    
    def _extract_source_data(self, extract_obj: wbapi_extract):
        from internal.models.wbgapi_model.extract_input_models import SourceInfoInput, SourceSeriesInput
        
        source_id = self._get_param('source_id')
        
        return {
            'info': extract_obj.source.get_info(SourceInfoInput(id=source_id)),
            'series': extract_obj.source.get_series(SourceSeriesInput(id=source_id))
        }
        
    def _extract_region_data(self, extract_obj: wbapi_extract):
        from internal.models.wbgapi_model.extract_input_models import RegionSeriesInput
        
        region_id = self._get_param('region_id')
        return extract_obj.region.get_series(RegionSeriesInput(id=region_id))
    
    def _extract_income_data(self, extract_obj: wbapi_extract):
        from internal.models.wbgapi_model.extract_input_models import IncomeSeriesInput
        
        income_id = self._get_param('income_id')
        return extract_obj.income.get_series(IncomeSeriesInput(id=income_id))
    
    def _extract_lending_data(self, extract_obj: wbapi_extract):
        from internal.models.wbgapi_model.extract_input_models import LendingSeriesInput
        
        lending_id = self._get_param('lending_id')
        return extract_obj.lending.get_series(LendingSeriesInput(id=lending_id))


# Usage example:
# economy_operator = WorldBankExtractOperator(
#     task_id='extract_economy',
#     pipeline_config=config,
#     extract_type='economy',
#     params={
#         'economy_dataframe_id': 'VNM',  # Vietnam economy data
#         'economy_metadata_id': 'VNM'
#     }
# )