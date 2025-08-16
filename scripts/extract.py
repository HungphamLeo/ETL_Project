from datetime import datetime
from internal.models import *
from internal.dags.wbapi_dag.extract import wbapi_extract
from airflow.providers.standard.operators.python import PythonOperator


class WorldBankExtractOperator(PythonOperator):
    """Custom operator cho World Bank API data extraction"""
    
    def __init__(self, pipeline_config, extract_type: str, **kwargs):
        self.extract_type = extract_type
        self.pipeline_config = pipeline_config
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
                'time': self._extract_income_data,
                'source': self._extract_source_data,
                'region': self._extract_region_data,
                'income': self._extract_income_data,
                'lending': self._extract_lending_data
            }
            
            if self.extract_type not in extraction_map:
                raise ValueError(f"Unknown extract type: {self.extract_type}")
            
            data = extraction_map[self.extract_type](extract_obj)
            
            # Store data in XCom for downstream tasks
            return {
                'extract_type': self.extract_type,
                'record_count': len(data) if isinstance(data, list) else data.count() if hasattr(data, 'count') else 0,
                'extraction_timestamp': datetime.now().isoformat(),
                'data': data  # Note: For large datasets, consider storing in external storage
            }
            
        except Exception as e:
            logger.error(f"Extraction failed for {self.extract_type}: {str(e)}")
            raise
    
    def _extract_economy_data(self, extract_obj: wbapi_extract):
        """Extract economy-related data"""
        from internal.models.extract_input_models import EconomyDataFrameInput, EconomyMetadataInput
        
        return {
            'dataframe': extract_obj.economy.dataframe_display(EconomyDataFrameInput(id='all')),
            'metadata': extract_obj.economy.get_metadata(EconomyMetadataInput(id='all'))
        }
    
    def _extract_series_data(self, extract_obj:wbapi_extract):
        """Extract series-related data"""
        from internal.models.extract_input_models import SeriesMetadataInput, SeriesGetInput
        
        return {
            'dataframe': extract_obj.series.get_series(SeriesGetInput(id='all')),
            'metadata': extract_obj.series.get_series_metadata(SeriesMetadataInput(id='all'))
        }

    # Additional extraction methods for other data types...
    def _extract_topic_data(self, extract_obj: wbapi_extract):
        from internal.models.extract_input_models import TopicInfoInput, TopicSeriesInput, TopicMembersInput
        return {
            'info': extract_obj.topic.get_info(TopicInfoInput(id='all')),
            'series': extract_obj.topic.get_series(TopicSeriesInput(id='all')),
            'members': extract_obj.topic.get_members(TopicMembersInput(id='all'))
        }

    def _extract_time_data(self, extract_obj: wbapi_extract):
        from internal.models.extract_input_models import TimeSeriesInput
        return extract_obj.time.get_time_periods_series(TimeSeriesInput(id='all'))
    
    def _extract_source_data(self, extract_obj: wbapi_extract):
        from internal.models.extract_input_models import SourceInfoInput, SourceSeriesInput
        return {
            'info': extract_obj.source.get_info(SourceInfoInput(id='all')),
            'series': extract_obj.source.get_series(SourceSeriesInput(id='all'))
        }
    def _extract_region_data(self, extract_obj: wbapi_extract):
        from internal.models.extract_input_models import RegionSeriesInput
        return extract_obj.region.get_series(RegionSeriesInput(id='all'))
    
    def _extract_income_data(self, extract_obj: wbapi_extract):
        from internal.models.extract_input_models import IncomeSeriesInput
        return extract_obj.income.get_series(IncomeSeriesInput(id='all'))
    
    def _extract_lending_data(self, extract_obj: wbapi_extract):
        from internal.models.extract_input_models import LendingSeriesInput
        return extract_obj.lending.get_series(LendingSeriesInput(id='all'))