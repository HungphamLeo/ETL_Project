from datetime import datetime
from airflow.operators.python import PythonOperator
from .wbapi_extract_obj import (base_extract_logger_obj, 
    wbapi_series, wbapi_economy, wbapi_topic,
    wbapi_time, wbapi_source, wbapi_region,
    wbapi_income, wbapi_lending
)


class wbapi_extract:
    def __init__(self, pipeline_logger = None):
        self.series = wbapi_series(pipeline_logger)
        self.series = wbapi_series(pipeline_logger)
        self.economy = wbapi_economy(pipeline_logger)
        self.topic = wbapi_topic(pipeline_logger)
        self.time = wbapi_time(pipeline_logger)
        self.source = wbapi_source(pipeline_logger)
        self.region = wbapi_region(pipeline_logger)
        self.income = wbapi_income(pipeline_logger)
        self.lending = wbapi_lending(pipeline_logger)


class WorldBankExtractOperator(PythonOperator):
    """Custom operator cho World Bank API data extraction"""

    def __init__(self, pipeline_config, pipeline_logger, extract_type: str, **kwargs):
        """
        :param pipeline_config: pipeline config
        :param extract_type: loại dữ liệu ('economy', 'series', ...)
        :param config: dict đã load từ YAML['extract'][extract_type]
        """
        self.extract_type = extract_type
        self.pipeline_config = pipeline_config
        self.logger = pipeline_logger
        self.params = pipeline_config or {}
        super().__init__(python_callable=self._extract_data, **kwargs)

    def _extract_data(self, **context):
        try:
            extract_obj = wbapi_extract(self.logger)
            extraction_map = {
                "economy": self._extract_economy_data,
                "series": self._extract_series_data,
                "topic": self._extract_topic_data,
                "time": self._extract_time_data,
                "source": self._extract_source_data,
                "region": self._extract_region_data,
                "income": self._extract_income_data,
                "lending": self._extract_lending_data,
            }

            if self.extract_type not in extraction_map:
                raise ValueError(f"Unknown extract type: {self.extract_type}")

            data = extraction_map[self.extract_type](extract_obj)

            return {
                "extract_type": self.extract_type,
                "record_count": len(data)
                if isinstance(data, list)
                else data.count()
                if hasattr(data, "count")
                else 0,
                "extraction_timestamp": datetime.now().isoformat(),
                "data": data,
            }

        except Exception as e:
            self.logger.error(f"Extraction failed for {self.extract_type}: {str(e)}")
            raise

    def _extract_economy_data(self, extract_obj: wbapi_extract):
        from internal.models.wbgapi_model.extract_input_models import (
            EconomyDataFrameInput,
            EconomyMetadataInput,
        )

        return {
            "dataframe": extract_obj.economy.dataframe_display(
                EconomyDataFrameInput(**self.params.get("dataframe", {}))
            ),
            "metadata": extract_obj.economy.get_metadata(
                EconomyMetadataInput(**self.params.get("metadata", {}))
            ),
        }

    def _extract_series_data(self, extract_obj: wbapi_extract):
        from internal.models.wbgapi_model.extract_input_models import (
            SeriesMetadataInput,
            SeriesGetInput,
        )

        return {
            "dataframe": extract_obj.series.get_series(
                SeriesGetInput(**self.params.get("get", {}))
            ),
            "metadata": extract_obj.series.get_series_metadata(
                SeriesMetadataInput(**self.params.get("metadata", {}))
            ),
        }

    def _extract_topic_data(self, extract_obj: wbapi_extract):
        from internal.models.wbgapi_model.extract_input_models import (
            TopicInfoInput,
            TopicSeriesInput,
            TopicMembersInput,
        )

        return {
            "info": extract_obj.topic.get_info(
                TopicInfoInput(**self.params.get("info", {}))
            ),
            "series": extract_obj.topic.get_series(
                TopicSeriesInput(**self.params.get("series", {}))
            ),
            "members": extract_obj.topic.get_members(
                TopicMembersInput(**self.params.get("members", {}))
            ),
        }

    def _extract_time_data(self, extract_obj: wbapi_extract):
        from internal.models.wbgapi_model.extract_input_models import TimeSeriesInput

        return extract_obj.time.get_time_periods_series(
            TimeSeriesInput(**self.params)
        )

    def _extract_source_data(self, extract_obj: wbapi_extract):
        from internal.models.wbgapi_model.extract_input_models import (
            SourceInfoInput,
            SourceSeriesInput,
        )

        return {
            "info": extract_obj.source.get_info(
                SourceInfoInput(**self.params.get("info", {}))
            ),
            "series": extract_obj.source.get_series(
                SourceSeriesInput(**self.params.get("series", {}))
            ),
        }

    def _extract_region_data(self, extract_obj: wbapi_extract):
        from internal.models.wbgapi_model.extract_input_models import RegionSeriesInput

        return extract_obj.region.get_series(RegionSeriesInput(**self.params))

    def _extract_income_data(self, extract_obj: wbapi_extract):
        from internal.models.wbgapi_model.extract_input_models import IncomeSeriesInput

        return extract_obj.income.get_series(IncomeSeriesInput(**self.params))

    def _extract_lending_data(self, extract_obj: wbapi_extract):
        from internal.models.wbgapi_model.extract_input_models import (
            LendingSeriesInput,
        )

        return extract_obj.lending.get_series(LendingSeriesInput(**self.params))
