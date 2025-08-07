import pymysql
from internal.dags.wbapi_dag.load import BaseDBLoader
from internal.models.transform_models import EconomyTransform, SeriesTransform, TopicTransform, TimeTransform, SourceTransform, RegionTransform, IncomeTransform, LendingTransform
from src.utils import TableCreator

class DatabaseLoaderService:
    def __init__(self, pipeline_config):
        self.config = pipeline_config
        self.conn = None
        self.cursor = None
        self.logger = self.config.logger

    def _get_database_connection(self):
        try:
            self.conn = pymysql.connect(
                host=self.config['database']['primary']['host'],
                user=self.config['database']['primary']['username'],
                password=self.config['database']['primary']['password'],
                database=self.config['database']['primary']['database'],
                charset=self.config['database']['primary']['charset'],
                cursorclass=pymysql.cursors.DictCursor
            )
            self.cursor = self.conn.cursor()
        except Exception as e:
            self.logger.error(f"Error during loading: {e}")
            return

    def create_or_replace_database(self):
        try:
            loader = BaseDBLoader(cursor=self.cursor, connection=self.conn, logger=self.logger, config=self.config)
            loader.create_or_replace_database(self.config['database']['primary']['database'])
        except Exception as e:
            self.logger.error(f"Error creating or replacing database: {e}")
            return

    def load_data(self, transformed_data):
        try:
            transform_classes = [
                (EconomyTransform, "ECONOMY", EconomyTransform.ECONOMY_DF_RULES),
                (SeriesTransform, "SERIES", SeriesTransform.SERIES_DF_RULES),
                (TopicTransform, "TOPIC", TopicTransform.TOPIC_DF_RULES),
                (TimeTransform, "TIME", TimeTransform.TIME_DF_RULES),
                (SourceTransform, "SOURCE", SourceTransform.SOURCE_DF_RULES),
                (RegionTransform, "REGION", RegionTransform.REGION_DF_RULES),
                (IncomeTransform, "INCOME", IncomeTransform.INCOME_DF_RULES),
                (LendingTransform, "LENDING", LendingTransform.LENDING_DF_RULES),
            ]

            table_df_map = {
                "economy_data": transformed_data["economy_data"],
                "economy_metadata": transformed_data["economy_metadata"],
                "series_data": transformed_data["series_data"],
                "series_metadata": transformed_data["series_metadata"],
                "topic_info": transformed_data["topic_info"],
                "topic_series": transformed_data["topic_series"],
                "topic_metadata": transformed_data["topic_metadata"],
                "time_series": transformed_data["time_series"],
                "source_info": transformed_data["source_info"],
                "source_series": transformed_data["source_series"],
                "region_info": transformed_data["region_info"],
                "income_series": transformed_data["income_series"],
                "lending_series": transformed_data["lending_series"],
            }

            for transform_cls, prefix, rules in transform_classes:
                for rule_name, rule_dict in rules.items():
                    if not rule_name.startswith("transform_"):
                        continue
                    table_name = f"{prefix.lower()}_{rule_name.replace('transform_', '').replace('_dataframe','').replace('_metadata','')}"
                    character_specific = ''.join([w.capitalize() for w in table_name.split('_')])
                    creator = TableCreator(machine_id=1, character_specific=character_specific)

                    try:
                        sql = creator.generate_create_table_sql(table_name, rule_dict)
                    except Exception as e:
                        self.logger.error(f"Error generating SQL for {table_name}: {e}")
                        return

                    try:
                        self.logger.create_or_replace_table(sql)
                    except Exception as e:
                        self.logger.error(f"Error creating or replacing table {table_name}: {e}")
                        return

                    try:
                        df = table_df_map.get(table_name)
                        if df is None:
                            self.logger.warning(f"No data found for table {table_name}. Skipping insert.")
                            continue
                        # Nếu df là None, bỏ qua insert
                        if df is not None:
                            df_with_id = creator.add_id_column(df)
                            # Insert từng dòng
                            cols = ','.join(df_with_id.columns)
                            placeholders = ','.join(['%s'] * len(df_with_id.columns))
                            insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
                            for row in df_with_id.itertuples(index=False, name=None):
                                self.logger.insert_table(insert_sql, row)
                    except Exception as e:
                        self.logger.error(f"Error inserting data into table {table_name}: {e}")
                        return
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
            return
        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
                           