import json
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
from datetime import datetime


class WorldBankKafkaProducer:
    """Kafka producer for World Bank data with error handling and retry logic"""
    
    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.producer = None
        self._connect()
    
    def _connect(self):
        """Establish Kafka producer connection"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config['bootstrap_servers'],
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8'),
                acks='all',
                retries=3,
                retry_backoff_ms=1000,
                batch_size=16384,
                linger_ms=10,
                compression_type='snappy'
            )
            
            self.logger.info("Kafka producer connected successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to connect Kafka producer: {str(e)}")
            raise
    
    def publish_message(self, topic: str, message: Dict[str, Any], key: Optional[str] = None):
        """Publish message to Kafka topic"""
        try:
            if not self.producer:
                self._connect()
            
            # Add metadata
            message['published_at'] = datetime.now().isoformat()
            
            # Send message
            future = self.producer.send(topic, value=message, key=key)
            
            # Wait for confirmation (blocking)
            record_metadata = future.get(timeout=30)
            
            self.logger.debug(f"Message published to {topic}: partition={record_metadata.partition}, offset={record_metadata.offset}")
            
        except KafkaError as e:
            self.logger.error(f"Kafka error publishing to {topic}: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error publishing message to {topic}: {str(e)}")
            raise
    
    def close(self):
        """Close Kafka producer"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            self.logger.info("Kafka producer closed")
class KafkaPublisherService(BaseETLProcessor):
    """Kafka publishing service for real-time data streaming"""
    
    def __init__(self, config_manager: ConfigurationManager):
        super().__init__(config_manager)
        self.producer = None
    
    def _get_kafka_producer(self) -> WorldBankKafkaProducer:
        """Get Kafka producer instance"""
        if self.producer is None:
            producer_config = self.config_manager.get_kafka_producer_config()
            self.producer = WorldBankKafkaProducer(producer_config, self.logger)
        return self.producer
    
    def _create_kafka_message(self, table_name: str, data: Any, context: ETLContext) -> Dict[str, Any]:
        """Create standardized Kafka message"""
        message = {
            'table': table_name,
            'timestamp': datetime.now().isoformat(),
            'execution_date': context.execution_date.isoformat(),
            'dag_id': context.dag_id,
            'task_id': context.task_id,
            'run_id': context.run_id,
            'correlation_id': context.correlation_id,
            'record_count': 0,
            'data_sample': None
        }
        
        # Add data information
        if data is not None:
            if hasattr(data, '__len__'):
                message['record_count'] = len(data)
                
            # Add data sample for monitoring
            if hasattr(data, 'head'):  # pandas DataFrame
                message['data_sample'] = data.head(5).to_dict('records')
            elif isinstance(data, list) and len(data) > 0:
                message['data_sample'] = data[:5]
            elif isinstance(data, dict):
                message['data_sample'] = data
        
        return message
    
    def process(self, context: ETLContext, transform_results: Dict[str, Any]) -> ProcessingResult:
        """Main processing method for Kafka publishing"""
        self._validate_context(context)
        start_time = datetime.now()
        published_count = 0
        errors = []
        
        try:
            if not transform_results:
                return ProcessingResult(
                    success=True,
                    records_processed=0,
                    execution_time_seconds=0,
                    warnings=["No transformation results to publish"]
                )
            
            # Get Kafka producer
            producer = self._get_kafka_producer()
            
            # Define topics for different data types
            topic_mapping = {
                'economy_data': 'worldbank.economy.data',
                'economy_metadata': 'worldbank.economy.metadata',
                'series_data': 'worldbank.series.data',
                'series_metadata': 'worldbank.series.metadata',
                'topic_info': 'worldbank.topic.info',
                'topic_series': 'worldbank.topic.series',
                'topic_members': 'worldbank.topic.members',
                'time_series': 'worldbank.time.series',
                'source_info': 'worldbank.source.info',
                'source_series': 'worldbank.source.series',
                'region_series': 'worldbank.region.series',
                'income_series': 'worldbank.income.series',
                'lending_series': 'worldbank.lending.series'
            }
            
            # Publish each dataset
            for table_name, data in transform_results.items():
                if data is not None:
                    topic = topic_mapping.get(table_name, 'worldbank.general.data')
                    message = self._create_kafka_message(table_name, data, context)
                    
                    try:
                        # Publish message to Kafka
                        producer.publish_message(topic, message, key=f"{table_name}:{context.run_id}")
                        published_count += 1
                        self.logger.info(f"Published {table_name} to Kafka topic {topic}")
                        
                    except Exception as e:
                        error_msg = f"Failed to publish {table_name} to Kafka: {str(e)}"
                        errors.append(error_msg)
                        self.logger.error(error_msg)
            
            # Calculate execution time
            duration = (datetime.now() - start_time).total_seconds()
            self._log_performance("kafka_publishing", duration, published_count)
            
            success = len(errors) == 0
            
            return ProcessingResult(
                success=success,
                records_processed=published_count,
                execution_time_seconds=duration,
                errors=errors,
                metadata={
                    'messages_published': published_count,
                    'topics_used': len(set(topic_mapping.values()))
                }
            )
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            error_msg = f"Kafka publishing failed: {str(e)}"
            self.logger.error(error_msg)
            
            return ProcessingResult(
                success=False,
                records_processed=published_count,
                execution_time_seconds=duration,
                errors=[error_msg]
            )
        finally:
            if self.producer:
                self.producer.close()
