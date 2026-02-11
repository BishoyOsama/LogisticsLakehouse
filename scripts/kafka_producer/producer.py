"""
Kafka producer wrapper.
"""

import json
from confluent_kafka import Producer
import logging

logger = logging.getLogger(__name__)


class KafkaProducer:
    """Kafka producer for Confluent Cloud."""
    
    def __init__(self, config: dict, topic: str):
        """Initialize producer."""
        self.topic = topic
        self.delivered = 0
        self.failed = 0
        
        config['error_cb'] = self._error_callback
        self.producer = Producer(config)
        
        logger.info(f"Producer initialized for topic: {topic}")
    
    def _error_callback(self, err) -> None:
        """Handle producer errors."""
        logger.error(f"Producer error: {err}")
    
    def _delivery_callback(self, err, msg) -> None:
        """Handle delivery confirmation."""
        if err:
            self.failed += 1
            logger.error(f"Delivery failed: {err}")
        else:
            self.delivered += 1
    
    def send(self, message: dict) -> bool:
        """Send message to Kafka."""
        try:
            value = json.dumps(message).encode('utf-8')
            key = message['sls_ord_num'].encode('utf-8')
            
            self.producer.produce(
                topic=self.topic,
                key=key,
                value=value,
                callback=self._delivery_callback
            )
            
            self.producer.poll(0)
            return True
            
        except BufferError:
            logger.warning("Queue full, flushing...")
            self.producer.flush()
            return self.send(message)
            
        except Exception as e:
            logger.error(f"Send error: {e}")
            return False
    
    def close(self) -> None:
        """Close producer and flush messages."""
        logger.info("Flushing messages...")
        remaining = self.producer.flush(timeout=30)
        
        logger.info(f"Producer closed - Delivered: {self.delivered}, Failed: {self.failed}")
        if remaining > 0:
            logger.warning(f"Undelivered: {remaining}")