"""
Configuration management for Kafka Producer.
"""

import os
from dataclasses import dataclass
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)

# Constants
DEFAULT_TOPIC = 'orders'
DEFAULT_RATE = 1.0
DEFAULT_MESSAGES_PER_RUN = 100
DEFAULT_SECURITY_PROTOCOL = 'SASL_SSL'
DEFAULT_SASL_MECHANISM = 'PLAIN'


@dataclass
class KafkaConfig:
    """Kafka connection configuration."""
    
    bootstrap_servers: str
    api_key: str
    api_secret: str
    client_id: str
    security_protocol: str = DEFAULT_SECURITY_PROTOCOL
    sasl_mechanisms: str = DEFAULT_SASL_MECHANISM
    topic: str = DEFAULT_TOPIC
    
    def to_producer_config(self) -> dict:
        """Convert to Confluent Kafka producer configuration."""
        return {
            'bootstrap.servers': self.bootstrap_servers,
            'security.protocol': self.security_protocol,
            'sasl.mechanisms': self.sasl_mechanisms,
            'sasl.username': self.api_key,
            'sasl.password': self.api_secret,
            'client.id': self.client_id,
            'acks': 'all',
            'retries': 3,
            'max.in.flight.requests.per.connection': 1,
            'compression.type': 'snappy',
        }


def load_env_file(env_path: str = ".env") -> None:
    """Load environment variables from .env file."""
    if os.path.exists(env_path):
        load_dotenv(dotenv_path=env_path)
        logger.info(f"Loaded environment from {env_path}")


def get_kafka_config_from_env() -> KafkaConfig:
    """Load Kafka configuration from environment variables."""
    return KafkaConfig(
        bootstrap_servers=os.getenv('BOOTSTRAP_SERVERS'),
        api_key=os.getenv('API_KEY'),
        api_secret=os.getenv('API_SECRET'),
        client_id=os.getenv('CLIENT_ID'),
        security_protocol=os.getenv('SECURITY_PROTOCOL', DEFAULT_SECURITY_PROTOCOL),
        sasl_mechanisms=os.getenv('SASL_MECHANISMS', DEFAULT_SASL_MECHANISM),
        topic=os.getenv('KAFKA_TOPIC', DEFAULT_TOPIC),
    )


def validate_config(config: KafkaConfig) -> bool:
    """Validate Kafka configuration."""
    if not all([config.bootstrap_servers, config.api_key, 
                config.api_secret, config.client_id]):
        logger.error("Missing required Kafka credentials!")
        logger.error("Required: BOOTSTRAP_SERVERS, API_KEY, API_SECRET, CLIENT_ID")
        return False
    return True