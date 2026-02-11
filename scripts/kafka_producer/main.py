#!/usr/bin/env python3
"""
Kafka Producer - Main Runner

This file imports and orchestrates all the modules.
Run this file directly or use it in Airflow.
"""

import sys
import time
import logging
import argparse

# Import our modules
import config
import state
from generator import SalesDataGenerator
from producer import KafkaProducer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_producer(
    kafka_config: config.KafkaConfig,
    message_count: int = 100,
    rate: float = 1.0,
    csv_path: str = None,
    start_order: int = None
) -> int:
    """
    Run the Kafka producer.
    
    Args:
        kafka_config: Kafka configuration
        message_count: Number of messages to produce
        rate: Messages per second
        csv_path: Optional CSV file for patterns
        start_order: Optional starting order number
        
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    # Get starting order number
    if start_order is not None:
        starting_order = start_order
        logger.info(f"Using provided starting order: SO{start_order}")
    else:
        starting_order = state.load_last_order_num()
    
    # Initialize generator
    try:
        generator = SalesDataGenerator(
            starting_order_num=starting_order,
            csv_path=csv_path
        )
    except Exception as e:
        logger.error(f"Failed to initialize generator: {e}")
        return 1
    
    # Initialize producer
    try:
        kafka_producer = KafkaProducer(
            config=kafka_config.to_producer_config(),
            topic=kafka_config.topic
        )
    except Exception as e:
        logger.error(f"Failed to initialize producer: {e}")
        return 1
    
    # Calculate delay
    delay = 1.0 / rate if rate > 0 else 0
    
    logger.info(f"Starting production: {message_count} messages at {rate} msg/sec")
    logger.info(f"Topic: {kafka_config.topic}")
    logger.info(f"Next order: SO{generator.get_current_order_num() + 1}\n")
    
    sent = 0
    start_time = time.time()
    
    try:
        while sent < message_count:
            # Generate and send
            record = generator.generate_record()
            success = kafka_producer.send(record)
            
            if success:
                sent += 1
                logger.info(
                    f"[{sent}/{message_count}] {record['sls_ord_num']} | "
                    f"{record['sls_prd_key']:12} | "
                    f"Cust: {record['sls_cust_id']:5} | "
                    f"${record['sls_sales']:>8.2f}"
                )
            
            # Maintain rate
            if delay > 0 and sent < message_count:
                time.sleep(delay)
        
        # Success
        elapsed = time.time() - start_time
        logger.info(f"\nCompleted: {sent} messages in {elapsed:.2f}s")
        logger.info(f"Last order: SO{generator.get_current_order_num()}")
        
        # Save state
        state.save_last_order_num(generator.get_current_order_num())
        logger.info(f"Next run will start from SO{generator.get_current_order_num() + 1}")
        
        return 0
        
    except KeyboardInterrupt:
        logger.info(f"\nStopped by user after {sent} messages")
        state.save_last_order_num(generator.get_current_order_num())
        return 0
        
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        state.save_last_order_num(generator.get_current_order_num())
        return 1
        
    finally:
        kafka_producer.close()


def main():
    """Main entry point with CLI arguments."""
    parser = argparse.ArgumentParser(
        description='Kafka Producer for Confluent Cloud',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using environment variables (recommended)
  export BOOTSTRAP_SERVERS="pkc-xxxxx.aws.confluent.cloud:9092"
  export API_KEY="YOUR_KEY"
  export API_SECRET="YOUR_SECRET"
  export CLIENT_ID="YOUR_CLIENT_ID"
  python main.py --count 100 --rate 5
  
  # Using .env file
  python main.py --count 100
  
  # With CSV patterns
  python main.py --csv sales_data.csv --count 200
        """
    )
    
    # Kafka settings
    parser.add_argument('--bootstrap-servers', help='Kafka bootstrap servers')
    parser.add_argument('--api-key', help='Confluent API key')
    parser.add_argument('--api-secret', help='Confluent API secret')
    parser.add_argument('--client-id', help='Client ID')
    parser.add_argument('--topic', default='orders', help='Kafka topic')
    
    # Producer settings
    parser.add_argument('--count', type=int, default=100, 
                       help='Number of messages (default: 100)')
    parser.add_argument('--rate', type=float, default=1.0,
                       help='Messages per second (default: 1.0)')
    parser.add_argument('--csv', help='CSV file for patterns')
    parser.add_argument('--start-order', type=int, help='Starting order number')
    
    args = parser.parse_args()
    
    # Load environment
    config.load_env_file()
    
    # Get Kafka config
    if args.bootstrap_servers:
        # From command line
        kafka_config = config.KafkaConfig(
            bootstrap_servers=args.bootstrap_servers,
            api_key=args.api_key,
            api_secret=args.api_secret,
            client_id=args.client_id,
            topic=args.topic
        )
    else:
        # From environment
        kafka_config = config.get_kafka_config_from_env()
        if args.topic != 'orders':
            kafka_config.topic = args.topic
    
    # Validate
    if not config.validate_config(kafka_config):
        logger.error("\nSet credentials via environment variables or .env file")
        return 1
    
    # Run
    return run_producer(
        kafka_config=kafka_config,
        message_count=args.count,
        rate=args.rate,
        csv_path=args.csv,
        start_order=args.start_order
    )


if __name__ == '__main__':
    sys.exit(main())