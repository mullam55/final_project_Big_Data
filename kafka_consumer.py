import csv
import json
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def initialize_kafka_consumer(topic_name: str, bootstrap_servers: str) -> KafkaConsumer:
    """
    Initializes and returns a KafkaConsumer instance.

    Args:
        topic_name (str): Kafka topic name to consume from.
        bootstrap_servers (str): Kafka broker addresses.

    Returns:
        KafkaConsumer: Initialized Kafka consumer.
    """
    return KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        group_id='my-consumer-group',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

def consume_and_save_to_csv(consumer: KafkaConsumer, output_file: str, buffer_size: int = 1000) -> None:
    """
    Consumes messages from a Kafka topic and saves them to a CSV file in batches.

    Args:
        consumer (KafkaConsumer): Kafka consumer instance.
        output_file (str): Path to the output CSV file.
        buffer_size (int): Number of messages to buffer before writing to the CSV file.
    """
    message_count = 0

    try:
        logging.info(f"Connected to Kafka. Listening to topic: {consumer.topics()}")

        with open(output_file, mode='w', newline='', encoding='utf-8') as file:
            writer = None
            buffer = []

            for message in consumer:
                data = message.value
                buffer.append(data)
                message_count += 1

                if writer is None:
                    fieldnames = list(data.keys())
                    writer = csv.DictWriter(file, fieldnames=fieldnames)
                    writer.writeheader()
                    logging.info(f"CSV header written: {fieldnames}")

                if len(buffer) >= buffer_size:
                    writer.writerows(buffer)
                    logging.info(f"Written {len(buffer)} messages to CSV.")
                    buffer.clear()

                if message_count % 1000 == 0:
                    logging.info(f"Received {message_count} messages so far.")

            if buffer:
                writer.writerows(buffer)
                logging.info(f"Written final {len(buffer)} messages to CSV.")

    except KafkaError as e:
        logging.error(f"Kafka error occurred: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        consumer.close()
        logging.info(f"Consumer closed. Total messages received: {message_count}")

def main():
    """
    Main function to initialize Kafka consumer and consume messages.
    """
    topic_name = "accidentstopic"
    bootstrap_servers = "localhost:9092"
    output_file = "received_accidents_data.csv"

    try:
        consumer = initialize_kafka_consumer(topic_name, bootstrap_servers)
        consume_and_save_to_csv(consumer, output_file)
    except KeyboardInterrupt:
        logging.warning("Consumer interrupted by user.")
    except Exception as e:
        logging.error(f"Error in the main function: {e}")

if __name__ == "__main__":
    main()
