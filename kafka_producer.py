import time
import json
import csv
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def initialize_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    """
    Initializes and returns a KafkaProducer instance.

    Args:
        bootstrap_servers (str): Kafka broker addresses.

    Returns:
        KafkaProducer: Initialized Kafka producer.
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        batch_size=327680,
        linger_ms=50,
        compression_type='gzip'
    )

def read_csv_and_produce(filename: str, producer: KafkaProducer, topic_name: str) -> None:
    """
    Reads a CSV file and sends each row as a message to a Kafka topic.

    Args:
        filename (str): Path to the CSV file.
        producer (KafkaProducer): Kafka producer instance.
        topic_name (str): Kafka topic name.

    Logs:
        - Total messages sent.
        - Time taken to send messages.
    """
    start_time = time.time()
    message_count = 0

    try:
        with open(filename, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                try:
                    message = json.dumps(row)
                    producer.send(topic_name, message.encode())
                    message_count += 1
                except KafkaError as e:
                    logging.error(f"Error sending message to Kafka: {e}")
    except FileNotFoundError:
        logging.error(f"File not found: {filename}")
        return
    except Exception as e:
        logging.error(f"Unexpected error while reading the CSV file: {e}")
        return

    end_time = time.time()
    total_time = end_time - start_time

    logging.info(f"Total messages published: {message_count}")
    logging.info(f"Total time taken: {total_time:.2f} seconds")

def main():
    """
    Main function to configure Kafka producer and process the CSV file.
    """
    bootstrap_servers = "localhost:9092"
    topic_name = "accidentstopic"
    csv_file = "Road Accident Data.csv"

    try:
        producer = initialize_kafka_producer(bootstrap_servers)
        logging.info("Kafka producer initialized.")
        read_csv_and_produce(csv_file, producer, topic_name)
    except KeyboardInterrupt:
        logging.warning("Producer interrupted by user.")
    finally:
        producer.close()
        logging.info("Kafka producer closed.")

if __name__ == "__main__":
    main()
