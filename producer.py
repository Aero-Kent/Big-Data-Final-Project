"""
Kafka Producer Template for Streaming Data Dashboard
STUDENT PROJECT: Big Data Streaming Data Producer

This is a template for students to build a Kafka producer that generates and sends
streaming data to Kafka for consumption by the dashboard.

DO NOT MODIFY THE TEMPLATE STRUCTURE - IMPLEMENT THE TODO SECTIONS
"""

import argparse
import json
import time
from datetime import datetime, UTC
from typing import Dict, Any
import os
import requests

from dotenv import load_dotenv
load_dotenv()

# Kafka libraries
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

class StreamingDataProducer:
    """
    Revised kafka producers, removing the synthetic data generation and adding now
    data retrieved from weatherapi.

    This class handles Kafka connection, weather data generation, and message sending
    """

    WEATHER_API_URL = "https://api.weatherapi.com/v1/current.json"
    
    def __init__(self, bootstrap_servers: str, topic: str, api_key: str, location: str):
        """
        Initialize Kafka producer configuration
        
        Parameters:
        - bootstrap_servers: Kafka broker addresses (e.g., "localhost:9092")
        - topic: Kafka topic to produce messages to
        - api_key: The API key for weatherapi.com
        - location: The geographic location to query
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.api_key = api_key
        self.location = location
        
        # Kafka producer configuration
        self.producer_config = {
            'bootstrap_servers': bootstrap_servers,
        }
        
        # Initialize Kafka producer
        try:
            self.producer = KafkaProducer(**self.producer_config)
            print(f"Kafka producer initialized for {bootstrap_servers} on topic {topic}")
        except NoBrokersAvailable:
            print(f"ERROR: No Kafka brokers available at {bootstrap_servers}")
            self.producer = None
        except Exception as e:
            print(f"ERROR: Failed to initialize Kafka producer: {e}")
            self.producer = None

    def generate_weather_data(self):
        """
        Generates now Current Weather Data from https://www.weatherapi.com/
        using their API Key.
        """
        if not self.api_key:
            print("ERROR: Weather API key is not configured. Cannot fetch data.")
            return None
        
        print(f"Fetching weather data for {self.location}...")

        try:
            params = {              # params for the api
                'key': self.api_key,
                'q': self.location, # q == params for the country
                'aqi': 'no'         # Air Quality Index
            }

            response = requests.get(self.WEATHER_API_URL, params=params)
            response.raise_for_status()
            
            data = response.json()

            weather_record = {
                "producer_timestamp": datetime.now(UTC).isoformat(),
                "location_name": data.get("location", {}).get("name"),
                "region": data.get("location", {}).get("region"),
                "country": data.get("location", {}).get("country"),
                "temp_c": data.get("current", {}).get("temp_c"),
                "humidity": data.get("current", {}).get("humidity"),
                "wind_kph": data.get("current", {}).get("wind_kph"),
                "condition_text": data.get("current", {}).get("condition", {}).get("text"),
                "is_day": data.get("current", {}).get("is_day"),
                "api_time": data.get("current", {}).get("last_updated_epoch"),
            }
            
            return weather_record

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err} - Response: {response.text}")
            return None
        except requests.exceptions.RequestException as req_err:
            print(f"Network/Connection error occurred: {req_err}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred during API call: {e}")
            return None

    def serialize_data(self, data: Dict[str, Any]) -> bytes:
        """       
        Convert the data dictionary to bytes for Kafka transmission.
        Formats used: JSON
        """
       
        try:
            serialized_data = json.dumps(data).encode('utf-8')           
            return serialized_data
        except Exception as e:
            print(f"Error during JSON serialization: {e}")
            return None

    def send_message(self, data: Dict[str, Any]) -> bool:
        """
        Implement message sending to Kafka
        
        Parameters:
        - data: Dictionary containing the message data
        
        Returns:
        - bool: True if message was sent successfully, False otherwise
        """
        
        # Check if producer is initialized
        if not self.producer:
            print("ERROR: Kafka producer not initialized")
            return False
        
        if not data:
            print("Skipping message send: Data generation failed.")
            return False
        
        # Serialize the data
        serialized_data = self.serialize_data(data)
        if not serialized_data:
            print("ERROR: Serialization failed")
            return False
        
        try:
            # Send message to Kafka
            future = self.producer.send(self.topic, value=serialized_data)
            # Wait for send confirmation with timeout
            result = future.get(timeout=10)
            print(f"Message sent successfully - Topic: {self.topic}, Data: {data}")
            return True
            
        except KafkaError as e:
            print(f"Kafka send error: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error during send: {e}")
            return False

    def produce_stream(self, messages_per_second: float = 0.1, duration: int = None):
        """
        STUDENT TODO: Implement the main streaming loop
        
        Parameters:
        - messages_per_second: Rate of message production (default: 0.1 for 10-second intervals)
        - duration: Total runtime in seconds (None for infinite)
        """
        
        print(f"Starting producer: {messages_per_second} msg/sec ({1/messages_per_second:.1f} second intervals), duration: {duration or 'infinite'}")
        
        start_time = time.time()
        message_count = 0
        
        try:
            while True:
                # Check if we've reached the duration limit
                if duration and (time.time() - start_time) >= duration:
                    print(f"Reached duration limit of {duration} seconds")
                    break
                
                # Generate and send data
                data = self.generate_weather_data()
                success = self.send_message(data)
                
                if success:
                    message_count += 1
                    if message_count % 10 == 0:  # Print progress every 10 messages
                        print(f"Sent {message_count} messages...")
                
                # Calculate sleep time to maintain desired message rate
                sleep_time = 1.0 / messages_per_second
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            print("\nProducer interrupted by user")
        except Exception as e:
            print(f"Streaming error: {e}")
        finally:
            # Implement proper cleanup
            self.close()
            print(f"Producer stopped. Total messages sent: {message_count}")

    def close(self):
        """Implement producer cleanup and resource release"""
        if self.producer:
            try:
                # Ensure all messages are sent
                self.producer.flush(timeout=10)
                # Close producer connection
                self.producer.close()
                print("Kafka producer closed successfully")
            except Exception as e:
                print(f"Error closing Kafka producer: {e}")

def parse_arguments():
    """
    STUDENT TODO: Configure command-line arguments for flexibility
    [completed] changed the default topic used as well as the changed
                the message per seconds to be 1:1 in order to be faster
                as well as the provided graph can see a better output.
    """
    parser = argparse.ArgumentParser(description='Kafka Weather Data Producer using WeatherAPI.com')
    
    """
    STUDENT TODO: Add additional command-line arguments as needed
    [completed] added cli arguments for --location and --api-key
                to flexibility change either country or if the
                provided api-key used has reached its limit/would
                want to change the api-key.
    """
    parser.add_argument(
        '--bootstrap-servers',
        type=str,
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    
    parser.add_argument(
        '--topic',
        type=str,
        default='weather-data',
        help='Kafka topic to produce to (default: weather-data)'
    )
    
    # changed this for a faster default
    parser.add_argument(
        '--rate',
        type=float,
        default=1.0,
        help='Messages per second (default: 1.0 for 1-second intervals)'
    )
    
    parser.add_argument(
        '--duration',
        type=int,
        default=None,
        help='Run duration in seconds (default: infinite)'
    )
    
    # addded this
    parser.add_argument(
        '--location',
        type=str,
        default='Philippines',
        help='Geographic location for weather query (default: Philippines)'
    )

    # addded this
    parser.add_argument(
        '--api-key',
        type=str,
        default=None,
        help='Weather API key (optional; will check WEATHER_API_KEY environment variable if not provided)'
    )

    return parser.parse_args()


def main():
    """
    STUDENT TODO: Customize the main execution flow as needed
    [completed] simply adjusted to accompodate for the api_key and
                location initialization.
    """

    print("=" * 60)
    print("WEATHER API KAFKA DATA PRODUCER")
    print("=" * 60)
    
    # Parse command-line arguments
    args = parse_arguments()

    api_key = args.api_key or os.environ.get('WEATHER_API_KEY')

    if not api_key:
        print("FATAL ERROR: Weather API Key not found.")
        print("Please set the WEATHER_API_KEY environment variable or use the --api-key command-line argument.")
        return
    
    # Initialize producer
    producer = StreamingDataProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        api_key=api_key,
        location=args.location
    )
    
    # Start producing stream
    try:
        producer.produce_stream(
            messages_per_second=args.rate,
            duration=args.duration
        )
    except Exception as e:
        print(f"Main execution error: {e}")
    finally:
        print("Producer execution completed")

if __name__ == "__main__":
    main()