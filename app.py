"""
Streaming Data Dashboard Template
STUDENT PROJECT: Big Data Streaming Dashboard

This is a template for students to build a real-time streaming data dashboard.
Students will need to implement the actual data processing, Kafka consumption,
and storage integration.

IMPLEMENT THE TODO SECTIONS
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import time
import json
from datetime import datetime, timedelta, UTC 
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from streamlit_autorefresh import st_autorefresh

import os
import pymongo

from dotenv import load_dotenv
load_dotenv()
DEFAULT_MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = os.environ.get("MONGO_DB")
COLLECTION_NAME = os.environ.get("MONGO_COLLECTION")

# Page configuration
st.set_page_config(
    page_title="Philippines Weather Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def setup_sidebar():
    """
    STUDENT TODO: Configure sidebar settings and controls
    Implement any configuration options students might need
    """
    st.sidebar.title("Dashboard Controls")

    # [completed] STUDENT TODO: Add configuration options for data sources
    st.sidebar.subheader("Kafka Configuration")
    
    # STUDENT TODO: Configure your Kafka broker address
    # [completed] left it as the usual localhost:9092
    kafka_broker = st.sidebar.text_input(
        "Kafka Broker", 
        value="localhost:9092",
        help="The Kafka broker address."
    )    

    # STUDENT TODO: Specify the Kafka topic to consume from
    # [completed] changed it to weather-data
    kafka_topic = st.sidebar.text_input(
        "Kafka Topic", 
        value="weather-data",
        help="The Kafka topic to consume from."
    )
    
    # STUDENT TODO: Choose your historical data storage solution
    # [completed] Selected MongoDB as the historical data storage solution
    #             to be used.
    st.sidebar.subheader("MongoDB Configuration")
    mongo_uri = st.sidebar.text_input(
        "MongoDB Connection URI",
        value=DEFAULT_MONGO_URI,
        help="URI for your MongoDB instance"
    )

    if not (DB_NAME and COLLECTION_NAME):
        st.sidebar.error("ERROR: DB_NAME and COLLECTION_NAME must be set in your .env file.")
    else:
        st.sidebar.info(f"DB: {DB_NAME}, Collection: {COLLECTION_NAME}")
    
    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "mongo_uri": mongo_uri
    }

# def generate_sample_data(): # no longer used since this was for demonstration purposes as was said.
    """
    STUDENT TODO: Replace this with actual data processing
    
    This function generates sample data for demonstration purposes.
    Students should replace this with real data from Kafka and storage systems.

    [completed] Since this function was for demonstration purposes, this was changed
                to various functions that does the generation through an api
                piece by piece.
    """

@st.cache_resource(ttl=300) 
def get_mongodb_client(mongo_uri):
    """Initializes and caches the MongoDB client connection."""
    
    if 'logs' not in st.session_state:
        st.session_state.logs = []
        
    # Check if the pymongo package is available globally
    if not pymongo:
        st.session_state.logs.append("FATAL: pymongo not installed.")
        return None
    
    log_msg = f"--- ATTEMPTING MONGO CONNECTION (Timeout: 5s) ---"
    st.session_state.logs.append(log_msg)

    try:
        # Connect to MongoDB using the fully qualified name
        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        # Check connection immediately (ping command)
        client.admin.command('ping')
        
        # Log success and update sidebar
        success_msg = "Successfully connected to MongoDB."
        st.session_state.logs.append(success_msg)
        st.sidebar.success("MongoDB Connection: 🟢 OK")
        return client
        
    # Catch specific errors using the fully qualified path
    except pymongo.errors.ConnectionError as e:
        # Log error and update sidebar
        error_msg = f"MongoDB Connection Error (NETWORK/SERVER issue): Failed to reach the server at {mongo_uri}. Common cause: IP not whitelisted in Atlas."
        st.session_state.logs.append(f"ERROR: {error_msg} Details: {e}")
        st.sidebar.error("MongoDB Connection Failed (ConnectionError).")
        return None
        
    except pymongo.errors.OperationFailure as e:
        # Log error and update sidebar
        error_msg = "MongoDB Operation Failure (AUTHENTICATION/FIREWALL issue): Check credentials, username/password, and ensure the firewall allows access."
        st.session_state.logs.append(f"ERROR: {error_msg} Details: {e}")
        st.sidebar.error("MongoDB Connection Failed (OperationFailure).")
        return None
        
    except Exception as e:
        # Log generic error and update sidebar
        error_msg = f"MongoDB Generic Error: {e}"
        st.session_state.logs.append(f"ERROR: {error_msg}")
        st.sidebar.error("MongoDB Connection Failed (Unknown Error).")
        return None

@st.cache_resource(ttl=300)
def get_kafka_consumer(kafka_broker, kafka_topic):
    try:
        consumer = KafkaConsumer(
            kafka_topic,
            bootstrap_servers=[kafka_broker],
            group_id='streamlit-weather-group',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=5000
        )
        st.sidebar.success("Kafka Consumer: 🟢 OK")
        return consumer
    except Exception as e:
        st.sidebar.error(f"Kafka Consumer Failed. Check broker: {kafka_broker}")
        return None

def get_latest_record_from_mongodb(client):
    """
    This function fetches the single most recent record from MongoDB and are used
    for initializing the metrics or as a fallback when Kafka is empty so that the visualization
    displays from streamlit does not show a blank.
    """
    if not client or not (DB_NAME and COLLECTION_NAME):
        return None
        
    try:
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        latest_record = collection.find_one(sort=[("producer_timestamp", -1)])
        
        if latest_record and '_id' in latest_record:
            latest_record['_id'] = str(latest_record['_id'])
            if isinstance(latest_record.get('producer_timestamp'), str):
                try:
                    latest_record['producer_timestamp'] = datetime.fromisoformat(latest_record['producer_timestamp'].replace('Z', '+00:00'))
                except ValueError:
                    pass
        
        return latest_record
    except Exception as e:
        print(f"Error fetching latest record from MongoDB: {e}")
        return None

def insert_to_mongodb(client, data: dict):
    """
    Inserts the weather records into MongoDB.
    """
    if not client or not (DB_NAME and COLLECTION_NAME):
        print("MongoDB client or DB/Collection names are missing/invalid. Skipping insertion.")
        return
    
    data_to_insert = data.copy()
    
    try:
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        timestamp_str = data_to_insert.get('producer_timestamp')
        if isinstance(timestamp_str, str):
            try:
                data_to_insert['producer_timestamp'] = datetime.fromisoformat(timestamp_str)
                
            except ValueError:
                print(f"Warning: Could not parse timestamp string '{timestamp_str}'. Skipping insertion.")
                return

        # Check if the conversion resulted in a datetime object before inserting
        if not isinstance(data_to_insert.get('producer_timestamp'), datetime):
             print(f"Warning: Timestamp is not a datetime object after conversion. Skipping insertion: {data_to_insert}")
             return

        result = collection.insert_one(data_to_insert)
        print(f"✅ Successfully inserted record with ID: {result.inserted_id}")
        
    except Exception as e:
        print(f"❌ Error inserting data into MongoDB: {e}")

def consume_kafka_data(config, mongo_client):
    """
    STUDENT TODO: Implement actual Kafka consumer
    [completed] separated it into two with the other being found at get_kafka_data()
                to be able to read it better...

                Also contains the mongoDB insertion of the data for the historical view.
    """
    kafka_broker = config.get("kafka_broker")
    kafka_topic = config.get("kafka_topic")
    
    consumer = get_kafka_consumer(kafka_broker, kafka_topic)
    
    if not consumer:
        return pd.DataFrame()

    messages = []
    
    try:
        msg_pack = consumer.poll(timeout_ms=500, max_records=10)
        
        for tp, messages_batch in msg_pack.items():
            for message in messages_batch:
                try:
                    data = message.value
                    expected_keys = ['producer_timestamp', 'temp_c', 'humidity', 'location_name']
                    if all(key in data for key in expected_keys):
                        
                        insert_to_mongodb(mongo_client, data.copy())
                        
                        if isinstance(data.get('producer_timestamp'), str):
                            try:
                                data['producer_timestamp'] = datetime.fromisoformat(data['producer_timestamp'])
                            except ValueError:
                                print(f"Warning: Could not parse timestamp string for DataFrame: {data['producer_timestamp']}")
                                continue
                                
                        messages.append(data)
                    else:
                        print(f"Skipping message due to missing keys: {data}")
                except Exception as e:
                    print(f"Error processing Kafka message: {e}")
    except (NoBrokersAvailable, KafkaError, Exception) as e:
        st.error(f"Kafka runtime error during polling: {e}. Returning no data.")
        return pd.DataFrame()
    
    return pd.DataFrame(messages) if messages else pd.DataFrame()

def query_historical_data(client, time_range="24h", metric_type="temp_c"):
    """
    STUDENT TODO: Implement actual historical data query
    [completed] Queries MongoDB for historical weather data based
                on time range and metric.
    
    This function should:
    1. Connect to HDFS/MongoDB [completed]
    2. Query historical data based on time range and selected metrics [completed]
    3. Return aggregated historical data [completed]
    
    Parameters:
    - time_range: time period to query "1h", "24h", "7d", and "30d"
    - metrics: list of metric types to include
    """
    if not client or not (DB_NAME and COLLECTION_NAME):
        st.warning("Cannot query historical data: MongoDB client or configuration missing.")
        return pd.DataFrame()
    
    time_deltas = {
        "1h": timedelta(hours=1),
        "24h": timedelta(hours=24),
        "7d": timedelta(days=7),
        "30d": timedelta(days=30),
    }
        
    delta = time_deltas.get(time_range, timedelta(hours=24))
    start_time = datetime.now(UTC) - delta
    
    try:

        # 1. Connect to HDFS/MongoDB
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        # 2. Query historical data based on time range and selected metrics

        query = {
            "producer_timestamp": {"$gte": start_time}
        }
        
        cursor = collection.find(query).sort("producer_timestamp", 1)
        data = list(cursor)
        
        if not data:
            return pd.DataFrame()
            
        df = pd.DataFrame(data)

        df['producer_timestamp'] = pd.to_datetime(df['producer_timestamp'], utc=True)
        
        if '_id' in df.columns:
            df = df.drop(columns=['_id'])
            
        if metric_type in df.columns:
            df = df.rename(
                columns={metric_type: 'value', 'producer_timestamp': 'timestamp'}
            )
            core_columns = ['timestamp', 'location_name', 'value']
            df = df[[col for col in core_columns if col in df.columns]]
            df['metric_type'] = metric_type

        # 3. Return aggregated historical data
        return df

    except Exception as e:
        st.error(f"Error querying historical data from MongoDB: {e}")
        return pd.DataFrame()

def display_real_time_view(config, mongo_client, refresh_interval):
    """
    Page 1: Real-time Streaming View
    STUDENT TODO: Implement real-time data visualization from Kafka
    [completed] the data visualization can now be seen here, both real-time
                and historical graph uses the same graphs with the difference
                being that while real-time view also uses a line-graph it is more
                closed up and offers the generalized details such as the location,
                temperature, humidity, and wind-speed for the very latest as
                compared to the historical that only has options for hours to 30 days.
    """
    st.header("📈 Real-time Streaming Dashboard")
    
    with st.spinner(f"Fetching real-time data from Kafka and persisting to MongoDB... (Polling every {refresh_interval}s)"):
        real_time_data = consume_kafka_data(config, mongo_client)
    
    latest_record = None
    
    if not real_time_data.empty:
        st.success(f"🟢 Successfully consumed {len(real_time_data)} new records.")
        latest_record = real_time_data.sort_values('producer_timestamp', ascending=False).iloc[0].to_dict()
        st.session_state['latest_batch_df'] = real_time_data
    elif mongo_client:
        st.info("No new real-time data received from Kafka. Displaying latest known state from MongoDB...")
        latest_record = get_latest_record_from_mongodb(mongo_client)
        
    # --- Metric Cards (Uses latest record from Kafka OR MongoDB fallback) ---
    if latest_record:
        st.subheader("📊 Latest Weather Metrics (Live/Persisted)")
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        location_name = latest_record.get('location_name', 'N/A')
        temp_c = latest_record.get('temp_c', 0)
        humidity = latest_record.get('humidity', 0)
        wind_kph = latest_record.get('wind_kph', 0)
        condition_text = latest_record.get('condition_text', 'No weather condition provided')

        with col1:
            st.metric("Location", location_name)
        with col2:
            st.metric("Temperature (C)", f"{temp_c:.1f}")
        with col3:
            st.metric("Humidity (%)", f"{humidity:.1f}")
        with col4:
            st.metric("Wind Speed (kph)", f"{wind_kph:.1f}")
        with col5:
            st.metric("current weather condition", condition_text, help = condition_text)
            
        # --- Real-time Chart ---
        if not st.session_state['latest_batch_df'].empty:
            st.subheader("🌡️ Real-time Temperature Trend (Last Batch)")
            
            temp_data = st.session_state['latest_batch_df'].rename(columns={'producer_timestamp': 'Time', 'temp_c': 'Temperature (°C)'})
            
            fig = px.line(
                temp_data,
                x='Time',
                y='Temperature (°C)',
                color='location_name', 
                title=f"Live Temperature Stream (Last {len(temp_data)} records)",
                labels={'Temperature (°C)': 'Temperature (°C)', 'Time': 'Timestamp'},
                template='plotly_white',
                line_shape='linear'
            )
            fig.update_layout(hovermode='x unified')
            st.plotly_chart(fig, width='stretch')
            
            with st.expander("📋 View Raw Data (Last Processed Batch)"):
                 st.dataframe(
                    st.session_state['latest_batch_df'].sort_values('producer_timestamp', ascending=False),
                    width='stretch',
                    height=300,
                    hide_index=True
                )
        else:
            st.info("Waiting for the first batch of data from Kafka to populate the real-time chart.")
    
    else:
        st.warning("No data available. Waiting for a successful MongoDB connection or Kafka message.")


def display_historical_view(config, mongo_client):
    """
    STUDENT TODO: Implement historical data query and visualization
    [completed] the output is the following.
    """
    st.header("📊 Historical Data Analysis (from MongoDB)")
    
    # Interactive controls
    st.subheader("Data Filters")
    col1, col2 = st.columns(2)
    
    with col1:
        # [completed] STUDENT TODO: Implement time-based filtering in your query
        time_range = st.selectbox(
            "Time Range",
            ["1h", "24h", "7d", "30d"],
            index=1,
            help="Select the historical time period to analyze."
        )
    
    with col2:
        # [completed] STUDENT TODO: Implement metric filtering in your query
        metric_type = st.selectbox(
            "Metric to Analyze",
            ["temp_c", "humidity", "wind_kph"],
            format_func=lambda x: x.replace('_', ' ').title(),
            help="Select the metric type for historical analysis."
        )
    
    # [completed] STUDENT TODO: Replace with actual historical data query
    # Query historical data from MongoDB
    with st.spinner(f"Querying historical {metric_type.replace('_', ' ').title()} data for the last {time_range} from MongoDB..."):
        historical_data = query_historical_data(mongo_client, time_range, metric_type)
    
    if not historical_data.empty:
        st.subheader(f"Historical Trend: {metric_type.replace('_', ' ').title()} over {time_range}")
        # [completed] STUDENT TODO: Customize data display for your specific dataset
        
        y_label = f"{metric_type.replace('_', ' ').title()}"
        
        fig = px.line(
            historical_data,
            x='timestamp',
            y='value',
            color='location_name', 
            title=f"Historical {y_label} vs. Time ({len(historical_data)} records)",
            labels={'value': y_label, 'timestamp': 'Time'},
            template='plotly_white'
        )
        fig.update_layout(hovermode='x unified')
        st.plotly_chart(fig, width='stretch')
            
        # STUDENT TODO: Implement data aggregation in your query
        # [completed] this is where the aggregation are which are the average, min, and max
        #             values of the choosen metric.
        # [completed] STUDENT TODO: Implement meaningful historical analysis and visualization
        # [completed] STUDENT TODO: Customize historical trend analysis
        st.subheader("Data Summary")
        col1, col2, col3 = st.columns(3)
        
        data_series = historical_data['value']
        
        with col1:
            st.metric("Total Records", len(data_series))
            st.metric("Average Value", f"{data_series.mean():.2f}")
        
        with col2:
            st.metric("Max Value", f"{data_series.max():.2f}")
            st.metric("Min Value", f"{data_series.min():.2f}")

        with col3:
            min_ts = historical_data['timestamp'].min().strftime('%Y-%m-%d %H:%M') if not historical_data['timestamp'].empty else 'N/A'
            max_ts = historical_data['timestamp'].max().strftime('%Y-%m-%d %H:%M') if not historical_data['timestamp'].empty else 'N/A'
            st.metric("Date Range", f"{min_ts} to {max_ts}", help = f"{min_ts} to {max_ts}")

        # Raw data table
        with st.expander("📋 View Raw Historical Data"):
            st.dataframe(
                historical_data.drop(columns=['metric_type']),
                width='stretch',
                height=300,
                hide_index=True
            )
            
    else:
        st.warning(f"No historical data found in MongoDB for the last {time_range} matching metric '{metric_type}'. Ensure data is being produced and persisted.")

def main():
    """
    STUDENT TODO: Customize the main application flow as needed
    [completed] 
    """
    st.title("🚀 Philippines Weather Data Dashboard")

    # Setup configuration
    config = setup_sidebar()
    mongo_client = get_mongodb_client(config['mongo_uri'])

    with st.expander("📋 Project Instructions and 🛠️ MongoDB Connection Log for debugging"):
        if mongo_client:
            st.success("MongoDB Client Initialized successfully.")
        else:
            st.error("MongoDB Client is NULL. Check log details below.")
        
        st.text_area(
            "Detailed Connection Attempts",
            value='\n'.join(st.session_state.logs),
            height=200
        )
        st.markdown(
            """
            **STUDENT PROJECT TEMPLATE**
            
            ### Implementation Required:
            - [completed] **Real-time Data**: Connect to Kafka and process streaming data
            - [completed] **Historical Data**: Query from HDFS/MongoDB
            - [completed] **Visualizations**: Create meaningful charts
            - [completed] **Error Handling**: Implement robust error handling
            """
        )
    
    if 'latest_batch_df' not in st.session_state:
         st.session_state['latest_batch_df'] = pd.DataFrame()

    if 'logs' not in st.session_state:
         st.session_state.logs = []

    # Initialize session state for refresh management
    if 'refresh_state' not in st.session_state:
        st.session_state.refresh_state = {
            'last_refresh': datetime.now(),
            'auto_refresh': True
        }
    
    # Refresh controls in sidebar
    st.sidebar.subheader("Refresh Settings")
    st.session_state.refresh_state['auto_refresh'] = st.sidebar.checkbox(
        "Enable Auto Refresh",
        value=st.session_state.refresh_state['auto_refresh'],
        help="Automatically refresh real-time data"
    )

    if st.session_state.refresh_state['auto_refresh']:
        refresh_interval = st.sidebar.slider(
            "Refresh Interval (seconds)",
            min_value=5,
            max_value=60,
            value=15,
            help="Set how often real-time data refreshes"
        )
        
        # Auto-refresh using streamlit-autorefresh package
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")
    else:
          refresh_interval = 15
    
    # Manual refresh button
    if st.sidebar.button("🔄 Manual Refresh"):
        st.session_state.refresh_state['last_refresh'] = datetime.now()
        st.session_state.logs = []
        st.rerun()
    
    # Display refresh status
    st.sidebar.markdown("---")
    st.sidebar.metric("Last Refresh", st.session_state.refresh_state['last_refresh'].strftime("%H:%M:%S"))
    
    # Create tabs for different views
    tab1, tab2 = st.tabs(["📈 Real-time Streaming", "📊 Historical Data"])
    
    with tab1:
        display_real_time_view(config, mongo_client, refresh_interval)
    
    with tab2:
        display_historical_view(config, mongo_client)

if __name__ == "__main__":
    main()
