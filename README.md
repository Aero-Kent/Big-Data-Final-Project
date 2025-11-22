# 🚀 Weather Data Dashboard: Project Guide

**GROUP MEMBERS**  
    De Guzman, Aero Kent **(leader)**  
    Alegre, Jhon Isaac  
    Garcia, Ron Allen  
    Molino, Carlos Ferdinand  
    Naigan, King Darryl

The projects provides the output construction of a big data streaming dashboad using **Kafka** and **Streamlit** with the data being sourced from the WeatherAPI, featuring separate real-time and historical data views.

## 🎯 Architecture & Components

A project features a **dual-pipeline** architecture separating both real-time with the historical for long-term storage and analysis.

| Pipeline | Flow | Output |
| :--- | :--- | :--- |
| **Real-time** | Kafka $\rightarrow$ Streamlit (Live Consumer) | `📈 Real-time Streaming View` |
| **Historical** | Kafka $\rightarrow$ **MongoDB** $\rightarrow$ Streamlit (Query) | `📊 Historical Data View` |

### Mandatory Components
* Kafka Producer/Consumer.
* **MongoDB** integration.
* Two-page Streamlit dashboard with charts.
* Robust error handling.

---

## 💻 Technical Implementation Tasks

### 1. Data Producer (`producer.py`)
Create a Kafka Producer that fetches real data from an **existing Application Programming Interface (API)** (e.g., a public weather API, stock market API, etc.).
Using a Kafka Producer, real data was fetched from the https://www.weatherapi.com to obtain current data
with the specific fields being the following.

**Required Data Schema Fields:**
* producer_timestamp (datetime)
* location_name (string)
* region (string)
* country (string)
* temp_c (numeric)
* humidity (numeric)
* wind_kph (numeric)
* condition_text (string)
* is_day (boolean)
* api_time (numeric)

### 2. Dashboard (`app.py`)
Implement the Streamlit logic:
* `consume_kafka_data()`: Real-time processing.
* `Database Storage` : Date stroage onto MongoDB
* `query_historical_data()`: Data retrieval from MongoDB.
* Create interactive widgets (filters, time-range selector) for the Historical View.

### 3. Storage Integration
Implement data writing and querying for **ONE** of the following: **HDFS** or **MongoDB** 
with the group opting to integrating **MongoDB** instead.

---

## 🏃‍♂️ Setup & Execution

### Prerequisites
Python 3.8+, Apache Kafka, MongoDB.

### Setup
1. **Setup environment**
    - Download miniconda
    - Create your python environment
    ```bash
    conda create -n <YOUR_VENV_NAME> python=3.12.11
    ```
2.  **Clone Repo & Install:**
    ```bash
    git clone [REPO_URL] (or simply download a .zip file instead)
    conda activate <YOUR_VENV_NAME>
    pip install -r requirements.txt
    ```
3.  **Configure:** Set up Kafka, activate MongoDB (e.g. configuring the proper credentials),
                    sign-up/login on the https://www.weatherapi.com/ to obtain an API-KEY.
4.  **Optional Environment File (`.env`):** Configure the details as provided in the .env

### Execution
1.  **Start Kafka Broker** (and Controller).
2.  **Start Producer:**
    ```bash
    python producer.py
    ```
3.  **Launch Dashboard:**
    ```bash
    streamlit run app.py
    ```
---
**brief output overview example**
<img width="1436" height="894" alt="gsrgrzghdrzh" src="https://github.com/user-attachments/assets/c859ac4a-f260-46e6-8ffe-9bc5efe770ab" />

---
