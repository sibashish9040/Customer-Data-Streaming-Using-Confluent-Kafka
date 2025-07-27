# Customer-Data-Streaming-Using-Confluent-Kafka
Stream customer records from a CSV file to a Kafka topic using the Confluent Kafka Python client. A practical demo of real-time data ingestion.

This project demonstrates how to stream customer data from a CSV file into an Apache Kafka topic using the Confluent Kafka Python client. It simulates a real-time ingestion pipeline by converting CSV rows to JSON and publishing them as Kafka messages.

## 🧩 Features

- Load customer data using `pandas`
- Convert CSV to JSON records
- Produce messages to Kafka with unique message keys
- Uses Confluent Cloud-compatible client configuration
- Includes message delivery callback for status logging

## 📁 Files in the Repository
```
.
├── src/
│   └── confluent_kafka_producer.py
├── data/
│   ├── customer.json
│   └── first_100_customers.csv
├── config/
│   ├── client.properties
│   └── requirement.txt
└── README.md

```

## ⚙️ Setup & Execution

Follow the steps below to set up and run the Kafka producer:

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/customer-data-streaming-kafka.git
cd customer-data-streaming-kafka
```

### 2. Install Dependencies
```
pip install pandas
pip install confluent-kafka
```
### 3. Prepare Kafka configuration
- Please refer to client.properties
### 4. Run the Kafka procedure
```
python confluent_kafka_producer.py
```
After succesful completion you will receieve message like:
```
Message delivered to ecommerce[0] at offset 123
Message delivered to ecommerce[0] at offset 124
...
```


