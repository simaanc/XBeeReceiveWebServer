from flask import Flask, request
import os
import csv
import configparser
from datetime import datetime
from pathlib import Path
import json

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.flux_table import FluxStructureEncoder
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.rest import ApiException

# Paths and configuration
source_path = Path(__file__).resolve()
source_dir = source_path.parent
config = configparser.ConfigParser()
configLocation = str(source_dir) + "/configfile.ini"

app = Flask(__name__)

def write_file():
    with open(configLocation, "w") as configfile:
        config.write(configfile)

if not os.path.exists(configLocation):
    config["ServerConf"] = {
        "api_key": "your_api_key_here",
        "influx_token": "your_influx_token_here",
        "influx_org": "your_influx_org_here",
        "influx_bucket": "your_influx_bucket_here",
        "influx_url": "your_influx_url_here"
    }
    write_file()
else:
    config.read(configLocation)
    print(config.sections())

API_KEY = config["ServerConf"]["api_key"]
INFLUX_TOKEN = config["ServerConf"]["influx_token"]
INFLUX_ORG = config["ServerConf"]["influx_org"]
INFLUX_BUCKET = config["ServerConf"]["influx_bucket"]
INFLUX_URL = config["ServerConf"]["influx_url"]

# Instantiate the client library
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)

# Instantiate the write and query apis
write_api = client.write_api(write_options=SYNCHRONOUS)
query_api = client.query_api()

def validate_token(token):
    return token == API_KEY

CSV_FILE = "received_data.csv"

def write_data_to_csv(data):
    headers = ["source_address_64", "date_time", "data"]
    mode = "a" if os.path.exists(CSV_FILE) else "w"

    with open(CSV_FILE, mode, newline='') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=headers)
        if mode == "w":
            csv_writer.writeheader()
        csv_writer.writerow(data)

@app.route('/receive', methods=['POST'])
def receive_data():
    try:
        # Get the Bearer token from the Authorization header
        bearer_token = request.headers.get('Authorization')
        if not bearer_token:
            return "Unauthorized", 401

        # Extract the token value from the Bearer token
        token = bearer_token.split(' ')[1]

        if not validate_token(token):
            return "Unauthorized", 401

        data = request.get_json()
        print("Received POST request data:")
        print(data)

        # Store data in CSV file
        write_data_to_csv(data)

        node = request.json["source_address_64"]
        value = request.json["data"]
        time = request.json["date_time"]

        point = (
            Point("sensor_data")
            .tag("node", node)
            .field("value", value)
            .time(time, WritePrecision.NS)
        )

        write_api.write(INFLUX_BUCKET, INFLUX_ORG, point)
        return {"result": "data accepted for processing"}, 200
    
    except InfluxDBError as e:
        if e.response.status == "401":
            return {"error": "Insufficent permissions"}, e.response.status
        if e.response.status == "404":
            return {"error": f"Bucket {INFLUX_BUCKET} does not exist"}, e.response.status
    except Exception as e:
        print("Error:", e)
        return "Error processing data", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)