from flask import Flask, request, render_template
import os
import csv
import configparser
from datetime import datetime
from pathlib import Path
import uuid
import json
import numpy

from influxdb_client import InfluxDBClient, Point, WritePrecision, Task, TaskCreateRequest
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
INFLUX_BUCKET_1H = INFLUX_BUCKET + "_1h"
INFLUX_BUCKET_24H = INFLUX_BUCKET + "_24h"
INFLUX_BUCKET_1W = INFLUX_BUCKET + "_1w"

# Instantiate the client library
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)

# Instantiate the write and query apis
write_api = client.write_api(write_options=SYNCHRONOUS)
buckets_api = client.buckets_api()
tasks_api = client.tasks_api()
query_api = client.query_api()

def validate_token(token):
    return token == API_KEY

def write_data_to_csv(data):
    headers = ["source_address_64", "date_time", "data"]
    mode = "a" if os.path.exists(CSV_FILE) else "w"

    with open(CSV_FILE, mode, newline='') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=headers)
        if mode == "w":
            csv_writer.writeheader()
        csv_writer.writerow(data)

if buckets_api.find_bucket_by_name(INFLUX_BUCKET_1H) is None:
    print(INFLUX_BUCKET_1H + " does not exist, creating...")
    buckets_api.create_bucket(bucket_name=INFLUX_BUCKET_1H, org=INFLUX_ORG, retention_rules=[{"type": "expire", "everySeconds": 3600}])

if buckets_api.find_bucket_by_name(INFLUX_BUCKET_24H) is None:
    print(INFLUX_BUCKET_24H + " does not exist, creating...")
    buckets_api.create_bucket(bucket_name=INFLUX_BUCKET_24H, org=INFLUX_ORG, retention_rules=[{"type": "expire", "everySeconds": 86400}])

if buckets_api.find_bucket_by_name(INFLUX_BUCKET_1W) is None:
    print(INFLUX_BUCKET_1W + " does not exist, creating...")
    buckets_api.create_bucket(bucket_name=INFLUX_BUCKET_1W, org=INFLUX_ORG, retention_rules=[{"type": "expire", "everySeconds": 604800}])

if not tasks_api.find_tasks(name="1h_aggregation"):
    print("1h_aggregation task does not exist, creating...")
    
    orgs = client.organizations_api().find_organizations(org=INFLUX_ORG)
     
    query = f'''
    data = from(bucket: "{INFLUX_BUCKET_1H}")
        |> range(start: -duration(v: int(v: 1h)))
        |> filter(fn: (r) => r._measurement == "sensor_data")
    data
        |> aggregateWindow(fn: mean, every: 1h)
        
        |> to(bucket: "{INFLUX_BUCKET_24H}", org: "{INFLUX_ORG}")
    '''
    tasks_api.create_task_every(name="1h_aggregation", flux=query, every="1h", organization=orgs[0])

if not tasks_api.find_tasks(name="24h_aggregation"):
    print("24h_aggregation task does not exist, creating...")
    
    orgs = client.organizations_api().find_organizations(org=INFLUX_ORG)
     
    query = f'''
    data = from(bucket: "{INFLUX_BUCKET_24H}")
        |> range(start: -duration(v: int(v: 24h)))
        |> filter(fn: (r) => r._measurement == "sensor_data")
    data
        |> aggregateWindow(fn: mean, every: 24h)
        
        |> to(bucket: "{INFLUX_BUCKET_1W}", org: "{INFLUX_ORG}")
    '''
    
    tasks_api.create_task_every(name="24h_aggregation", flux=query, every="24h", organization=orgs[0])

# Flask routes
@app.route('/')
def index():
    return render_template("index.html")

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

        # point = (
        #     Point("sensor_data")
        #     .tag("node", node)
        #     .field("value", value)
        #     .time(time, WritePrecision.NS)
        # )

        value = float(value)

        dict_structure = {
            "measurement": "sensor_data",
            "tags": {"node": node},
            "fields": {
                "value": value,
            },
            "time": time
        }

        point = Point.from_dict(dict_structure)
        write_api.write(INFLUX_BUCKET_1H, INFLUX_ORG, point)
        return {"result": "data accepted for processing"}, 200
    
    except InfluxDBError as e:
        if e.response.status == "401":
            return {"error": "Insufficent permissions"}, e.response.status
        if e.response.status == "404":
            return {"error": f"Bucket {INFLUX_BUCKET} does not exist"}, e.response.status
    except Exception as e:
        print("Error:", e)
        return "Error processing data", 500
    
@app.route("/devices", methods=["GET"])
def get_devices():
    try:
        with mysql.connector.connect(
            host=sqlhost,
            user=sqluser,
            password=sqlpass,
            database="zigbee_data",
            port=sqlport,
        ) as connection:
            select_query = "SELECT id, node FROM devices_list"
            with connection.cursor() as cursor:
                cursor.execute(select_query)
                device_rows = cursor.fetchall()

                devices = [{"id": row[0], "node": row[1]} for row in device_rows]

                return jsonify(devices)
    except Error as e:
        print(e)
        return jsonify([])

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)