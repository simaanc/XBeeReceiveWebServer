from flask import Flask, request, render_template, jsonify
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
INFLUX_BUCKET_1M = INFLUX_BUCKET + "_1m"
INFLUX_BUCKET_DEVICES = INFLUX_BUCKET + "_devices"

INFLUX_TASK_1H = INFLUX_BUCKET + "_1h_aggregation"
INFLUX_TASK_24H = INFLUX_BUCKET + "_24h_aggregation"
INFLUX_TASK_1W = INFLUX_BUCKET + "_1w_aggregation"
INFLUX_TASK_1M = INFLUX_BUCKET + "_1m_aggregation"

# Instantiate the client library
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)

# Instantiate the write and query apis
write_api = client.write_api(write_options=SYNCHRONOUS)
buckets_api = client.buckets_api()
tasks_api = client.tasks_api()
query_api = client.query_api()

def validate_token(token):
    return token == API_KEY

if buckets_api.find_bucket_by_name(INFLUX_BUCKET_1H) is None:
    print(INFLUX_BUCKET_1H + " does not exist, creating...")
    buckets_api.create_bucket(bucket_name=INFLUX_BUCKET_1H, org=INFLUX_ORG, retention_rules=[{"type": "expire", "everySeconds": 3600}])

if buckets_api.find_bucket_by_name(INFLUX_BUCKET_24H) is None:
    print(INFLUX_BUCKET_24H + " does not exist, creating...")
    buckets_api.create_bucket(bucket_name=INFLUX_BUCKET_24H, org=INFLUX_ORG, retention_rules=[{"type": "expire", "everySeconds": 86400}])

if buckets_api.find_bucket_by_name(INFLUX_BUCKET_1W) is None:
    print(INFLUX_BUCKET_1W + " does not exist, creating...")
    buckets_api.create_bucket(bucket_name=INFLUX_BUCKET_1W, org=INFLUX_ORG, retention_rules=[{"type": "expire", "everySeconds": 604800}])

if buckets_api.find_bucket_by_name(INFLUX_BUCKET_1M) is None:
    print(INFLUX_BUCKET_1M + " does not exist, creating...")
    buckets_api.create_bucket(bucket_name=INFLUX_BUCKET_1M, org=INFLUX_ORG, retention_rules=[{"type": "expire", "everySeconds": 2592000}])

if buckets_api.find_bucket_by_name(INFLUX_BUCKET_DEVICES) is None:
    print(INFLUX_BUCKET_DEVICES + " does not exist, creating...")
    buckets_api.create_bucket(bucket_name=INFLUX_BUCKET_DEVICES, org=INFLUX_ORG)

if not tasks_api.find_tasks(name=INFLUX_TASK_1H):
    print(INFLUX_TASK_1H + " does not exist, creating...")
    
    orgs = client.organizations_api().find_organizations(org=INFLUX_ORG)
     
    query = f'''
    data = from(bucket: "{INFLUX_BUCKET_1H}")
        |> range(start: -duration(v: int(v: 1h)))
        |> filter(fn: (r) => r._measurement == "sensor_data")
    data
        |> aggregateWindow(fn: mean, every: 1h)
        
        |> to(bucket: "{INFLUX_BUCKET_24H}", org: "{INFLUX_ORG}")
    '''
    tasks_api.create_task_cron(name=INFLUX_TASK_1H, flux=query, cron="0 * * * *", org_id=orgs[0].id)

if not tasks_api.find_tasks(name=INFLUX_TASK_24H):
    print(INFLUX_TASK_1H + " does not exist, creating...")
    
    orgs = client.organizations_api().find_organizations(org=INFLUX_ORG)
     
    query = f'''
    data = from(bucket: "{INFLUX_BUCKET_24H}")
        |> range(start: -duration(v: int(v: 24h)))
        |> filter(fn: (r) => r._measurement == "sensor_data")
    data
        |> aggregateWindow(fn: mean, every: 24h)
        
        |> to(bucket: "{INFLUX_BUCKET_1W}", org: "{INFLUX_ORG}")
    '''
    
    tasks_api.create_task_cron(name=INFLUX_TASK_24H, flux=query, cron="0 0 * * *", org_id=orgs[0].id)

if not tasks_api.find_tasks(name=INFLUX_TASK_1M):
    print(INFLUX_BUCKET_1M + " does not exist, creating...")
    
    orgs = client.organizations_api().find_organizations(org=INFLUX_ORG)
     
    query = f'''
    data = from(bucket: "{INFLUX_BUCKET_1W}")
        |> range(start: -duration(v: int(v: 7d)))
    data        
        |> to(bucket: "{INFLUX_BUCKET_1M}", org: "{INFLUX_ORG}")
    '''
    
    tasks_api.create_task_cron(name=INFLUX_TASK_1M, flux=query, cron="0 0 * * 0", org_id=orgs[0].id)

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

        data_dict_structure = {
            "measurement": "sensor_data",
            "tags": {"node": node},
            "fields": {
                "value": value,
            },
            "time": time
        }
        
        devices_dict_structure = {
            "measurement": "devices_list",
            "tags": {"node": node},
            "fields": {
                "node": node,
            },
            "time": "2023-01-01T00:00:00Z"
        }

        data_point = Point.from_dict(data_dict_structure)
        devices_point = Point.from_dict(devices_dict_structure)
        write_api.write(INFLUX_BUCKET_1H, INFLUX_ORG, data_point)
        write_api.write(INFLUX_BUCKET_DEVICES, INFLUX_ORG, devices_point)
        return {"result": "data accepted for processing"}, 200
    
    except InfluxDBError as e:
        if e.response.status == "401":
            return {"error": "Insufficent permissions"}, e.response.status
        if e.response.status == "404":
            return {"error": f"Bucket {INFLUX_BUCKET} does not exist"}, e.response.status
    except Exception as e:
        print("Error:", e)
        return "Error processing data", 500
    
@app.route('/data')
def get_data():
    device = request.args.get('device')
    time_range = request.args.get('range')

    if time_range == '1h':
        bucket = INFLUX_BUCKET_1H
        duration = '1h'
    elif time_range == '24h':
        bucket = INFLUX_BUCKET_24H
        duration = '24h'
    elif time_range == '1w':
        bucket = INFLUX_BUCKET_1W
        duration = '1w'
    elif time_range == '1m':
        bucket = INFLUX_BUCKET_1M  # Use the 1w bucket for 1 Month
        duration = '4w'  # Adjust the duration as needed
    else:
        return jsonify([])  # Invalid time range, return empty data

    query = f'''
    from(bucket: "{bucket}")
        |> range(start: -{duration})
        |> filter(fn: (r) => r._measurement == "sensor_data" and r.node == "{device}")
    '''
    
    result = query_api.query(org=INFLUX_ORG, query=query)
    data = []

    for table in result:
        for record in table.records:
            data.append({
                "time": record.get_time(),
                "value": record.get_value()
            })
    
    return jsonify(data)
    
@app.route("/devices", methods=["GET"])
def get_devices():
    query = f'''
    from(bucket: "{INFLUX_BUCKET_DEVICES}")
        |> range(start: 2023-01-01T00:00:00Z, stop: 2023-01-01T00:00:01Z)
    '''
    
    records = query_api.query_stream(org=INFLUX_ORG, query=query)

    devices = []  # Initialize the devices list outside the loop

    for record in records:
        devices.append({"id": record["table"], "node": record["node"]})  # Append each record to the devices list
        
    return jsonify(devices)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)