from flask import Flask, request, abort, make_response
from google.cloud import storage, pubsub_v1
from google.cloud import logging
import mysql.connector
from datetime import datetime
import os

DB_CONFIG = {
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASS'),
    'host': os.environ.get('DB_HOST'),
    'database': os.environ.get('DB_NAME'),
    'use_pure': True
}

app = Flask(__name__)

BANNED_COUNTRIES = ['North Korea', 'Iran', 'Cuba', 'Myanmar', 'Iraq', 'Libya', 'Sudan', 'Zimbabwe', 'Syria']

logging_client = logging.Client()
log_name = "hw4-logs"
logger = logging_client.logger(log_name)

def insert_request_details(country, client_ip, gender, age, income, is_banned, time_of_day, requested_file):
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor()
    
    insert_query = ("INSERT INTO request_details (country, client_ip, gender, age, income, is_banned, time_of_day, requested_file) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
    data = (country, client_ip, gender, age, income, is_banned, time_of_day, requested_file)
    
    cursor.execute(insert_query, data)
    connection.commit()
    
    cursor.close()
    connection.close()

def insert_failed_request(time_of_request, requested_file, error_code):
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor()
    
    insert_query = "INSERT INTO failed_requests (time_of_request, requested_file, error_code) VALUES (%s, %s, %s)"
    data = (time_of_request, requested_file, error_code)
    
    cursor.execute(insert_query, data)
    connection.commit()
    
    cursor.close()
    connection.close()

def get_client_ip():
    if 'X-Forwarded-For' in request.headers:
        # In case of multiple proxies, the left-most IP is the original client.
        original_client_ip = request.headers.getlist("X-Forwarded-For")[0].split(',')[0]
        return original_client_ip
    return request.remote_addr

@app.route('/<path:path>', methods=['GET', 'PUT', 'POST', 'DELETE', 'PATCH', 'OPTIONS', 'HEAD'])
def handle_request(path):
    if request.method != 'GET':
        error_message = "501 Not Implemented: The server does not support the functionality required to fulfill the request."
        logger.log_text(error_message, severity='ERROR')
        insert_failed_request(datetime.now(), "NULL", 501)
        return make_response("Not Implemented", 501)
    
    path_parts = path.lstrip('/').split('/')
    if len(path_parts) < 2:
        error_message = "400 Bad Request: Invalid path format."
        logger.log_text(error_message, severity='ERROR')
        insert_failed_request(datetime.now(), "NULL", 400)
        return make_response("Invalid path format", 400)
    
    bucket_name = path_parts[0]
    file_name = '/'.join(path_parts[1:])
    country = request.headers.get('X-country')
    logger.log_text(f"Received request for file: {file_name} from country: {country}")
    client_ip = get_client_ip()
    gender = request.headers.get('X-gender')
    age = request.headers.get('X-age', '0')
    income = request.headers.get('X-income', '0.0')
    is_banned = country in BANNED_COUNTRIES
    time_of_day = datetime.now().time()
    if country in BANNED_COUNTRIES:
        warning_message = f"Forbidden request from {country}. Sending message to second app."
        logger.log_text(warning_message, severity='WARNING')
        insert_failed_request(datetime.now(), file_name, 403)
        return make_response("Forbidden", 403)
    
    storage_client = storage.Client(project='myaccountProject')

    try:
        bucket = storage_client.get_bucket(bucket_name)
        blob = storage.Blob(file_name, bucket)
        file_content = blob.download_as_text()
        insert_request_details(country, client_ip, gender, age, income, is_banned, time_of_day, file_name)
        logger.log_text("200 OK: File retrieved successfully.")
        return file_content, 200
    except Exception as e:
        logger.log_text(f"404 Not Found: {e}", severity='ERROR')
        insert_failed_request(datetime.now(), file_name, 404)
        return make_response("Not Found", 404)

@app.route('/')
def hello_world():
    return 'Hello World!'

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)
