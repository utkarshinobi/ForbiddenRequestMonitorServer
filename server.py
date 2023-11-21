from flask import Flask, request, abort, make_response
from google.cloud import storage, pubsub_v1
from google.cloud import logging
import requests
import os

app = Flask(__name__)

BANNED_COUNTRIES = ['North Korea', 'Iran', 'Cuba', 'Myanmar', 'Iraq', 'Libya', 'Sudan', 'Zimbabwe', 'Syria']

logging_client = logging.Client()
log_name = "hw8-logs"
logger = logging_client.logger(log_name)

def publish_message(project_id, topic_name, message):
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_name)
 
        data = message.encode('utf-8')
        future = publisher.publish(topic_path, data=data)
        logger.log_text(f"Publishing message to {topic_path}")

        message_id = future.result()
        logger.log_text(f"Published message ID: {message_id}")

    except Exception as e:
        logger.log_text(f"An error occurred: {e}", severity='ERROR')

def get_instance_zone():
    """ Fetches the zone information of the current GCP instance. """
    metadata_server = "http://metadata.google.internal/computeMetadata/v1/instance/zone"
    metadata_flavor = {"Metadata-Flavor": "Google"}
    try:
        response = requests.get(metadata_server, headers=metadata_flavor)
        if response.status_code == 200:
            return response.text.split('/')[-1]
    except requests.exceptions.RequestException:
        return "Unknown"

@app.route('/<path:path>', methods=['GET', 'PUT', 'POST', 'DELETE', 'PATCH', 'OPTIONS', 'HEAD'])
def handle_request(path):
    if request.method != 'GET':
        error_message = "501 Not Implemented: The server does not support the functionality required to fulfill the request."
        logger.log_text(error_message, severity='ERROR')
        return make_response("Not Implemented", 501)
    
    path_parts = path.lstrip('/').split('/')
    if len(path_parts) < 2:
        error_message = "400 Bad Request: Invalid path format."
        logger.log_text(error_message, severity='ERROR')
        return make_response("Invalid path format", 400)
    
    bucket_name = path_parts[0]
    file_name = '/'.join(path_parts[1:])
    country = request.headers.get('X-country')

    logger.log_text(f"Received request for file: {file_name} from country: {country}")

    if country in BANNED_COUNTRIES:
        warning_message = f"Forbidden request from {country}. Sending message to second app."
        logger.log_text(warning_message, severity='WARNING')
        project_id = 'myaccountproject'
        topic_name = 'forbidden-requests'
        message = f"Forbidden request attempted from {country} for file {file_name}"
        publish_message(project_id, topic_name, message)
        return make_response("Forbidden", 403)
    
    storage_client = storage.Client(project='myaccountProject')

    try:
        bucket = storage_client.get_bucket(bucket_name)
        blob = storage.Blob(file_name, bucket)
        file_content = blob.download_as_text()
        logger.log_text("200 OK: File retrieved successfully.")
        zone = get_instance_zone()  # Fetch the zone information
        response = make_response("File retrieved successfully.", 200)
        response.headers['X-Server-Zone'] = zone
        return response
    except Exception as e:
        logger.log_text(f"404 Not Found: {e}", severity='ERROR')
        zone = get_instance_zone()
        response = make_response("Not Found", 404)
        response.headers['X-Server-Zone'] = zone
        return response

@app.route('/', methods=['GET'])
def health_check():
    return "Health Check OK", 200

if __name__ == "__main__":
    app.run(host = '0.0.0.0', port = 80)


# curl http://EXTERNAL_IP:5000/your_endpoint


# curl -H "X-country: USA" http://35.211.56.56/utkarsh-hw2-bucket/new-folder/0.html
# curl -H "X-country: USA" http://127.0.0.1:5000/utkarsh-hw2-bucket/new-folder/0.html


# python3 http-client.py -d 35.196.164.219 -p 80 -b /utkarsh-hw2-bucket -w new-folder -n 100 -i 99 -v
