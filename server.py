from flask import Flask, request, abort, make_response
from google.cloud import storage, pubsub_v1
from google.cloud import logging

app = Flask(__name__)

BANNED_COUNTRIES = ['North Korea', 'Iran', 'Cuba', 'Myanmar', 'Iraq', 'Libya', 'Sudan', 'Zimbabwe', 'Syria']

logging_client = logging.Client()
log_name = "hw4-logs"
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

@app.route('/<path:path>', methods=['GET'])
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
        return file_content, 200
    except Exception as e:
        logger.log_text(f"404 Not Found: {e}", severity='ERROR')
        return make_response("Not Found", 404)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)

