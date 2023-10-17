from google.cloud import pubsub_v1

def callback(message):
    country = message.data.decode('utf-8')
    print(f"ERROR: Forbidden request from {country}. Access Denied due to export restrictions!")
    message.ack()

def main():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path('myaccountproject', 'forbidden-requests-sub')
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

    print(f"Listening for messages on {subscription_path}...")

    try:
        # Block indefinitely to keep the subscriber running
        streaming_pull_future.result()
    except Exception as e:
        streaming_pull_future.cancel()
        print(f"Subscriber failed with exception: {e}")

if __name__ == '__main__':
    main()
