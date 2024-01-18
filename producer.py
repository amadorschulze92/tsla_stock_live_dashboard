from confluent_kafka.admin import AdminClient
import os
from dotenv import load_dotenv
import finnhub
import websocket
import json
from confluent_kafka import Producer
import time


# Load environment variables
load_dotenv()
def load_config():
    """Load Kafka configuration."""
    return {
        'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
        'security.protocol': os.getenv('SECURITY_PROTOCOL'),
        'sasl.mechanisms': os.getenv('SASL_MECHANISMS'),
        'sasl.username': os.getenv('SASL_USERNAME'),
        'sasl.password': os.getenv('SASL_PASSWORD')
    }

config = load_config()
admin_client = AdminClient(config)


# Producer callback
def delivery_report(err, msg):
    """Callback to report the result of the produce operation."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


# function that actually produces data from finnhub to confluent
counter = 0
def on_message(ws, message):
    # initialize data
    topic_name = 'TSLA'
    global counter
    producer = Producer(config)
    # load data from finnhub
    data = json.loads(message)
    for d in data['data']:
        print(d)
        message_key = str(counter) + '_' + str(d['t'])
        message_value = json.dumps(d, indent=2).encode('utf-8')
        producer.produce(topic_name, key=message_key, value=message_value, callback=delivery_report)
        producer.poll()
        time.sleep(0.1)
    producer.flush()
    counter += 1
    print('='*7, 'Messages received Count','='*7 , counter)
    ### Comment the below out during production leave in during testing.
    # if counter == 10:
    #     ws.close()


def on_error(ws, error):
    print(error)


def on_close(ws):
    print("### closed ###")


def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"TSLA"}')




### main: Producer
base_url = 'wss://ws.finnhub.io?'
query = 'token={}'.format(os.getenv('FINNHUB_API'))
topic_name = "TSLA"
websocket.enableTrace(True)
ws = websocket.WebSocketApp(base_url + query,
                            on_message = on_message,
                            on_error = on_error,
                            on_close = on_close)
ws.on_open = on_open
ws.run_forever()