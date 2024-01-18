import streamlit as st
import pandas as pd
import asyncio, os
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv
import json


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
CONSUMER_GROUP_ID = os.getenv('CONSUMER_GROUP_ID', 'default-group-id')


def on_assign(consumer, partitions):
    """Callback executed when partitions are assigned. Sets partition offset to beginning."""
    for partition in partitions:
        partition.offset = OFFSET_BEGINNING
    consumer.assign(partitions)


async def consume(topic_name, my_chart):
    """Asynchronously consume data from the specified Kafka Topic."""
    
    # Short delay before initiating the consumer
    await asyncio.sleep(2.5)

    # Configure consumer with Kafka settings and subscribe to the topic
    c = Consumer({
        **config,
        "group.id": CONSUMER_GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    c.subscribe([topic_name], on_assign=on_assign)

    # Continuously poll for new messages in the topic
    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")
            new_data = message.value()
            new_data = json.loads(new_data.decode('utf-8'))
            temp_df = pd.DataFrame([[new_data['p'], new_data['v']]],
                                   columns=['price', 'volume'])
            temp_df = temp_df.astype('float')
            my_chart.add_rows(temp_df)
            # temp_df.to_csv('tsla_data_log.csv', header=None, mode='a')
        await asyncio.sleep(0.1)  # Brief pause to reduce CPU load


async def consume_tsla(topic_name, my_chart):
    consumed_info = asyncio.create_task(consume(topic_name, my_chart))
    await consumed_info



def main():
    # initialize kafka info
    admin_client = AdminClient(config)
    topic_name = "TSLA"
    # initialize dataset
    df = pd.DataFrame(columns=['price', 'volume'])
    # df.to_csv('tsla_data_log.csv')
    my_chart = st.line_chart(df)
    # Start the main async function
    try:
        asyncio.run(consume_tsla(topic_name, my_chart))
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()

