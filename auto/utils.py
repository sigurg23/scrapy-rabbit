# -*- coding: utf-8 -*-

import pika
import json
import datetime

URL = "amqp://aytzrnsk:UBRrZHaH0qwS8l7dOiE3cM1ncK8LNdzn@bulldog.rmq.cloudamqp.com/aytzrnsk"


def consume():
    print("Waiting for a message...")

    parameters = pika.URLParameters(URL)
    parameters.socket_timeout = 5

    connection = pika.BlockingConnection(parameters=parameters)

    def on_message(channel, method_frame, header_frame, body):
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        channel.stop_consuming()
        connection.close()

        body = bytes(body).decode()
        body = body.strip()
        body = body.replace("\n", "")
        body = body.replace("\r", "")

        print("BODY: {}".format(body))
        print("Done...")

    channel = connection.channel()
    channel.exchange_declare(exchange="main_exchange", exchange_type="direct")
    channel.queue_declare(queue="tasks")
    channel.queue_bind(exchange="main_exchange", queue="tasks", routing_key="tasks")
    channel.basic_consume(queue="tasks", on_message_callback=on_message)
    channel.start_consuming()


def publish():
    print("Publishing...")

    params = pika.URLParameters(URL)
    params.socket_timeout = 5

    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(exchange="main_exchange", exchange_type="direct")

    data = [str(datetime.datetime.now()), "https://www.google.com", "https://www.yandex.ru"]
    body = json.dumps(data, indent=4, separators=(',', ': '), ensure_ascii=False)

    channel.basic_publish(
        exchange="main_exchange",
        routing_key="results",
        body=body
    )

    connection.close()

    print("Done...")


if __name__ == "__main__":
    consume()
    # publish()
