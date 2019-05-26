# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import pika
import datetime

from auto.settings import (
    URL,
    EXCHANGE,
    TASK_QUEUE,
    RESULT_QUEUE,
    ROUTING_KEY_TO_TASK_QUEUE,
    ROUTING_KEY_TO_RESULT_QUEUE
)

DEBUG = True


class AutoPipeline(object):
    def __init__(self):
        self.scrapped_items = []

    def open_spider(self, spider):
        if DEBUG: print("Open spider. Waiting for a task...")

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

            try:
                urls_to_scrap = json.loads(body)
            except json.JSONDecodeError as error:
                raise RuntimeError(error)

            print("Urls to scrap: {}".format(urls_to_scrap))

            spider.start_urls = urls_to_scrap

        channel = connection.channel()
        channel.exchange_declare(exchange=EXCHANGE, exchange_type="direct")
        channel.queue_declare(queue=TASK_QUEUE)
        channel.queue_bind(exchange=EXCHANGE, queue=TASK_QUEUE, routing_key=ROUTING_KEY_TO_TASK_QUEUE)
        channel.basic_consume(queue=TASK_QUEUE, on_message_callback=on_message)
        channel.start_consuming()

        if DEBUG: print("Start scrapping")

    def process_item(self, item, spider):
        if DEBUG: print("Processing item: {}".format(item))

        self.scrapped_items.append(item)
        return item

    def close_spider(self, spider):
        if DEBUG: print("Closing spider")

        params = pika.URLParameters(URL)
        params.socket_timeout = 5

        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.exchange_declare(exchange=EXCHANGE, exchange_type="direct")

        if not self.scrapped_items:
            self.scrapped_items.append(str(datetime.datetime.now()))

        body = json.dumps(self.scrapped_items, indent=4, separators=(',', ': '), ensure_ascii=False)

        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=ROUTING_KEY_TO_RESULT_QUEUE,
            body=body
        )

        if DEBUG: print("Results: {}".format(self.scrapped_items))

        spider.start_urls = []
        self.scrapped_items = []

        # time.sleep(1)
        connection.close()

        if DEBUG: print("Finished")
