# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import time
from json import JSONDecodeError

import pika
import datetime

from auto.settings import URL, TASK_QUEUE, RESULT_QUEUE

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
            except JSONDecodeError as error:
                raise RuntimeError(error)

            print("Urls to scrap: {}".format(urls_to_scrap))

            spider.start_urls = urls_to_scrap

        channel = connection.channel()
        channel.queue_declare(queue=TASK_QUEUE)
        channel.basic_consume(TASK_QUEUE, on_message)
        channel.start_consuming()
        # time.sleep(1)

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
        channel.queue_declare(queue=RESULT_QUEUE)

        if not self.scrapped_items:
            self.scrapped_items.append(str(datetime.datetime.now()))

        result = json.dumps(self.scrapped_items, indent=4, separators=(',', ': '), ensure_ascii=False)

        channel.basic_publish(
            exchange='',
            routing_key=RESULT_QUEUE,
            body=result
        )

        if DEBUG: print("Results: {}".format(self.scrapped_items))

        spider.start_urls = []
        self.scrapped_items = []

        # time.sleep(1)
        connection.close()

        if DEBUG: print("Finished")
