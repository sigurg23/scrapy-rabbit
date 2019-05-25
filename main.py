# -*- coding: utf-8 -*-

import sys
import subprocess
from time import sleep
from auto.settings import SPIDER_NAME, URL, TASK_QUEUE

try:
    while True:
        print("Running...")
        command = "scrapy crawl {}".format(SPIDER_NAME)
        subprocess.run(command, shell=True)
        sleep(1)

except KeyboardInterrupt:
    sys.exit(0)
