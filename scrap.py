# -*- coding: utf-8 -*-

import time

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from auto.spiders.auto import AutoSpider


def scrap():
    settings = get_project_settings()
    settings['ITEM_PIPELINES'] = {'auto.pipelines.AutoPipeline': 1}

    process = CrawlerProcess(settings)
    process.crawl(AutoSpider)
    process.start(stop_after_crawl=False)
    time.sleep(1)


if __name__ == "__main__":
    scrap()
