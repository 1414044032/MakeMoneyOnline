# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import MySQLdb
import MySQLdb.cursors
from twisted.enterprise import adbapi
from scrapy.exceptions import DropItem
import redis
from pybloom_live import ScalableBloomFilter


class MyspiderPipeline:

    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, setting):
        dbparms = dict(
            host=setting["MYSQL_HOST"],
            db=setting["MYSQL_DBNAME"],
            user=setting["MYSQL_USER"],
            passwd=setting["MYSQL_PASSWORD"],
            charset='utf8',
            port=setting["MYSQL_PORT"],
            cursorclass=MySQLdb.cursors.DictCursor,
            use_unicode=True,
        )
        dbpool = adbapi.ConnectionPool("MySQLdb", **dbparms)
        return cls(dbpool)

    # mysql异步插入执行
    def process_item(self, item, spider):
        query = self.dbpool.runInteraction(self.do_insert, item)
        query.addErrback(self.handle_error, item, spider)
        return item

    def handle_error(self, failure, item, spider):
        # 处理异步插入的异常
        print(failure)

    def do_insert(self, cursor, item):
        insert_sql, parms = item.get_insert_sql()
        cursor.execute(insert_sql, parms)


class SpiderUrlPipeline:
    # 爬取过得url保存到redis
    def process_item(self, item, spider):
        item.save_to_redis()
        return item


class RemoveSameUrlPipLine:
    def __init__(self):
        redis_db = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
        result = redis_db.smembers('spider:url')
        self.sbf = ScalableBloomFilter(mode=ScalableBloomFilter.SMALL_SET_GROWTH)
        for item in result:
            self.sbf.add(item)
    def process_item(self, item, spider):
        if item['link'] in self.sbf:
            raise DropItem("same title in %s" % item['link'])
        else:
            return item
