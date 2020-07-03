# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
import redis

redis_db = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)


class MakemoneyonlineItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    title = scrapy.Field()
    desc = scrapy.Field()
    cover = scrapy.Field()
    source = scrapy.Field()
    link = scrapy.Field()
    tag = scrapy.Field()
    push_time = scrapy.Field()
    content = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
                            insert into 
                            online_message (title,`desc`,cover,source,link,tag,pushtime,content) 
                            values (%s,%s,%s,%s,%s,%s,%s,%s)
                             """
        parms = (self['title'], self['desc'], self['cover'], self['source'], self["link"],
                 self["tag"], self["push_time"], self["content"])
        return insert_sql, parms

    def save_to_redis(self):
        redis_db.sadd('spider:url', self['link'])
        return
