# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request
from urllib.parse import urljoin

from MakeMoneyOnline.items import MakemoneyonlineItem


class MessageSpider(scrapy.Spider):
    name = 'message'
    start_urls = [
        'https://www.wz169.com/category/xianbao',
        'http://sthd100.com'
    ]

    def choose_parse(self, url):
        def_dict = {
            'https://www.wz169.com/category/xianbao': self.parse,
            'http://sthd100.com': self.sth_parse
        }
        return def_dict[url]

    def start_requests(self):
        for item in self.start_urls:
            yield Request(url=item, callback=self.choose_parse(item))

    def parse(self, response):
        body_list = response.xpath('.//main[@id="main"]//article')
        for item in body_list:
            title = item.xpath('./header//a/text()').extract_first()
            desc = item.xpath('./div[@class="entry-content"]/div[@class="archive-content"]/text()').extract_first()
            cover = item.xpath('./figure//img/@src').extract_first()
            link = item.xpath('./span/a/@href').extract_first()
            tag = ""
            source = '网赚活动网'
            push_time = item.xpath('./div[@class="entry-content"]//span[@class="date"]/text()').extract_first()
            make_item = MakemoneyonlineItem()
            make_item['title'] = title
            make_item['desc'] = desc
            make_item['cover'] = cover
            make_item['link'] = link
            make_item['tag'] = tag
            make_item['source'] = source
            make_item['push_time'] = push_time
            yield Request(url=link, meta={"item": make_item}, callback=self.parse_content)

    def parse_content(self, response):
        make_item = response.meta.get("item", {})
        # 保存body部分
        make_item['content'] = response.xpath('//div[@class="single-content"]').extract_first("")
        yield make_item

    def sth_parse(self, response):
        counts = response.meta.get("counts", 1)
        if counts < 5:
            body_list = response.xpath('.//div[@class="layui-tab-item layui-show"]/ul/li[@class="list_li"]')
            next_page = response.xpath(
                './/div[@class="layui-tab-item layui-show"]/ul[@class="pager"]/li[2]/a/@href').extract_first()
            format_url = urljoin(response.url, next_page)
            yield Request(url=format_url, meta={'counts': counts + 1}, callback=self.sth_parse)
            for item in body_list:
                title = item.xpath('./div[@class="list_title"]/a/text()').extract_first()
                desc = ''
                cover = ''
                source = '惠享快报'
                link = urljoin(response.url, item.xpath('./div[@class="list_title"]/a/@href').extract_first())
                tag = item.xpath('./div[@class="list_title"]/div[@class="div_type_1"]//a/text()').extract()
                push_time = item.xpath('./div[@class="div_ico-date"]/div/text()').extract_first()
                make_item = MakemoneyonlineItem()
                make_item['title'] = title
                make_item['desc'] = desc
                make_item['cover'] = cover
                make_item['link'] = link
                make_item['tag'] = '-'.join(tag)
                make_item['source'] = source
                make_item['push_time'] = '2020-' + push_time
                yield Request(url=link, meta={"item": make_item}, callback=self.sth_pars_content)

    def sth_pars_content(self, response):
        make_item = response.meta.get("item", {})
        # 保存body部分
        make_item['content'] = response.xpath('//div[@class="div_content_text"]').extract_first("")
        yield make_item
