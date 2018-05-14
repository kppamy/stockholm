from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.selector import HtmlXPathSelector
from scrapy.http import Request

from selenium import webdriver

class SeleniumSpider(CrawlSpider):
    name = "SeleniumSpider"
    start_urls = ["http://www.domain.com"]

    rules = (
        Rule(LinkExtractor(allow=('\.html', )), callback='parse_page',follow=True),
    )

    def __init__(self):
        CrawlSpider.__init__(self)
        self.verificationErrors = []
        # self.selenium = selenium("localhost", 4444, "*chrome", "http://www.domain.com")
        # self.selenium.start()
        self.driver = webdriver.Firefox()

    def __del__(self):
        # self.selenium.stop()
        print(self.verificationErrors)
        CrawlSpider.__del__(self)

    def parse_page(self, response):
        # item = Item()

        self.driver.get(response.url)

        hxs = HtmlXPathSelector(response)
        # Do some XPath selection with Scrapy
        hxs.select('//div').extract()

        # sel = self.selenium
        # sel.open(response.url)

        # Wait for javscript to load in Selenium
        # time.sleep(2.5)

        # Do some crawling of javascript created content with Selenium
        next = self.driver.find_element_by_xpath('//td[@class="pagn-next"]/a')
        # yield item
        self.driver.close()