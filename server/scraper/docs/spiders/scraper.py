from scrapy.spiders import Request, SitemapSpider, Spider
from scrapy.utils.sitemap import Sitemap
from scrapy_playwright.page import PageMethod
from scrapy.http import Response

def parse_response(response: Response):
    url = response.url
    xpath_query = "/descendant::div[contains(@class, 'section__content')][2]/div/node()"

    doc = response.xpath(xpath_query).extract()

    if doc:
        title = response.css("title::text").get().split(" |")[0]
        desc = response.css("meta[name=description]::attr(content)").get() or title

        return {
            "meta": {
                "title": title,
                "desc": desc,
                "url": url
            },
            "doc": doc
        }      
    else:
        return {"generated_idx": True} 

class DocSpider(SitemapSpider):
    name = "docs"
    page_total = 0
    custom_settings = {
        "ITEM_PIPELINES": {
            "scraper.docs.pipelines.DocsPipeline": 300,
        }
    }

    sitemap_urls = ["https://support.aerospike.com/s/sitemap-topicarticle-1.xml"]

    def _parse_sitemap(self, response):
        body = self._get_sitemap_body(response)
        sitemap = Sitemap(body)

        for entry in sitemap:
            url = entry["loc"]
            self.page_total += 1
            yield Request(url, parse_response, meta={
                "playwright": True, 
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "article.content > div.full")
                ]
            })