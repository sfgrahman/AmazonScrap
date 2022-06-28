import requests
from bs4 import BeautifulSoup
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse
from lxml import html
import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/", maxPoolSize=200)
dblist = myclient.list_database_names()
mydb = myclient["mydatabase_product"]
if "mydatabase_product" in dblist:
    print("The database exists.")
    # myclient.drop_database('mydatabase_new_1st_part')
    # print("Database dropped")

mycol_all = mydb["product"]


class MultiThreadScraper:

    def __init__(self, base_url):

        self.base_url = base_url
        self.root_url = '{}://{}'.format(urlparse(self.base_url).scheme, urlparse(self.base_url).netloc)
        self.pool = ThreadPoolExecutor(max_workers=40)
        self.scraped_pages = set([])
        self.to_crawl = Queue()
        self.to_crawl.put(self.base_url)

    def parse_links(self, html_content, url):
        dom = html.fromstring(html_content)
        next_page_link_el = dom.xpath('//ul[@class="a-pagination"]/li[@class="a-last"]/a/@href')
        if len(next_page_link_el) > 0:
            next_url = f'https://www.amazon.com{next_page_link_el[0]}'
            print(next_url)
            if next_url not in self.scraped_pages:
                self.to_crawl.put(next_url)

        a_tag_text = dom.xpath('//span/div[@class="s-result-list s-search-results sg-row"]//*/h2/a/@href')
        if len(a_tag_text) > 0:
            for link in a_tag_text:
                product_url_f = f"https://www.amazon.com{link}"
                if product_url_f not in self.scraped_pages:
                    self.to_crawl.put(product_url_f)


    def scrape_info(self, html_content, product_url):
        dom_product = html.fromstring(html_content)
        try:
            product_title = dom_product.xpath('//*/span[@id="productTitle"]/text()')[0].strip()
        except Exception:
            product_title = ""
        try:
            avgrating = dom_product.xpath('//*/span[@id="acrPopover"]/@title')[0].strip()
        except Exception:
            avgrating = ""
        try:
            totalnumber = dom_product.xpath('//*/span[@id="acrCustomerReviewText"]/text()')[0].strip()
        except Exception:
            totalnumber = ""
        try:
            regularprice_i = dom_product.xpath('//*/span[@class="priceBlockStrikePriceString"]/text()')[0].strip()
        except Exception:
            regularprice_i = ""

        try:
            discountedprice_i = dom_product.xpath('//*/span[@id="priceblock_ourprice"]/text()')[0].strip()
        except Exception:
            discountedprice_i = ""

        try:
            breadcrumb = dom_product.xpath('//ul[@class="a-unordered-list a-horizontal a-size-small"]/li//*/a/text()')
            category = '>'.join([item.strip() for item in breadcrumb])
        except Exception:
            category = ""

        try:
            ac_link = dom_product.xpath('//*/span[@class="ac-keyword-link"]/a/@href')[0]
            final_link = f"https://www.amazon.com{ac_link}"
        except Exception:
            final_link = ""

        try:
            selected = dom_product.xpath(
                '//select[@class="nav-search-dropdown searchSelect"]/option[@selected="selected"]/text()')[
                0]
        except Exception:
            selected = ""

        if regularprice_i != "":
            regularprice = regularprice_i
            discountedprice = discountedprice_i
        else:
            regularprice = discountedprice_i
            discountedprice = ""

        if product_title != "":
            links_title = [product_title, product_url, avgrating, totalnumber, regularprice, discountedprice,
                           category, final_link, selected]
            columns = ["Product Title", "Product Url", "Avg Customer Reviews", "Total Number Customer Ratings",
                   "Regular Price", "Discounted Price", "Category", "Amazon's Choice",
                   "Catgeory fell under the Commision Structure"]
            # Create a zip object from two lists
            zipbObj = zip(columns, links_title)
            # Create a dictionary from zip object
            dictOfWords = dict(zipbObj)
            # print(registration_date)
            x = mycol_all.insert_one(dictOfWords)

    def post_scrape_callback(self, res):
        result = res.result()
        # print(res.target_url)
        if result and result.status_code == 200:
            self.parse_links(result.content, res.t_url)
            self.scrape_info(result.content, res.t_url)
        else:
            self.to_crawl.put(res.t_url)

    def scrape_page(self, url):
        try:
            payload = {'api_key': 'cbe9c3fc89b96f073c09308533257f13', 'url': url, 'country_code': 'us'}
            res = requests.get('http://api.scraperapi.com', params=payload)
            return res
        except requests.RequestException:
            return

    def run_scraper(self):
        while True:
            try:
                target_url = self.to_crawl.get(timeout=60)
                if target_url not in self.scraped_pages:
                    print("Scraping URL: {}".format(target_url))
                    self.scraped_pages.add(target_url)
                    job = self.pool.submit(self.scrape_page, target_url)
                    job.t_url = target_url
                    job.add_done_callback(self.post_scrape_callback)
            except Empty:
                return
            except Exception as e:
                print(e)
                continue


if __name__ == '__main__':
    s = MultiThreadScraper("https://www.amazon.com/s?i=luxury-beauty&bbn=20951535011&rh=n%3A3760911%2Cn%3A7627913011%2Cn%3A7627927011%2Cn%3A20951535011%2Cp_72%3A3-&dc&fst=as%3Aoff&qid=1589954424&refresh=1&ref=sr_pg_1")
    s.run_scraper()
