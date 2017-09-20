import time
from crawler import PoloneixCrawler, PoloneixDataMunger

ETH_START = 1441065600 # SEPT 1, 2015
END = int(time.time())

TEST_START = 1441065600
TEST_END   = 1441365600


CURRENCY_PAIRS = {
    'usd_eth': 'USDT_ETH',
    'usd_btc': 'USDT_BTC'
}

INTERVAL_TYPES = {
    '5 Minutes': 300
}

file_dir = "/home/timothy/Projects/PoloneixCrawler/data/"
base_dir = "/home/timothy/Projects/PoloneixCrawler/"


def crawl(currency_pair, interval):
    output_dir = file_dir + currency_pair + '/'
    crawler = PoloneixCrawler(currency_pair, interval, output_dir)
    crawler.iterate_and_crawl(ETH_START, END)


def munge(currency_pair):
    output_dir = file_dir + currency_pair + '/'
    csv_file = base_dir + currency_pair + '.csv'
    munger = PoloneixDataMunger(output_dir, csv_file)
    munger.write_data_to_csv()

# crawl(CURRENCY_PAIRS['usd_btc'], INTERVAL_TYPES['5 Minutes'])
# munge(CURRENCY_PAIRS['usd_btc'])
munge(CURRENCY_PAIRS['usd_eth'])
