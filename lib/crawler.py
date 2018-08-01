import urllib.parse
import time
import json
import requests


class PoloneixCrawler:

    BASE_URL = 'https://poloniex.com/public?'

    def __init__(self, currency_pair, interval, directory, num_records=1000):
        self.currency_pair = currency_pair
        self.interval = interval
        self.directory = directory
        self.num_records = num_records

    def iterate_and_crawl(self, start_time, end_time):
        timestamp_step = self.num_records * self.interval
        start = start_time
        end = start + timestamp_step
        counter = 1

        while start < end_time:
            self.__request_and_write(start, end, counter)

            start += timestamp_step
            end += timestamp_step
            counter += 1
            time.sleep(1)

    def __request_and_write(self, start, end, counter):
        url = self.__create_url(start, end)
        response = self.__make_request(url)
        print(f'Making {counter} request, from {start} to {end}. {len(response)} records crawled.')
        self.__write_to_file(f'{start}_{end}', response)

    @staticmethod
    def __make_request(url):
        try:
            r = requests.get(url)
            return r.json()
        except:
            print(f'Unable to send request successfully. Url is {url}')

    def __write_to_file(self, file_name, json_contents):
        with open(self.directory + file_name, 'w') as f:
            json.dump(json_contents, f)

    def __create_url(self, start, end):
        params = self.__create_parameters(start, end)
        return self.BASE_URL + urllib.parse.urlencode(params)

    def __create_parameters(self, start, end):
        return {
            'command': 'returnChartData',
            'currencyPair': self.currency_pair,
            'start': start,
            'end': end,
            'period': self.interval
        }
