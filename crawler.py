import urllib.parse
import time
import json
import requests
import csv
from os import listdir
from os.path import isfile, join


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


class PoloneixDataMunger():

    def __init__(self, input_directory, output_file_name):
        self.input_directory = input_directory
        self.output_file_name = output_file_name

    def write_data_to_csv(self):
        with open(self.output_file_name, 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(self.__json_keys())
            self.__write_to_csv(writer)

    def __write_to_csv(self, writer):
        records = 0
        timestamp = 1
        for file in self.__input_file_list():
            with open(file) as f:
                json_arr = json.load(f)
                for item in json_arr:
                    if self.__is_not_duplicate(item, timestamp):
                        writer.writerow(self.__json_to_arr(item))
                        records += 1
                    timestamp = item['date']
        print(f'{records} have been printed')

    def __json_to_arr(self, json_obj):
        return [json_obj[key] for key in self.__json_keys()]

    @staticmethod
    def __is_not_duplicate(json_obj, timestamp):
        return json_obj['date'] != timestamp

    @staticmethod
    def __json_keys():
        return ['date', 'high', 'low', 'open', 'close', 'volume', 'quoteVolume', 'weightedAverage']

    def __input_file_list(self):
        files = [join(self.input_directory, f) for f in listdir(self.input_directory) if isfile(join(self.input_directory, f))]
        return sorted(files)
