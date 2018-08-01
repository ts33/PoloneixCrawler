import csv
from os import listdir
from os.path import isfile, join
import json


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