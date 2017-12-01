import luigi
import os
from config import PathConfig, BucketConfig
import csv
import json
from steps.preflightcheck import PreflightCheck
from steps.readdata import ReadData

STEP = 'readbucketdata'


# class ReadBucketData(luigi.Task):
#
#     datafile = luigi.Parameter(default=STEP)
#
#     @staticmethod
#     def requires():
#         return [PreflightCheck()]
#
#     def output(self):
#         return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))
#
#     def run(self):
#         purchase_data_file = BucketConfig().bucket_data
#         purchase_keys = BucketConfig().bucket_keys.split(",")
#         purchase_data = []
#
#         with open(purchase_data_file) as csvfile:
#             purchase_reader = csv.reader(csvfile)
#             for row in purchase_reader:
#                 purchase_record = {}
#                 for i in range(len(purchase_keys)):
#                     purchase_record[purchase_keys[i]] = row[i]
#                 purchase_data.append(purchase_record)
#
#         with self.output().open('w') as out_file:
#             out_file.write(json.dumps(purchase_data))



class ReadBucketData(ReadData):

    datafile = luigi.Parameter(default=STEP)

    def __init__(self):
        # self.task_id = ReadData.task_id
        # super(ReadBucketData, self).__init__('purchase_buckets.csv', BucketConfig().bucket_keys.split(","))

        ReadData.__init__(self, BucketConfig().purchase_buckets, BucketConfig().bucket_keys.split(","))
        self.csv_file = BucketConfig().purchase_buckets
        self.csv_file_keys = BucketConfig().bucket_keys.split(",")
        # self.csv_file = csv_file
        # self.csv_file_keys = csv_file_keys

    # @staticmethod
    # def requires():
    #     return [PreflightCheck()]
    #
    # def output(self):
    #     return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))
    #
    # def run(self):
    #     purchase_buckets_file = self.csv_file
    #     bucket_keys = self.csv_file_keys
    #     bucket_data = []
    #
    #     with open(purchase_buckets_file) as csvfile:
    #         bucket_reader = csv.reader(csvfile)
    #         for row in bucket_reader:
    #             bucket_record = {}
    #             for i in range(len(bucket_keys)):
    #                 bucket_record[bucket_keys[i]] = row[i]
    #             bucket_data.append(bucket_record)
    #
    #     with self.output().open('w') as out_file:
    #         out_file.write(json.dumps(bucket_data))


    # def __init__(self):
    #     #ReadData.__init__(self, BucketConfig(), BucketConfig().bucket_keys.split(","))
    #     #super(ReadBucketData, self).__init__(BucketConfig(), BucketConfig().bucket_keys.split(","))
    #     super().__init__(BucketConfig(), BucketConfig().bucket_keys.split(","))

    # @staticmethod
    # def requires():
    #     return [PreflightCheck()]
    #
    # def output(self):
    #     return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    # def run(self):
    #     purchase_buckets_file = BucketConfig().purchase_buckets
    #     bucket_keys = BucketConfig().bucket_keys.split(",")
        #bucket_data = []

        # ReadData.run(self, purchase_buckets_file, bucket_keys)
        #
        # with open(purchase_buckets_file) as csvfile:
        #     bucket_reader = csv.reader(csvfile)
        #     for row in bucket_reader:
        #         bucket_record = {}
        #         for i in range(len(bucket_keys)):
        #             bucket_record[bucket_keys[i]] = row[i]
        #         bucket_data.append(bucket_record)
        #
        # with self.output().open('w') as out_file:
        #     out_file.write(json.dumps(bucket_data))


if __name__ == "__main__":
    luigi.run([
        'ReadBucketData',
        '--workers', '1',
        '--local-scheduler'])