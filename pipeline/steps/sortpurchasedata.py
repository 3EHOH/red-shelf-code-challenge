import luigi
import os
from config import PathConfig
import json
from steps.createoutputbuckets import CreateOutputBuckets

STEP = 'sortpurchasedata'


class SortPurchaseData(luigi.Task):

    datafile = luigi.Parameter(default=STEP)

    @staticmethod
    def sort_data(self, purchase_data, bucket_data, output_buckets):

        no_duplicates = []

        unique_records = set()

        for record in purchase_data:

            record_values = ','.join(record.values())
            record_publisher_lc = record['publisher'].lower()
            record_duration_lc = record['duration'].lower()

            if next((bucket for bucket in bucket_data
                    if bucket['publisher'].lower() == record_publisher_lc
                    and record['price'] == bucket['price']
                    and record_duration_lc == bucket['duration'].lower()), None) is not None:

                bucket_name_match = self.mock_bucket_name(record['publisher'], record['price'], record['duration'])

                matched_bucket = next((bucket for bucket in output_buckets if bucket['bucket'].lower() == bucket_name_match.lower()), None)
                if matched_bucket is not None:
                    matched_bucket['purchases'].append(record_values)

            elif next((bucket for bucket in bucket_data
                       if bucket['publisher'].lower() == record_publisher_lc
                       and record['price'] == bucket['price']), None) is not None:

                bucket_name_match = self.mock_bucket_name(record['publisher'], record['price'])
                print(bucket_name_match)

                matched_bucket = next((bucket for bucket in output_buckets if bucket['bucket'].lower() == bucket_name_match.lower()), None)
                if matched_bucket is not None:
                    matched_bucket['purchases'].append(record_values)

            elif next((bucket for bucket in bucket_data
                       if record['price'] == bucket['price']
                       and record_duration_lc == bucket['duration'].lower()), None) is not None:

                bucket_name_match = self.mock_bucket_name(None, record['price'], record['duration'])
                print(bucket_name_match)

                matched_bucket = next((bucket for bucket in output_buckets if bucket['bucket'].lower() == bucket_name_match.lower()), None)
                if matched_bucket is not None:
                    matched_bucket['purchases'].append(record_values)

            elif next((bucket for bucket in bucket_data
                       if bucket['publisher'].lower() == record_publisher_lc), None) is not None:

                bucket_name_match = self.mock_bucket_name(record['publisher'])

                matched_bucket = next((bucket for bucket in output_buckets if bucket['bucket'].lower() == bucket_name_match.lower()), None)
                if matched_bucket is not None:
                    matched_bucket['purchases'].append(record_values)

            elif next((bucket for bucket in bucket_data
                       if record['price'] == bucket['price']), None) is not None:

                bucket_name_match = self.mock_bucket_name(None, record['price'])

                matched_bucket = next((bucket for bucket in output_buckets if bucket['bucket'].lower() == bucket_name_match.lower()), None)
                if matched_bucket is not None:
                    matched_bucket['purchases'].append(record_values)

            elif next((bucket for bucket in bucket_data
                     if record_duration_lc == bucket['duration'].lower()), None) is not None:

                bucket_name_match = self.mock_bucket_name(None, None, record['duration'])

                matched_bucket = next((bucket for bucket in output_buckets if bucket['bucket'].lower() == bucket_name_match.lower()), None)
                if matched_bucket is not None:
                    matched_bucket['purchases'].append(record_values)

            else:
                bucket_name_match = self.mock_bucket_name()

                matched_bucket = next((bucket for bucket in output_buckets if bucket['bucket'].lower() == bucket_name_match.lower()), None)
                if matched_bucket is not None:
                    matched_bucket['purchases'].append(record_values)



    @staticmethod
    def find_and_assign(compare, output_buckets, record_values, unique_buckets_and_purchases):
        for i, dic in enumerate(output_buckets):
            if dic['bucket'].lower() == compare and record_values not in dic['purchases'] and not \
                    any(d['bucket_name'].lower() == compare and d['purchase'] == record_values for d in unique_buckets_and_purchases):
                dic['purchases'].append(record_values)
                unique_buckets_and_purchases.append({"bucket_name": compare, "purchase": record_values})

    @staticmethod
    def mock_bucket_name(publisher=None, price=None, duration=None):

        if publisher is None:
            bucket_name = "*"
        else:
            bucket_name = publisher

        bucket_name = bucket_name + ","

        if price is None:
            bucket_name = bucket_name + "*"
        else:
            bucket_name = bucket_name + price

        bucket_name = bucket_name + ","

        if duration is None:
            bucket_name = bucket_name + "*"
        else:
            bucket_name = bucket_name + duration

        return bucket_name

    @staticmethod
    def requires():
        return [CreateOutputBuckets()]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    @staticmethod
    def read_files(pathname):
        path_to_file = os.path.join(PathConfig().target_path, pathname)

        with open(path_to_file, 'r') as f:
            file_output = json.load(f)

        return file_output

    def run(self):

        output_buckets = self.read_files("createoutputbuckets")
        purchase_data = self.read_files("readpurchasedata")
        bucket_data = self.read_files("readbucketdata")

        self.sort_data(self, purchase_data, bucket_data, output_buckets)

        with self.output().open('w') as out_file:
            out_file.write(json.dumps(output_buckets))


if __name__ == "__main__":
    luigi.run([
        'SortPurchaseData',
        '--workers', '1',
        '--local-scheduler'])














# import luigi
# import os
# from config import PathConfig
# import json
# from steps.createoutputbuckets import CreateOutputBuckets
#
# STEP = 'sortpurchasedata'
#
#
# class SortPurchaseData(luigi.Task):
#
#     datafile = luigi.Parameter(default=STEP)
#
#     @staticmethod
#     def sort_data(self, purchase_data, bucket_data, output_buckets):
#
#         no_duplicates = []
#
#         unique_records = set()
#
#         for record in purchase_data:
#
#             record_values = ','.join(record.values())
#
#             if not record['order_id'] in unique_records:
#
#                 for bucket in bucket_data:
#
#                     if record['publisher'].lower() == bucket['publisher'].lower() and record['price'] == bucket['price'] and \
#                                     record['duration'].lower() == bucket['duration'].lower():
#
#                         potential_bucket_name_match = self.mock_bucket_name(record['publisher'].lower(), record['price'], record['duration'].lower())
#                         self.find_and_assign(potential_bucket_name_match, output_buckets, record_values, no_duplicates)
#
#                     elif record['publisher'].lower() == bucket['publisher'].lower() and record['price'] == bucket['price']:
#
#                         potential_bucket_name_match = self.mock_bucket_name(record['publisher'].lower(), record['price'])
#                         self.find_and_assign(potential_bucket_name_match, output_buckets, record_values, no_duplicates)
#
#                     elif record['price'].lower() == bucket['price'].lower() and record['duration'] == bucket['duration']:
#
#                         # compare = "*" + "," + record['price'] + "," + record['duration']
#                         potential_bucket_name_match = self.mock_bucket_name(None, record['price'], record['duration'].lower())
#                         self.find_and_assign(potential_bucket_name_match, output_buckets, record_values, no_duplicates)
#
#                     elif record['publisher'].lower() == bucket['publisher'].lower():
#
#                         # compare = record['publisher'].lower() + "," + "*" + "," + "*"
#                         potential_bucket_name_match = self.mock_bucket_name(record['publisher'].lower())
#                         self.find_and_assign(potential_bucket_name_match, output_buckets, record_values, no_duplicates)
#
#                     elif record['price'].lower() == bucket['price'].lower():
#
#                         # compare = "*" + "," + record['price'] + "," + "*"
#                         potential_bucket_name_match = self.mock_bucket_name(record['price'])
#                         self.find_and_assign(potential_bucket_name_match, output_buckets, record_values, no_duplicates)
#
#                     elif record['duration'].lower() == bucket['duration'].lower():
#
#                         # compare = record['publisher'].lower() + "," + "*" + "," + record['duration']
#                         potential_bucket_name_match = self.mock_bucket_name(record['duration'].lower())
#                         self.find_and_assign(potential_bucket_name_match, output_buckets, record_values, no_duplicates)
#
#                     else:
#
#                         potential_bucket_name_match = self.mock_bucket_name()
#                         self.find_and_assign(potential_bucket_name_match, output_buckets, record_values, no_duplicates)
#
#                     unique_records.add(record['order_id'])
#
#     @staticmethod
#     def find_and_assign(compare, output_buckets, record_values, unique_buckets_and_purchases):
#         for i, dic in enumerate(output_buckets):
#             if dic['bucket'].lower() == compare and record_values not in dic['purchases'] and not \
#                     any(d['bucket_name'].lower() == compare and d['purchase'] == record_values for d in unique_buckets_and_purchases):
#                 dic['purchases'].append(record_values)
#                 unique_buckets_and_purchases.append({"bucket_name": compare, "purchase": record_values})
#
#     @staticmethod
#     def mock_bucket_name(publisher=None, price=None, duration=None):
#
#         if publisher is None:
#             bucket_name = "*"
#         else:
#             bucket_name = publisher
#
#         bucket_name = bucket_name + ","
#
#         if price is None:
#             bucket_name = bucket_name + "*"
#         else:
#             bucket_name = bucket_name + price
#
#         bucket_name = bucket_name + ","
#
#         if duration is None:
#             bucket_name = bucket_name + "*"
#         else:
#             bucket_name = bucket_name + duration
#
#         return bucket_name
#
#     @staticmethod
#     def requires():
#         return [CreateOutputBuckets()]
#
#     def output(self):
#         return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))
#
#     @staticmethod
#     def read_files(pathname):
#         path_to_file = os.path.join(PathConfig().target_path, pathname)
#
#         with open(path_to_file, 'r') as f:
#             file_output = json.load(f)
#
#         return file_output
#
#     def run(self):
#
#         output_buckets = self.read_files("createoutputbuckets")
#         purchase_data = self.read_files("readpurchasedata")
#         bucket_data = self.read_files("readbucketdata")
#
#         self.sort_data(self, purchase_data, bucket_data, output_buckets)
#
#         with self.output().open('w') as out_file:
#             out_file.write(json.dumps(output_buckets))
#
#
# if __name__ == "__main__":
#     luigi.run([
#         'SortPurchaseData',
#         '--workers', '1',
#         '--local-scheduler'])
