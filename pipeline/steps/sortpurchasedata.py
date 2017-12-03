import luigi
import os
from config import PathConfig
import json
from steps.createoutputbuckets import CreateOutputBuckets
from steps.readpurchasedata import ReadPurchaseData
from steps.readpurchasedata import ReadBucketData
from steps.readfile import ReadFile

STEP = 'sortpurchasedata'
WILDCARD = '*'


class SortPurchaseData(luigi.Task):
    datafile = luigi.Parameter(default=STEP)

    @staticmethod
    def sort_data(self, purchase_data, bucket_data, output_buckets):

        for record in purchase_data:

            record_values = ','.join(record.values())
            record_publisher_lc = record['publisher'].lower()
            record_duration_lc = record['duration'].lower()
            record_price = record['price']

            # match all fields

            if next((bucket for bucket in bucket_data
                     if  bucket['publisher'].lower() == record_publisher_lc
                     and bucket['price']             == record_price
                     and bucket['duration'].lower()  == record_duration_lc), None) is not None:

                bucket_name_match = self.concat_bucket_name(record['publisher'], record['price'], record['duration'])

                matched_bucket = next(
                    (bucket for bucket in output_buckets if bucket['bucket'].lower() == bucket_name_match.lower()),
                    None)
                if matched_bucket is not None:
                    matched_bucket['purchases'].append(record_values)

            # match publisher and duration

            elif next((bucket for bucket in bucket_data
                       if  bucket['publisher'].lower() == record_publisher_lc
                       and bucket['price']             == WILDCARD
                       and bucket['duration'].lower()  == record_duration_lc), None) is not None:

                bucket_name_match = self.concat_bucket_name(record['publisher'], record['price'])
                matched_bucket = next(
                    (bucket for bucket in output_buckets if bucket['bucket'].lower() == bucket_name_match.lower()),
                    None)
                if matched_bucket is not None:
                    matched_bucket['purchases'].append(record_values)

            # match publisher and price

            elif next((bucket for bucket in bucket_data
                       if  bucket['publisher'].lower() == record_publisher_lc
                       and bucket['price']             == record_price
                       and bucket['duration'].lower()  == WILDCARD), None) is not None:

                bucket_name_match = self.concat_bucket_name(record['publisher'], record['price'])
                matched_bucket = next(
                    (bucket for bucket in output_buckets if bucket['bucket'].lower() == bucket_name_match.lower()),
                    None)
                if matched_bucket is not None:
                    matched_bucket['purchases'].append(record_values)

            # match price and duration

            elif next((bucket for bucket in bucket_data
                       if  bucket['publisher'].lower() == WILDCARD
                       and bucket['price']             == record_price
                       and bucket['duration'].lower()  == record_duration_lc), None) is not None:

                bucket_name_match = self.concat_bucket_name(None, record['price'], record['duration'])
                matched_bucket = next(
                    (bucket for bucket in output_buckets if bucket['bucket'].lower() == bucket_name_match.lower()),
                    None)

                if matched_bucket is not None:
                    matched_bucket['purchases'].append(record_values)

            # match publisher

            elif next((bucket for bucket in bucket_data
                       if  bucket['publisher'].lower() == record_publisher_lc
                       and bucket['price']             == WILDCARD
                       and bucket['duration'].lower()  == WILDCARD), None) is not None:

                bucket_name_match = self.concat_bucket_name(record['publisher'])

                matched_bucket = next(
                    (bucket for bucket in output_buckets if bucket['bucket'].lower() == bucket_name_match.lower()),
                    None)
                if matched_bucket is not None:
                    matched_bucket['purchases'].append(record_values)

            # match duration

            elif next((bucket for bucket in bucket_data
                       if  bucket['publisher'].lower() == WILDCARD
                       and bucket['price']             == WILDCARD
                       and bucket['duration'].lower()  == record_duration_lc), None) is not None:

                bucket_name_match = self.concat_bucket_name(None, None, record['duration'])

                matched_bucket = next(
                    (bucket for bucket in output_buckets if bucket['bucket'].lower() == bucket_name_match.lower()),
                    None)
                if matched_bucket is not None:
                    matched_bucket['purchases'].append(record_values)

            # match price

            elif next((bucket for bucket in bucket_data
                       if  bucket['publisher'].lower() == WILDCARD
                       and bucket['price']             == record_price
                       and bucket['duration'].lower()  == WILDCARD), None) is not None:

                bucket_name_match = self.concat_bucket_name(None, record['price'])

                matched_bucket = next(
                    (bucket for bucket in output_buckets if bucket['bucket'].lower() == bucket_name_match.lower()),
                    None)
                if matched_bucket is not None:
                    matched_bucket['purchases'].append(record_values)

            # match none

            else:

                bucket_name_match = self.concat_bucket_name()

                bucket = next(bucket for bucket in output_buckets if bucket['bucket'] == bucket_name_match)
                bucket['purchases'].append(record_values)

    @staticmethod
    def find_and_assign(compare, output_buckets, record_values, unique_buckets_and_purchases):
        for i, dic in enumerate(output_buckets):
            if dic['bucket'].lower() == compare and record_values not in dic['purchases'] and not \
                    any(d['bucket_name'].lower() == compare and d['purchase'] == record_values for d in
                        unique_buckets_and_purchases):
                dic['purchases'].append(record_values)
                unique_buckets_and_purchases.append({"bucket_name": compare, "purchase": record_values})

    @staticmethod
    def concat_bucket_name(publisher=None, price=None, duration=None):

        if publisher is None:
            bucket_name = WILDCARD
        else:
            bucket_name = publisher

        bucket_name = bucket_name + ","

        if price is None:
            bucket_name = bucket_name + WILDCARD
        else:
            bucket_name = bucket_name + price

        bucket_name = bucket_name + ","

        if duration is None:
            bucket_name = bucket_name + WILDCARD
        else:
            bucket_name = bucket_name + duration

        return bucket_name

    @staticmethod
    def requires():
        return [CreateOutputBuckets(), ReadPurchaseData()]

    def output(self):
        return luigi.LocalTarget(os.path.join(PathConfig().target_path, self.datafile))

    def run(self):

        output_buckets = ReadFile.read_file(CreateOutputBuckets().datafile)
        purchase_data = ReadFile.read_file(ReadPurchaseData().datafile)
        bucket_data = ReadFile.read_file(ReadBucketData().datafile)

        self.sort_data(self, purchase_data, bucket_data, output_buckets)

        with self.output().open('w') as out_file:
            out_file.write(json.dumps(output_buckets))


if __name__ == "__main__":
    luigi.run()
