#!/usr/bin/env python3

import csv
import json


def main():

    purchase_keys = ['order_id', 'isbn', 'publisher', 'school', 'price', 'duration', 'order_datetime']

    bucket_keys = ['publisher', 'price', 'duration']

    purchase_data = []

    bucket_data = []

    with open('purchase_data.csv') as csvfile:
            purchase_reader = csv.reader(csvfile)
            for row in purchase_reader:
                purchase_record = {}
                for i in range(len(purchase_keys)):
                    purchase_record[purchase_keys[i]] = row[i]
                purchase_data.append(purchase_record)

    with open('purchase_buckets.csv') as csvfile:
        bucket_reader = csv.reader(csvfile)
        for row in bucket_reader:
            bucket_record = {}
            for i in range(len(bucket_keys)):
                bucket_record[bucket_keys[i]] = row[i]
            bucket_data.append(bucket_record)

    final_buckets = []

    for bucket in bucket_data:
        bucket_name = ','.join(bucket.values())
        final_buckets.append({"bucket": bucket_name, "purchases": []})

    for record in purchase_data:

        record_values = ','.join(record.values())

        for bucket in bucket_data:

            if record['publisher'].lower() == bucket['publisher'].lower() and record['price'] == bucket['price'] and \
                            record['duration'].lower() == bucket['duration'].lower():

                compare = record_values
                find_and_assign(compare, final_buckets, record_values)

            elif record['publisher'].lower() == bucket['publisher'].lower() and record['price'] == bucket['price']:

                compare = record['publisher'].lower() + "," + record['price'] + "," + "*"
                find_and_assign(compare, final_buckets, record_values)

            elif record['price'].lower() == bucket['price'].lower() and record['duration'] == bucket['duration']:

                compare = "*" + "," + record['price'] + "," + record['duration']
                find_and_assign(compare, final_buckets, record_values)

            elif record['publisher'].lower() == bucket['publisher'].lower():

                compare = record['publisher'].lower() + "," + "*" + "," + "*"
                find_and_assign(compare, final_buckets, record_values)

            elif record['price'].lower() == bucket['price'].lower():

                compare = "*" + "," + record['price'] + "," + "*"
                find_and_assign(compare, final_buckets, record_values)

            elif record['duration'].lower() == bucket['duration'].lower():

                compare = record['publisher'].lower() + "," + "*" + "," + record['duration']
                find_and_assign(compare, final_buckets, record_values)

            else:
                compare = "*,*,*"
                find_and_assign(compare, final_buckets, record_values)

    # for d in final_buckets):
    #     for purchases in d['purchases']:
    #
    #         d['purchase'] = set(purchase)

    #either use a set and convert to lists here or use list insert if not exists

    json_data = json.dumps(final_buckets)
    print(json_data)


def find_and_assign(compare, final_buckets, record_values):
    for i, dic in enumerate(final_buckets):
        if dic['bucket'].lower() == compare and record_values not in dic['purchases']:
            dic['purchases'].append(record_values)


if __name__ == "__main__":
    main()