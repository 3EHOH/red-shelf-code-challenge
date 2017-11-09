#!/usr/bin/python

import boto3
import sys

def main():

    ec2 = boto3.resource('ec2')

    name_tag = sys.argv[1]

    instances = ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}, {'Name': 'tag:Name', 'Values': [name_tag]}])

    print(list(instances)[0].private_ip_address)


if __name__ == '__main__':
    main()
