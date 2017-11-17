import requests
from slackclient import SlackClient
from config import SlackChannelName
import os
import boto3
import datetime

class LogUtils(object):


    
    def log_start(STEP):
        slack_token = SlackChannelName().SLACK_API_TOKEN
        response = requests.get('http://169.254.169.254/latest/meta-data/instance-id')
        id = response.text
        ec2 = boto3.resource('ec2')
        ec2instance = ec2.Instance(id)
        instancename = ''
        for tags in ec2instance.tags:
            if tags["Key"] == 'Name':
                instancename = tags["Value"]
        sc = SlackClient(slack_token)
        c_name = SlackChannelName().CHANNEL_NAME
        now = datetime.datetime.now()
        print (STEP)
        message = instancename+" with Instance_ID->("+id+") has started running "+STEP+" at "+now.strftime('%Y/%m/%d %H:%M:%S')
        sc.api_call("chat.postMessage",channel=c_name,text=message)

    
    def log_stop(STEP):
        slack_token = SlackChannelName().SLACK_API_TOKEN
        #fetch instance_id
        response = requests.get('http://169.254.169.254/latest/meta-data/instance-id')
        id = response.text
        #using instance tags fetch instance name
        ec2 = boto3.resource('ec2')
        ec2instance = ec2.Instance(id)
        instancename = ''
        for tags in ec2instance.tags:
            if tags["Key"] == 'Name':
                instancename = tags["Value"]
        sc = SlackClient(slack_token)
        c_name = SlackChannelName().CHANNEL_NAME
        now = datetime.datetime.now()
        message = instancename+" with Instance_ID->("+id+") has finished running "+STEP+" on "+now.strftime('%Y/%m/%d %H:%M:%S')
        sc.api_call("chat.postMessage",channel=c_name,text=message)

    
    def log_step_error(hit):
        #set token value
        slack_token = os.environ["SLACK_API_TOKEN"]
        sc = SlackClient(slack_token)
        c_name = SlackChannelName().channel_name
        #sc.api_call("chat.postMessage",channel=c_name,text=hit)
        sc.api_call("chat.postMessage",channel=c_name,text=instancename+" encountered error in PreflightCheck!!! PLEASE CHECK LOG FILE!")
        #sc.api_call("files.upload",channel=c_name,filename="")o
