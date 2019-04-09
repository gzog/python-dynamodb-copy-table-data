#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
import argparse
import os
import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr

parser = argparse.ArgumentParser(description='Copy data from src DynamoDB table to dest')
parser.add_argument('-src', help='source table name', required=True)
parser.add_argument('-dest', help='destination table name', required=True)

args = parser.parse_args()


def chunks(l, n):
    """Yield successive n-sized chunks from l."""

    for i in range(0, len(l), n):
        yield l[i:i + n]


REGION_NAME = os.environ.get('AWS_DEFAULT_REGION', 'eu-central-1')
SOURCE_TABLE_NAME = args.src
DESTINATION_TABLE_NAME = args.dest

client = boto3.client('dynamodb', region_name=REGION_NAME)

last_evaluated_key = True
chunk_idx = 0
total_items = 0

while last_evaluated_key:
    if type(last_evaluated_key) == bool:
        response = client.scan(TableName=SOURCE_TABLE_NAME)
    else:
        response = client.scan(TableName=SOURCE_TABLE_NAME,
                               ExclusiveStartKey=last_evaluated_key)

    for chunk in chunks(response['Items'], 25):
        total_items += len(chunk)
        chunk_idx += 1

        batch_write = {DESTINATION_TABLE_NAME: []}

        for item in chunk:
            batch_write_item = {'PutRequest': {'Item': {}}}

            for (k, v) in item.items():
                batch_write_item['PutRequest']['Item'][k] = v

            batch_write[DESTINATION_TABLE_NAME].append(batch_write_item)
        client.batch_write_item(RequestItems=batch_write)

        print('Written chunk: {}'.format(chunk_idx))
    last_evaluated_key = response.get('LastEvaluatedKey', None)
    print('Last Evaluated Key: {}'.format(last_evaluated_key))

print('Total items copied: {}'.format(total_items))