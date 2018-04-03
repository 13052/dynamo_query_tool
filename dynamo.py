#coding:utf-8
from datetime import datetime
import boto3
import logging
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO
    , format='%(asctime)s.%(msecs)03d %(name)-24s %(levelname)-8s %(message)s'
    , datefmt='%Y-%m-%dT%H:%M:%SZ%z'
)


# Simple select
def select(index_name, select_way='SPECIFIC_ATTRIBUTES'):

    # Initialize parameters for dynamo query
    IndexName = 'AttributeType-AttributeValue-index'
    ProjectionExpression='NationalID'
    KeyConditionExpression = 'AttributeType = :attribute_type AND AttributeValue = :attribute_value'
    FilterExpression = 'NationalID <> :national_id AND Context {0} :check_context'.format('=' if check_context == 'Company' else '<>')
    ExpressionAttributeValues = {':attribute_type': generate_dynamo_expression(attribute_type)
                                ,':attribute_value': generate_dynamo_expression(attribute_value)
                                ,':national_id': generate_dynamo_expression(national_id)
                                ,':check_context': generate_dynamo_expression(check_context)
                                }

    response = client.query(
        TableName=TABLE_NAME,
        IndexName=IndexName,
        Select='SPECIFIC_ATTRIBUTES',
        ProjectionExpression=ProjectionExpression, 
        ConsistentRead=False,
        ReturnConsumedCapacity='NONE',
        KeyConditionExpression=KeyConditionExpression,
        FilterExpression=FilterExpression,
        ExpressionAttributeValues=ExpressionAttributeValues
        )
    response_all['Items'].extend(response['Items'])

    while 'LastEvaluatedKey' in response: # Some query results return data larger than 1MB
        logging.info('  Querying from DynamoDB for exceeded data...................................')
        response = client.query(
            TableName=TABLE_NAME,
            IndexName=IndexName,
            Select='SPECIFIC_ATTRIBUTES',
            ProjectionExpression=ProjectionExpression, 
            ConsistentRead=False,
            ReturnConsumedCapacity='NONE',
            KeyConditionExpression=KeyConditionExpression,
            FilterExpression=FilterExpression,
            ExpressionAttributeValues=ExpressionAttributeValues,
            ExclusiveStartKey=response['LastEvaluatedKey']
            )
        logging.info('  Querying from DynamoDB for exceeded data done...................................')
        response_all['Items'].extend(response['Items'])
    if len(response_all[Items])>0:
        if ProjectionExpression:
            df = pd.DataFrame(columns=ProjectionExpression.split(','))
        else:
            df = pd.DataFrame(columns=[k for k in response['Items'][0]])
        index = 0
        for item in response_all['Items']:
            value = dict()
            for k,v in item.iteritems():
                value[k] = v['S']
            df.loc[index] = value
            index += 1

    else:
        return 0