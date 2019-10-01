import boto3
import json
import sys
import time

textract=boto3.client('textract',aws_access_key_id='xxxxxxxxxxxxxxxxxxxx',aws_secret_access_key='xxxxxxxxxxxxxxxxxxxx',region_name='us-east-2')

s3=boto3.client('s3',aws_access_key_id='xxxxxxxxxxxxxxxxxxxx',aws_secret_access_key='xxxxxxxxxxxxxxxxxxxx',region_name='us-east-2')

sns=boto3.client('sns',aws_access_key_id='xxxxxxxxxxxxxxxxxxxx',aws_secret_access_key='xxxxxxxxxxxxxxxxxxxx',region_name='us-east-2')

#sqs=boto3.resource('sqs',aws_access_key_id='xxxxxxxxxxxxxxxxxxxx',aws_secret_access_key='xxxxxxxxxxxxxxxxxxxx',region_name='us-east-2')

##############################################################################################
def lambda_handler(event,context):

    send_message_sns_lambda=sns.subscribe(
        TopicArn='arn:aws:sns:us-east-2:671511125968:sns_topic_name',
        Protocol='lambda',
        Endpoint='arn:aws:lambda:us-east-2:671511125968:function:lambda_name',
    )
    ###########################################################################################
    response = textract.start_document_analysis(
        DocumentLocation={
            'S3Object': {
                'Bucket': 'bucket_name',
                'Name': 'handwritten.pdf'
            }
        },
        FeatureTypes=[
            'FORMS'
        ],
        NotificationChannel={
            'SNSTopicArn': 'arn:aws:sns:us-east-2:671511125968:sns_topic_name',
            'RoleArn': 'arn:aws:iam::671511125968:role/role_name'
        }
    )
    #####################################################################################################
    ##########################################################################################################
    x=sns.publish(
            TopicArn='arn:aws:sns:us-east-2:671511125968:sns_topic_name', 
            Message=json.dumps({'default': json.dumps(response)}),
            MessageStructure='json')

    #queue = sqs.get_queue_by_name(QueueName='h-sample-queue')


    #messages = queue.receive_messages(QueueUrl='https://sqs.us-east-2.amazonaws.com/h-sample-queue',MessageAttributeNames=['ALL'],MaxNumberOfMessages=10)

    #response2 = textract.get_document_analysis(JobId=response['JobId'],MaxResults=1000)


    print("response: ",response)
    print("sns publish:", x)
    return response

