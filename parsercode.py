import json
import sys
import time
import boto3


#Credentials
bucket_name = 'bucket_name'
document_name = 'handwritten.pdf'
aws_access_key_id = "xxxxxxxxxxxxxxxxxxxx"
aws_secret_access_key = "xxxxxxxxxxxxxxxxxxxx"
region_name = "us-east-2"


#AWS service credentials
topic_name = "arn:aws:sns:us-east-2:671511125968:sns_topic_name"
lambda_name = "arn:aws:lambda:us-east-2:671511125968:function:lambda_name"
role_name = "arn:aws:iam::671511125968:role/role_name"


#Initializing AWS services
textract = boto3.client('textract',aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key, region_name = region_name)

s3 = boto3.client('s3', aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key, region_name = region_name)

sns = boto3.client('sns', aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key, region_name = region_name)

#sqs = boto3.resource('sqs, aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key, region_name = region_name)

##############################################################################################

def lambda_handler(event,context):
    
    
    """  This lambda function is automatically triggred when a file is dropped in S3 bucket (Parameter : S3 bucket event)  """
    
    
    send_message_sns_lambda = sns.subscribe(
                                    TopicArn = topic_name,
                                    Protocol='lambda',
                                    Endpoint = lambda_name,
                              )
    
    ###########################################################################################

    # Asyncronous call to Textract API
    
    textract_response = textract.start_document_analysis(
        
                        DocumentLocation={
                            
                                    'S3Object': {
                                    'Bucket': bucket_name,
                                    'Name': document_name
                                        
                                    }
                        },
        
                        FeatureTypes=[
                                    'FORMS'
                                    ],
        
                        NotificationChannel={
                                            'SNSTopicArn': topic_name,
                                            'RoleArn': role_name
                                            }
        
                        )
    
    #####################################################################################################
    #####################################################################################################
    
    #Sending Response to SNS topic
    
     sns_response = sns.publish(
 
                            TopicArn = topic_name, 
                            Message = json.dumps({'default': json.dumps(textract_response)}),
                            MessageStructure = 'json'
         
                    )

        
    #queue = sqs.get_queue_by_name(QueueName='h-sample-queue')

    #messages = queue.receive_messages(QueueUrl='https://sqs.us-east-2.amazonaws.com/h-sample-queue',MessageAttributeNames=['ALL'],MaxNumberOfMessages=10)

    #response2 = textract.get_document_analysis(JobId=response['JobId'],MaxResults=1000)


    print("response: ", textract_response)
    print("sns publish:", sns_response)
    
    
    return textract_response

