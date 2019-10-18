# AWS-Textract-Comprehend-Medical
This application is a demonstration of AWS OCR service(Textract and Comprehend Medical) in combination with AWS serverless component(Lambda).

<h2>What does this application do?</h2>
To keep this simple, this application extracts the information from the documents(PDF format) and stores it in a Redshift database(structured format).
While the application is serverless, it is completely asynchronous (can be used on a document with multiple pages).

To get into the detail of the application, it is important that we know what does the AWS Comprehend Medical do as the application is little tailored to meet the medical needs. 

AWS Comprehend Medical is a service similar to Textract(OCR) but picks up the medical specifications or medical related terms from a text document for example PHI(patient health information),Medication taken, Dosage of the medication and many more.

More information can be found related to Textract and Comprehend Medical in the documentation.

[Textract](https://aws.amazon.com/textract/)
<br>
[Comprehend Medical](https://aws.amazon.com/comprehend/medical/)

<h2>Working</h2>

This application is completely dependent on asynchronous calls made to the Lambda functions which are the crux of this application.
Lambda functions are the serverless components of AWS which can be triggered in different ways such as SNS topic listening to it(Pub/Sub model), SQS(Messaging Queues), S3 Bucket(sends notifications if there is a change in the bucket) and the most widely used AWS CloudWatchEvents(event tracker,logger).
<br>(More information on lambda can be found here: [Lambda functions](https://aws.amazon.com/lambda/?nc2=h_ql_prod_fs_lbd))

There are three Lambda functions(and associated scripts) in this application.

<b>Lambda1: parsercode.py</b><br>
Makes an asynchronous call to the Textract API as soon as a document is dropped into s3 bucket, which starts the text detection. As soon as the detection is complete, sends a notification to cloudwatch which triggres the next lambda function.

<b>Lambda2: gcs-parser.py</b><br>
Gets triggered as soon as it receives notification from cloud watch. Receives the detected text in the from of an event as it is subscribed to cloudwatch. This event is given as an input to Comprehend Medical. Comprehend Medical picks up all the medical terms(with the confidence score). This script segregates all the fields and drops them into their particular columns(pandas dataframes) and simulataneously converts these dataframes into csv files and downloads them into an s3 bucket.

<b>Lambda3: s3_to_redshift.py</b><br>
This is an optional task(as we already have the csv files). This function(or script) is custom written for this application as Redshift is little particular with the tables defined in it. The tables need to predeclared with the columns before any data is loaded into it (and hence different loading functions). But Redshift is very helpful for scaling purposes which could be advantageous in our case depending on the size of data.

To conclude this, if these scripts,triggers are in place, All we need to do is load the document into s3 bucket and we can see the associated csv files in s3 bucket and tables in Redshift database.

<h2>Notes</h2>
It is very important that we need to give appropriate IAM Roles(permissions) to the lambda functions for this process and consider the confidence scores accordingly.
<br><br>
<b>AWS Services Used:</b>

[Lambda functions](https://aws.amazon.com/lambda/?nc2=h_ql_prod_fs_lbd)
<br>
[S3](https://aws.amazon.com/s3/?nc2=h_ql_prod_fs_s3)
<br>
[SNS](https://aws.amazon.com/sns/?whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc)
<br>
[Redshift](https://aws.amazon.com/redshift/#)
<br>
[IAM](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html)
<br>
[Textract](https://aws.amazon.com/textract/)
<br>
[Comprehend Medical](https://aws.amazon.com/comprehend/medical/)



