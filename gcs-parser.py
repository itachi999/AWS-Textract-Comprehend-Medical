import boto3
import json
import pandas as pd
import io
from io import StringIO
from collections import defaultdict


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

s3resource = boto3.resource('s3', aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key, region_name = region_name)

cm = boto3.client('comprehendmedical', aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key, region_name = region_name)


#sqs=boto3.resource('sqs',aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key, region_name = region_name)
#lambda1=boto3.client('lambda',aws_access_key_id = aws_access_key_id, aws_secret_access_key = aws_secret_access_key, region_name = region_name)
#response=s3.get_object(Bucket='h-textract',Key='SoftwareDeveloperResume-Hari.pdf')



##############################################################################################
def lambda_handler(event,context):
    
    """
    
    This functions receives text (and also triggered) from the Textract API(parsercode.py) and feeds it to the ComprehendMedical API.
    The response from ComprehendMedical API is further broken in to subsequent entities and is stored in Redshift database.
    SNS Topic is always listening to this function and subsequently triggers s3_to_redshift.py with the output from this function.
    
    
    """
    
    
    '''
    
    Response from the parsercode.py , Output from the Textract API is received here in the form event. 
    The response(event) has to be further decoded and is to be given as an input to ComprehendMedical API.
    ComprehendMedical API extracts medical terms from the given input text.
    
    
    '''
    
    print(event['Records'][0]['s3']['object']['key'])
    bucket1 = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    
    
    s3.download_fileobj(Bucket = bucket1, Key = key, Fileobj = bytes_buffer)
    
    
    bytes_buffer = io.BytesIO()
    byte_value = bytes_buffer.getvalue()
    s = byte_value.decode()


    #notify_s3_to_redshift = s3resource.BucketNotification('bucket_name')


    
    if s:
        
        #Each list stores the respective column or attribute extracted by the ComprehendMedical API.
        category = []
        id1 = []
        text = []
        attribute = []
        unmapped = []
        traits_name = []
        traits_score = []
        attributes_type = []
        attributes_score = []
        attributes_text = []
        score = []
        type1 = []
        traits = []
        temp_entity_text = []
        default_dicts = []
        
        
        type1_dict = {}
        traits_dict = {}
        
        
        #Asyncronous call to comprehend medical API with the input from the Textract API.
        result = cm.detect_entities(Text = s)
        entities = result['Entities']

        
        #Breaking the output into entities and converting them to tables and csv files. 
        for entity in entities:
            
            
            if 'Id' in entity:
                id1.append(entity['Id'])
            
            
            else:
                id1.append(None)
            
            
            if 'Type' in entity:
                type1.append(entity['Type'])
                
                
                if entity['Type'] not in type1_dict:
                    type1_dict[entity['Type']] = [(entity['Text'], entity['Score'])]
                
                
                else:
                    type1_dict[entity['Type']].append([(entity['Text'], entity['Score'])])
            
            
            else:
                type1.append(None)
            
            
            if 'Traits' in entity:
                
                
                temp_traits = []
                temp_score_traits = []
                
                
                for trait in entity['Traits']:
                    
                    
                    temp_traits.append(trait['Name'])
                    temp_score_traits.append(trait['Score'])
                    
                    
                    if trait['Name'] not in traits_dict:
                        traits_dict[trait['Name']] = [(entity['Text'], trait['Score'])]
                    
                    
                    else:
                        traits_dict[trait['Name']].append((entity['Text'], trait['Score']))
                
                
                traits_name.append(temp_traits)
                traits_score.append(temp_score_traits)
                #traits.append(entity['Traits'])
            
            else:
                traits_name.append(None)
                traits_score.append(None)
            
            
            if 'Category' in entity:
                category.append(entity['Category'])
            
            
            else:
                category.append(None)
            
            
            if 'Text' in entity:
                text.append(entity['Text'])
            
            
            else:
                text.append(None)
            
            
            if 'Attributes' in entity:
                temp_type = []
                temp_score = []
                temp_text = []
                temp_entity_text.append(entity['Text'])
                entity_dict = defaultdict(lambda : None)
                #attribute.append(entity['Attributes'])
                
                
                for e_a in entity['Attributes']:  #e_a = each_attribute (sub attributes)
                    
                 
                    temp_type.append(e_a['Type'])
                    temp_score.append(e_a['Score'])
                    temp_text.append(e_a['Text'])
                    
                    
                    if e_a['Type'] not in entity_dict:
                        entity_dict['MEDICATION'] = entity['Text']
                        entity_dict[e_a['Type']] = str(e_a['Text'])
                        entity_dict[str(e_a['Type'])+'_SCORE'] = str(e_a['Score'])
                    
                    
                    else:
                        entity_dict[e_a['Type']] += "," + str(e_a['Text'])
                        entity_dict[str(e_a['Type'])+'_SCORE'] += "," + str(e_a['Score'])
                        
                        
                default_dicts.append(entity_dict)
                attributes_type.append(temp_type)
                attributes_score.append(temp_score)
                attributes_text.append(temp_text)
            
            
            else:
                attributes_type.append(None)
                attributes_score.append(None)
                attributes_text.append(None)
            
            
            if 'UnmappedAttributes' in entity:
                unmapped.append(entity['UnmappedAttributes'])
            
            
            else:
                unmapped.append(None)
            
            
            if 'Score' in entity:
                score.append(entity['Score'])
            
            
            else:
                score.append(None)
                
         
        #Complete Patient data detected by comprehend medical is stored in s3 bucket as PATIENT_DATA.csv .
        fi = list(zip(id1, category, score, type1, traits_name, traits_score, text, attributes_type, attributes_text,
                        attributes_score, unmapped))
        
        df1 = pd.DataFrame(fi, columns=['Id', 'category', 'score', 'type', 'traits_name', 'traits_score', 'text',
                                      'attributes_type', 'attributes_text', 'attributes_score', 'unmappedattributes'])
        
        
        csv_buffer1 = StringIO()
        df1.to_csv(csv_buffer1, index = False)
        s3resource.Object('bucket_name', 'PATIENT_DATA.csv').put(Body = csv_buffer1.getvalue())

        
        #Few columns to be dropped for further filtering of data.
        columns_to_drop = ['Id', 'category', 'type', 'traits_name', 'traits_score', 'attributes_type', 'attributes_text',
                         'attributes_score', 'unmappedattributes']

        
        #Segregating data into seperate tables based on the attributes detected and categorized by Comprehend medical from given text. 
        
        
        #ANATOMY table
        df_ANATOMY = df1[df1['category'] == 'ANATOMY']
        display_ANATOMY = df_ANATOMY.drop(columns_to_drop, axis = 1)
        
        
        csv_buffer2 = StringIO()
        df_ANATOMY.to_csv(csv_buffer2, index = False)
        s3resource.Object('bucket_name', 'st_df_ANATOMY.csv').put(Body = csv_buffer2.getvalue())
        
        
        csv_buffer2_1 = StringIO()
        display_ANATOMY.to_csv(csv_buffer2_1, index = False)
        s3resource.Object('bucket_name', 'display_ANATOMY.csv').put(Body = csv_buffer2_1.getvalue())
    
    
        #MEDICAL_CONDITION table
        df_MEDICAL_CONDITION = df1[df1['category'] == 'MEDICAL_CONDITION']
        
        
        csv_buffer3 = StringIO()
        df_MEDICAL_CONDITION.to_csv(csv_buffer3, index = False)
        s3resource.Object('bucket_name', 'st_df_MEDICAL_CONDITION.csv').put(Body = csv_buffer3.getvalue())

        
        #MEDICATION table
        df_MEDICATION = df1[df1['category'] == 'MEDICATION']
        
        
        csv_buffer4 = StringIO()
        df_MEDICATION.to_csv(csv_buffer4,index = False)
        s3resource.Object('bucket_name', 'st_df_MEDICATION.csv').put(Body = csv_buffer4.getvalue())

        
        #TEST_TREATMENT_PROCEDURE table
        df_TEST_TREATMENT_PROCEDURE = df1[df1['category'] == 'TEST_TREATMENT_PROCEDURE']
        
        
        csv_buffer5 = StringIO()
        df_TEST_TREATMENT_PROCEDURE.to_csv(csv_buffer5,index = False)
        s3resource.Object('bucket_name', 'st_df_TEST_TREATMENT_PROCEDURE.csv').put(Body = csv_buffer5.getvalue())

        
        #PHI table
        df_PROTECTED_HEALTH_INFORMATION = df1[df1['category'] == 'PROTECTED_HEALTH_INFORMATION']
        
        
        csv_buffer6 = StringIO()
        df_PROTECTED_HEALTH_INFORMATION.to_csv(csv_buffer6, index = False)
        s3resource.Object('bucket_name', 'st_df_PROTECTED_HEALTH_INFORMATION.csv').put(Body = csv_buffer6.getvalue())

        
        #BRAND_NAME table
        df_brandname = df_MEDICATION[df_MEDICATION['type'] == 'BRAND_NAME']
        display_brandname = df_brandname.drop(columns_to_drop,axis = 1)
        
        
        csv_buffer7 = StringIO()
        df_brandname.to_csv(csv_buffer7, index = False)
        s3resource.Object('bucket_name', 'st_BRANDNAME.csv').put(Body = csv_buffer7.getvalue())
        
        
        csv_buffer7_1 = StringIO()
        display_brandname.to_csv(csv_buffer7_1, index = False)
        s3resource.Object('bucket_name', 'display_BRANDNAME.csv').put(Body = csv_buffer7_1.getvalue())

        
        #GENERIC_NAME (for the medication) table
        df_genericname = df_MEDICATION[df_MEDICATION['type'] == 'GENERIC_NAME']
        display_genericname = df_genericname.drop(columns_to_drop, axis = 1)
        
        
        csv_buffer8 = StringIO()
        df_genericname.to_csv(csv_buffer8, index = False)
        s3resource.Object('bucket_name', 'st_GENERICNAME.csv').put(Body = csv_buffer8.getvalue())
        
        
        csv_buffer8_1 = StringIO()
        display_genericname.to_csv(csv_buffer8_1, index = False)
        s3resource.Object('bucket_name', 'display_GENERICNAME.csv').put(Body = csv_buffer8_1.getvalue())

        
        #Medication entity has subattributes which is also depicted in the from of a table  
        req1 = pd.DataFrame(default_dicts,columns=('MEDICATION', 'STRENGTH', 'FORM','DOSAGE','FREQUENCY', 'ROUTE_OR_MODE',
                                                   'DURATION'
                                                  )
                           )
        
        
        csv_buffer9 = StringIO()
        req1.to_csv(csv_buffer9, index = False)
        s3resource.Object('bucket_name', 'st_MEDICATION.csv').put(Body = csv_buffer9.getvalue())
        
        
        #Every deteced entity, subattribute is associated with the confidence. This table includes confidence(Scores)
        req2 = pd.DataFrame(default_dicts, columns = ('MEDICATION', 'STRENGTH', 'STRENGTH_SCORE', 'FORM',
                                                      'FORM_SCORE', 'DOSAGE', 'DOSAGE_SCORE', 'FREQUENCY',
                                                      'FREQUENCY_SCORE', 'ROUTE_OR_MODE', 'ROUTE_OR_MODE_SCORE',
                                                      'DURATION', 'DURATION_SCORE'
                                                     )
                           )
        
        
        csv_buffer10 = StringIO()
        req2.to_csv(csv_buffer10, index = False)
        s3resource.Object('bucket_name', 'st_MEDICATION_WITH_SCORES.csv').put(Body = csv_buffer10.getvalue())


        df_trait_array = []
        csv_buffer_loop = StringIO()
        
        
        for key,value in traits_dict.items():
            
            temp_file_name = str(key)
            temp_name = pd.DataFrame(value,columns = [str(key),'score'])
            
            
            df_trait_array.append(temp_name)
            temp_name.to_csv(csv_buffer_loop,index = False)
            s3resource.Object(bucket_name, 'st_' + temp_file_name + '.csv').put(Body = csv_buffer_loop.getvalue())
            
            
            csv_buffer_loop.seek(0)
            csv_buffer_loop.truncate(0)
        
        
        columns_to_drop2 = ['Id', 'category', 'type', 'traits_name', 'traits_score']
        
        
        #TESTS table
        df_TESTS = df_TEST_TREATMENT_PROCEDURE[df_TEST_TREATMENT_PROCEDURE['type'] == 'TEST_NAME']
        display_TESTS = df_TESTS.drop(columns_to_drop2, axis = 1)
        
        
        csv_buffer11 = StringIO()
        df_TESTS.to_csv(csv_buffer11, index = False)
        s3resource.Object(bucket_name, 'st_TESTS.csv').put(Body = csv_buffer11.getvalue())
        
        
        csv_buffer11_1 = StringIO()
        display_TESTS.to_csv(csv_buffer11_1,index = False)
        s3resource.Object(bucket_name, 'display_TESTS.csv').put(Body = csv_buffer11_1.getvalue())
        
        
        #TREATMENTS table
        df_TREATMENTS = df_TEST_TREATMENT_PROCEDURE[df_TEST_TREATMENT_PROCEDURE['type'] == 'TREATMENT_NAME']
        display_TREATMENTS = df_TREATMENTS.drop(columns_to_drop2, axis = 1)
        
        
        csv_buffer12 = StringIO()
        df_TREATMENTS.to_csv(csv_buffer12, index = False)
        s3resource.Object(bucket_name, 'st_TREATMENTS.csv').put(Body = csv_buffer12.getvalue())
        
        
        csv_buffer12_1 = StringIO()
        display_TREATMENTS.to_csv(csv_buffer12_1, index = False)
        s3resource.Object(bucket_name, 'display_TREATMENTS.csv').put(Body = csv_buffer12_1.getvalue())

        
        #TEST_TREATMENT_PROCEDURE table
        df_PROCEDURE = df_TEST_TREATMENT_PROCEDURE[df_TEST_TREATMENT_PROCEDURE['type'] == 'PROCEDURE_NAME']
        display_PROCEDURE = df_PROCEDURE.drop(columns_to_drop2, axis = 1)
        
        
        csv_buffer13 = StringIO()
        df_PROCEDURE.to_csv(csv_buffer13,index = False)
        s3resource.Object(bucket_name, 'st_PROCEDURES.csv').put(Body = csv_buffer13.getvalue())
        
        
        csv_buffer13_1 = StringIO()
        display_PROCEDURE.to_csv(csv_buffer13_1,index = False)
        s3resource.Object(bucket_name, 'display_PROCEDURES.csv').put(Body = csv_buffer13_1.getvalue())

        
        #Publishing the response to the SNS topic, triggering the s3_to_redshift.py function
        response = sns.publish(
                                TopicArn = topic_name,
                                Message='SUCCESSFUL',
                                MessageStructure='string'
                              )
        print("job complete")
        
    
    #The status can be viewed in the CloudWatch events
    else:
        print("running")
