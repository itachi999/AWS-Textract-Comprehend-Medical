import boto3
import json
import pandas as pd
import io
from io import StringIO
from collections import defaultdict


textract=boto3.client('textract',aws_access_key_id='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',aws_secret_access_key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',region_name='us-east-2')

s3=boto3.client('s3',aws_access_key_id='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',aws_secret_access_key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',region_name='us-east-2')

s3resource=boto3.resource('s3',aws_access_key_id='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',aws_secret_access_key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',region_name='us-east-2')

sns=boto3.client('sns',aws_access_key_id='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',aws_secret_access_key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',region_name='us-east-2')

cm=boto3.client('comprehendmedical',aws_access_key_id='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',aws_secret_access_key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',region_name='us-east-2')

#sqs=boto3.resource('sqs',aws_access_key_id='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',aws_secret_access_key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',region_name='us-east-2')

#lambda1=boto3.client('lambda',aws_access_key_id='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',aws_secret_access_key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',region_name='us-east-2')

#response=s3.get_object(Bucket='h-textract',Key='SoftwareDeveloperResume-Hari.pdf')
##############################################################################################
def lambda_handler(event,context):
    print(event['Records'][0]['s3']['object']['key'])
    bucket1=event['Records'][0]['s3']['bucket']['name']
    key=event['Records'][0]['s3']['object']['key']
    client = boto3.client('s3')
    bytes_buffer = io.BytesIO()
    client.download_fileobj(Bucket=bucket1, Key=key, Fileobj=bytes_buffer)
    byte_value = bytes_buffer.getvalue()
    s=byte_value.decode()


    #notify_s3_to_redshift = s3resource.BucketNotification('bucket_name')


    
    if s:
        cm=boto3.client('comprehendmedical',aws_access_key_id='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',aws_secret_access_key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',region_name='us-east-2')

        category=[]
        id1=[]
        text=[]
        attribute=[]
        unmapped=[]
        traits_name=[]
        traits_score=[]
        attributes_type=[]
        attributes_score=[]
        attributes_text=[]
        score=[]
        type1=[]
        traits=[]
        temp_entity_text=[]
        default_dicts=[]

        type1_dict={}
        traits_dict={}

        result = cm.detect_entities(Text=s)
        #print(result)
        #print("#########")
        #print("\n")


        entities = result['Entities']


        for entity in entities:
            if 'Id' in entity:
                id1.append(entity['Id'])
            else:
                id1.append(None)
            if 'Type' in entity:
                type1.append(entity['Type'])
                if entity['Type'] not in type1_dict:
                    type1_dict[entity['Type']]=[(entity['Text'],entity['Score'])]
                else:
                    type1_dict[entity['Type']].append([(entity['Text'],entity['Score'])])
            else:
                type1.append(None)
            if 'Traits' in entity:
                temp_traits=[]
                temp_score_traits=[]
                for trait in entity['Traits']:
                    temp_traits.append(trait['Name'])
                    temp_score_traits.append(trait['Score'])
                    if trait['Name'] not in traits_dict:
                        traits_dict[trait['Name']]=[(entity['Text'],trait['Score'])]
                    else:
                        traits_dict[trait['Name']].append((entity['Text'],trait['Score']))
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
                temp_type=[]
                temp_score=[]
                temp_text=[]
                temp_entity_text.append(entity['Text'])
                entity_dict=defaultdict(lambda : None)
                #attribute.append(entity['Attributes'])
                for e_a in entity['Attributes']:
                    temp_type.append(e_a['Type'])
                    temp_score.append(e_a['Score'])
                    temp_text.append(e_a['Text'])
                    if e_a['Type'] not in entity_dict:
                        entity_dict['MEDICATION']=entity['Text']
                        entity_dict[e_a['Type']]=str(e_a['Text'])
                        entity_dict[str(e_a['Type'])+'_SCORE']=str(e_a['Score'])
                    else:
                        entity_dict[e_a['Type']]+=","+str(e_a['Text'])
                        entity_dict[str(e_a['Type'])+'_SCORE']+=","+str(e_a['Score'])
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
        fi=list(zip(id1,category,score,type1,traits_name,traits_score,text,attributes_type,attributes_text,attributes_score,unmapped))
        df1=pd.DataFrame(fi,columns=['Id','category','score','type','traits_name','traits_score','text','attributes_type','attributes_text','attributes_score','unmappedattributes'])
        csv_buffer1 = StringIO()
        df1.to_csv(csv_buffer1,index=False)
        s3resource.Object('bucket_name', 'PATIENT_DATA.csv').put(Body=csv_buffer1.getvalue())

        columns_to_drop=['Id','category','type','traits_name','traits_score','attributes_type','attributes_text','attributes_score','unmappedattributes']


        df_ANATOMY=df1[df1['category']=='ANATOMY']
        display_ANATOMY=df_ANATOMY.drop(columns_to_drop,axis=1)
        csv_buffer2 = StringIO()
        csv_buffer2_1 = StringIO()
        df_ANATOMY.to_csv(csv_buffer2,index=False)
        display_ANATOMY.to_csv(csv_buffer2_1,index=False)
        s3resource.Object('bucket_name', 'st_df_ANATOMY.csv').put(Body=csv_buffer2.getvalue())
        s3resource.Object('bucket_name', 'display_ANATOMY.csv').put(Body=csv_buffer2_1.getvalue())

        df_MEDICAL_CONDITION=df1[df1['category']=='MEDICAL_CONDITION']
        csv_buffer3 = StringIO()
        df_MEDICAL_CONDITION.to_csv(csv_buffer3,index=False)
        s3resource.Object('bucket_name', 'st_df_MEDICAL_CONDITION.csv').put(Body=csv_buffer3.getvalue())


        df_MEDICATION=df1[df1['category']=='MEDICATION']
        csv_buffer4 = StringIO()
        df_MEDICATION.to_csv(csv_buffer4,index=False)
        s3resource.Object('bucket_name', 'st_df_MEDICATION.csv').put(Body=csv_buffer4.getvalue())


        df_TEST_TREATMENT_PROCEDURE=df1[df1['category']=='TEST_TREATMENT_PROCEDURE']
        csv_buffer5 = StringIO()
        df_TEST_TREATMENT_PROCEDURE.to_csv(csv_buffer5,index=False)
        s3resource.Object('bucket_name', 'st_df_TEST_TREATMENT_PROCEDURE.csv').put(Body=csv_buffer5.getvalue())


        df_PROTECTED_HEALTH_INFORMATION=df1[df1['category']=='PROTECTED_HEALTH_INFORMATION']
        csv_buffer6 = StringIO()
        df_PROTECTED_HEALTH_INFORMATION.to_csv(csv_buffer6,index=False)
        s3resource.Object('bucket_name', 'st_df_PROTECTED_HEALTH_INFORMATION.csv').put(Body=csv_buffer6.getvalue())



        df_brandname=df_MEDICATION[df_MEDICATION['type']=='BRAND_NAME']
        display_brandname=df_brandname.drop(columns_to_drop,axis=1)
        csv_buffer7 = StringIO()
        csv_buffer7_1 = StringIO()
        df_brandname.to_csv(csv_buffer7,index=False)
        display_brandname.to_csv(csv_buffer7_1,index=False)
        s3resource.Object('bucket_name', 'st_BRANDNAME.csv').put(Body=csv_buffer7.getvalue())
        s3resource.Object('bucket_name', 'display_BRANDNAME.csv').put(Body=csv_buffer7_1.getvalue())


        df_genericname=df_MEDICATION[df_MEDICATION['type']=='GENERIC_NAME']
        display_genericname=df_genericname.drop(columns_to_drop,axis=1)
        csv_buffer8 = StringIO()
        csv_buffer8_1 = StringIO()
        df_genericname.to_csv(csv_buffer8,index=False)
        display_genericname.to_csv(csv_buffer8_1,index=False)
        s3resource.Object('bucket_name', 'st_GENERICNAME.csv').put(Body=csv_buffer8.getvalue())
        s3resource.Object('bucket_name', 'display_GENERICNAME.csv').put(Body=csv_buffer8_1.getvalue())


        req1=pd.DataFrame(default_dicts,columns=('MEDICATION', 'STRENGTH', 'FORM','DOSAGE','FREQUENCY', 'ROUTE_OR_MODE','DURATION'))
        csv_buffer9 = StringIO()
        req1.to_csv(csv_buffer9,index=False)
        s3resource.Object('bucket_name', 'st_MEDICATION.csv').put(Body=csv_buffer9.getvalue())

        req2=pd.DataFrame(default_dicts,columns=('MEDICATION', 'STRENGTH','STRENGTH_SCORE', 'FORM','FORM_SCORE','DOSAGE','DOSAGE_SCORE','FREQUENCY','FREQUENCY_SCORE','ROUTE_OR_MODE','ROUTE_OR_MODE_SCORE','DURATION','DURATION_SCORE'))
        csv_buffer10 = StringIO()
        req2.to_csv(csv_buffer10,index=False)
        s3resource.Object('bucket_name', 'st_MEDICATION_WITH_SCORES.csv').put(Body=csv_buffer10.getvalue())


        df_trait_array=[]
        csv_buffer_loop= StringIO()
        for key,value in traits_dict.items():
            temp_file_name=str(key)
            temp_name=pd.DataFrame(value,columns=[str(key),'score'])
            df_trait_array.append(temp_name)
            temp_name.to_csv(csv_buffer_loop,index=False)
            s3resource.Object('bucket_name', 'st_'+temp_file_name+'.csv').put(Body=csv_buffer_loop.getvalue())
            csv_buffer_loop.seek(0)
            csv_buffer_loop.truncate(0)

        columns_to_drop2=['Id','category','type','traits_name','traits_score']


        df_TESTS=df_TEST_TREATMENT_PROCEDURE[df_TEST_TREATMENT_PROCEDURE['type']=='TEST_NAME']
        display_TESTS=df_TESTS.drop(columns_to_drop2,axis=1)
        csv_buffer11 = StringIO()
        csv_buffer11_1=StringIO()
        df_TESTS.to_csv(csv_buffer11,index=False)
        display_TESTS.to_csv(csv_buffer11_1,index=False)
        s3resource.Object('bucket_name', 'st_TESTS.csv').put(Body=csv_buffer11.getvalue())
        s3resource.Object('bucket_name', 'display_TESTS.csv').put(Body=csv_buffer11_1.getvalue())

        df_TREATMENTS=df_TEST_TREATMENT_PROCEDURE[df_TEST_TREATMENT_PROCEDURE['type']=='TREATMENT_NAME']
        display_TREATMENTS=df_TREATMENTS.drop(columns_to_drop2,axis=1)
        csv_buffer12 = StringIO()
        csv_buffer12_1 = StringIO()
        df_TREATMENTS.to_csv(csv_buffer12,index=False)
        display_TREATMENTS.to_csv(csv_buffer12_1,index=False)
        s3resource.Object('bucket_name', 'st_TREATMENTS.csv').put(Body=csv_buffer12.getvalue())
        s3resource.Object('bucket_name', 'display_TREATMENTS.csv').put(Body=csv_buffer12_1.getvalue())


        df_PROCEDURE=df_TEST_TREATMENT_PROCEDURE[df_TEST_TREATMENT_PROCEDURE['type']=='PROCEDURE_NAME']
        display_PROCEDURE=df_PROCEDURE.drop(columns_to_drop2,axis=1)
        csv_buffer13 = StringIO()
        csv_buffer13_1 = StringIO()
        df_PROCEDURE.to_csv(csv_buffer13,index=False)
        display_PROCEDURE.to_csv(csv_buffer13_1,index=False)
        s3resource.Object('bucket_name', 'st_PROCEDURES.csv').put(Body=csv_buffer13.getvalue())
        s3resource.Object('bucket_name', 'display_PROCEDURES.csv').put(Body=csv_buffer13_1.getvalue())


        response = sns.publish(
        TopicArn='arn:aws:sns:us-east-2:671511125968:h-sample-redshift',
        Message='SUCCESSFUL',
        MessageStructure='string'
        )
        print("job complete")
        

    else:
        print("running")
