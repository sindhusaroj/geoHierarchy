# Task Fetching the Data from the Csv file and export as the JSON format
#Version : 1.0
import csv
import json
import boto3
import sys
from botocore.exceptions import ClientError

def geoHierarchy_handler(event, context):
    
    outputs = []
    dictCreate = {}
    targetbucket = 'geohierarchys3'
    csvkey = 'GeoHierarchyReference.csv'
    jsonkey = 'test.json'
    try:
        s3 = boto3.resource('s3')
        csv_object = s3.Object(targetbucket, csvkey)
        csv_content =csv_object.get()['Body'].read().splitlines()
        s3_client = boto3.client('s3')
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            print("The specified csv file does not exist!. Detail. {}".format(ex))
       

    try:
        for line in csv_content:
            line =  line.decode('utf-8')
            splitLine = line.split(',')
            while len(splitLine[3]) <6:
                splitLine[3]= "0" + (splitLine[3])
            while len(splitLine[5]) <6:
                splitLine[5]= "0" + (splitLine[5])  
            while len(splitLine[10]) <6:
                splitLine[10]= "0" + (splitLine[10])
            dictCreate['continentName'] = str(splitLine[0])                      
            dictCreate['countryCode'] = str(splitLine[1])
            dictCreate['countryName'] = str(splitLine[2])
            dictCreate['regionCode'] = str(splitLine[3])
            dictCreate['regionName'] = str(splitLine[4])
            dictCreate['destinationCode'] = str(splitLine[5])
            dictCreate['destinationName'] = str(splitLine[6])
            dictCreate["accomodation"] = [{ 'accomId' :str(splitLine[10]),
            'accomName': str(splitLine[15]), 
            'longitude' : str(splitLine[16]),
            'latitude' : str(splitLine[17]),
            "brand":{'TUIUKBrand' : str(splitLine[12])}}]
            if dictCreate['countryName'] == event['countryName'] :
                outputs.append(dictCreate.copy())


    except Exception as e:
        print("Failed reading file from bucket. Continuing. {}".format(e))

    s3_client.put_object(
        Bucket=targetbucket,
        Body = str(outputs),
        Key=jsonkey,
        ServerSideEncryption='AES256'
    )
    s3_obj =boto3.client('s3')
    s3_clientobj = s3_obj.get_object(Bucket='geohierarchys3', Key='GeoHierarchyResponse.json')
    s3_clientdata = s3_clientobj['Body'].read()
    s3clientlist=json.loads(s3_clientdata)
    return {'geoHierarchyResponse':{"geoHierarchy":outputs }}
    
