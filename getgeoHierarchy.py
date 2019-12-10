5.04 //15/11/2019
import csv
import json
import boto3


def geoHierarchy_handler(event, context):
    targetbucket = 'geohierarchys3'
    csvkey = 'GeoHierarchyReference.csv'
    jsonkey = 'GeoHierarchyResponse.json'

    s3 = boto3.resource('s3')
    csv_object = s3.Object(targetbucket, csvkey)
    csv_content = csv_object.get()['Body'].read().splitlines()
    s3_client = boto3.client('s3')
    
    l= []
    
    for line in csv_content:
        x = json.dumps(line.decode('utf-8')).split(',')
        continentName = str(x[0])
        countryCode = str(x[1])
        countryName = str(x[2])
        regionCode = str(x[3])
        regionName = str(x[4])
        destinationCode = str(x[5])
        destinationName = str(x[6])
        accomId = str(x[10])
        accomName = str(x[11])
        longitude = str(x[12])
        latitude = str(x[13])
        
        
        if event['countryName'] in countryName:
            jsonCompose = '{ "continentName": ' + continentName + '"' + ','  \
                + ' "countryCode": ' + '"' + countryCode + '"' + ',' \
                + ' "countryName": ' + '"' + countryName + '"' + ',' \
                + ' "regionCode": ' + '"' + regionCode + '"' + ',' \
                + ' "regionName": ' + '"' + regionName + '"' + ',' \
                + ' "destinationCode": ' + '"' + destinationCode + '"' + ',' \
                + ' "destinationName": ' + '"' + destinationName + '"' + ',' \
                + ' "accomId": ' + '"' + accomId + '"' + ',' \
                + ' "accomName": ' + '"' + accomName + '"' + ',' \
                + ' "longitude": ' + '"' + longitude + '"' + ',' \
                + ' "latitude": ' + '"' + latitude + '"' + '}' 
            l.append(jsonCompose)    
    s3_client.put_object(
        Bucket=targetbucket,
        Body=str(l).replace("'", ""),
        Key=jsonkey,
        ServerSideEncryption='AES256'
    )
    
    s3_obj =boto3.client('s3')
    s3_clientobj = s3_obj.get_object(Bucket='geohierarchys3', Key='GeoHierarchyResponse.json')
    s3_clientdata = s3_clientobj['Body'].read().decode('utf-8')
    s3clientlist=json.loads(s3_clientdata)

    
    return {"geoHierarchyResponse":{'geoHierarchy': s3clientlist}}
   