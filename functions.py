import math
import logging
import sys
import time
from console_logging.console import Console
import requests
import json
import gzip
from azure.storage.blob import BlobServiceClient
import os


date_strftime_format = "%Y-%m-%d %H:%M:%S"
message_format = "%(asctime)s.%(msecs)05d - %(levelname)s - %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format= message_format, datefmt = date_strftime_format,
    handlers=[
#         logging.FileHandler("{:%Y-%m-%d}.log".format(datetime.now())),
        logging.StreamHandler(sys.stdout)
    ]
)

console = Console()

backend_url = "internal-pipeline-geoserver.applications"
backend_port: "8080"
storage_account_key = "o5WqG+2endJxJ531zMI2uUTBpbj23r/bLdBGqAxiA52Z81a/33KDC4bgtHYGY9mLxZ158f1QGKE4jAE2TOGwIg=="
storage_account_name = "sak8sospipeprod"
connection_string = "DefaultEndpointsProtocol=https;AccountName=sak8sospipeprod;AccountKey=o5WqG+2endJxJ531zMI2uUTBpbj23r/bLdBGqAxiA52Z81a/33KDC4bgtHYGY9mLxZ158f1QGKE4jAE2TOGwIg==;EndpointSuffix=core.windows.net"
container_name = "geomesa-fsds"

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "%s %s" % (s, size_name[i])


def uploadToBlobStorage(file_path,file_name):
   blob_service_client = BlobServiceClient.from_connection_string(connection_string)
   blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
   with open(file_path,'rb') as data:
      blob_client.upload_blob(data)
      print(f'Uploaded {file_name}.')

# calling a function to perform upload



def geo_nocount(CQL_FILTER: str):
    try:
        url = "http://internal-pipeline-geoserver.applications:8080/geoserver/omi/ows?service=WFS&version=2.0.0&request=GetFeature&outputFormat=application/json&exceptions=application/json&propertyName=mmsi,status,turn,speed,accuracy,lat,lon,course,heading,maneuver,raim,radio,vessel_type,vessel_name,call_sign,imo,eta,draught,destination,ais_version,md_datetime,md_ds,md_sds,pos_ds,pos_sds,dte,dtg,geom&typeName=omi:ais-enriched-archive&CQL_FILTER={}".format(CQL_FILTER)
        payload={}
        headers = {
        'Authorization': 'Basic YWRtaW46Z2Vvc2VydmVy'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        # result = json.loads(response.text)
        result = response.text
        return result
    except Exception as e:
        logging.exception(e)

def geo_nocount2(CQL_FILTER: str, _year: int, _month: int, _day: int):
    try:
        start_time = time.time()
        url = "http://internal-pipeline-geoserver.applications:8080/geoserver/omi/ows?service=WFS&version=2.0.0&request=GetFeature&outputFormat=application/json&exceptions=application/json&propertyName=mmsi,status,turn,speed,accuracy,lat,lon,course,heading,maneuver,raim,radio,vessel_type,vessel_name,call_sign,imo,eta,draught,destination,ais_version,md_datetime,md_ds,md_sds,pos_ds,pos_sds,dte,dtg,geom&typeName=omi:ais-enriched-archive&CQL_FILTER={}".format(CQL_FILTER)
        payload={}
        headers = {
        'Authorization': 'Basic YWRtaW46Z2Vvc2VydmVy'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        # result = json.loads(response.text)
        result = json.loads(response.text)
        size_temp = len(response.text)
        size = convert_size(size_temp)
        duration = (time.time() - start_time)
        stringdata = json.dumps(result).encode('utf8')
        file_json = "{}-{}-{}.json".format(_year,_month,_day)
        file_gz = "{}-{}-{}.gz".format(_year,_month,_day)
        full_path_json = "./" + file_json
        full_path_gz = "./" + file_gz
        path_blob = "vessel_position/data_exported/hbase/" + _year + "/" + _month + "/" + _day+ "/" + file_gz
        

        with open(full_path_json, "w") as json_file:
            json_file.write(stringdata.decode('utf8'))
        

        with open(full_path_json, "rb") as f_in:
            with gzip.open(full_path_gz, "wb") as f_out:
                f_out.write(f_in.read())
        uploadToBlobStorage(full_path_gz,path_blob)
        os.remove(full_path_json)
        os.remove(full_path_gz)    

        return {
                "size": size,
                "duration": duration,
                "url": CQL_FILTER
                }
    except Exception as e:
        logging.exception(e)


def geo(count: int, CQL_FILTER: str):
    try:
        url = "http://internal-pipeline-geoserver.applications:8080/geoserver/omi/ows?service=WFS&version=2.0.0&request=GetFeature&count={}&outputFormat=application/json&exceptions=application/json&propertyName=mmsi,status,turn,speed,accuracy,lat,lon,course,heading,maneuver,raim,radio,vessel_type,vessel_name,call_sign,imo,eta,draught,destination,ais_version,md_datetime,md_ds,md_sds,pos_ds,pos_sds,dte,dtg,geom&typeName=omi:ais-enriched-archive&CQL_FILTER={}".format(count, CQL_FILTER)
        payload={}
        headers = {
        'Authorization': 'Basic YWRtaW46Z2Vvc2VydmVy'
        }
        response = requests.request("GET", url, headers=headers, data=payload, timeout= 120)
        # result = json.loads(response.text)
        result = response.text
        return result
    except Exception as e:
        logging.exception(e)