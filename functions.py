
import math
import logging
import sys
import time
from console_logging.console import Console
import requests

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


async def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "%s %s" % (s, size_name[i])

async def geo_nocount(CQL_FILTER: str):
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

async def geo_nocount2(CQL_FILTER: str):
    try:
        start_time = time.time()
        url = "http://internal-pipeline-geoserver.applications:8080/geoserver/omi/ows?service=WFS&version=2.0.0&request=GetFeature&outputFormat=application/json&exceptions=application/json&propertyName=mmsi,status,turn,speed,accuracy,lat,lon,course,heading,maneuver,raim,radio,vessel_type,vessel_name,call_sign,imo,eta,draught,destination,ais_version,md_datetime,md_ds,md_sds,pos_ds,pos_sds,dte,dtg,geom&typeName=omi:ais-enriched-archive&CQL_FILTER={}".format(CQL_FILTER)
        payload={}
        headers = {
        'Authorization': 'Basic YWRtaW46Z2Vvc2VydmVy'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        # result = json.loads(response.text)
        # result = response.text
        size_temp = len(response.text)
        size = convert_size(size_temp)
        duration = (time.time() - start_time)
        return {
                "size": size,
                "duration": duration,
                "url": CQL_FILTER
                }
    except Exception as e:
        logging.exception(e)

async def geo(count: int, CQL_FILTER: str):
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