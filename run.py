from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import requests
import logging
import sys
from console_logging.console import Console
import time
import uuid

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
def process_data(start_time, end_time):
    startT = start_time.isoformat()
    endT = end_time.isoformat()
    print(f"Processing time range from {startT} to {endT}")
    # Your function to process data for the given time range goes here
    pass

def main():
    # Get the input date from the user
    # input_date_str = input("Enter a date (yyyy-mm-dd): ")
    input_date_str = "2022-02-20"
    input_date = datetime.strptime(input_date_str, "%Y-%m-%d")

    # Split the day into 5-minute intervals
    start_time = input_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = start_time + timedelta(days=1)
    interval = timedelta(minutes=2)
    time_range = []
    while start_time < end_time:
        time_range.append((start_time, start_time + interval))
        start_time += interval
    # Run the processing function in 6 threads
    with ThreadPoolExecutor(max_workers=4) as executor:
        for start, end in time_range:
            executor.submit(callAPI, start, end)

def callAPI(start_time, end_time):
    startT = start_time.isoformat()
    endT = end_time.isoformat()
    try:
        id_session = uuid.uuid4()
        logging.info("ID: {} - Requesting GeoMesa API: range time from {} to {}".format(id_session,startT,endT))
        url = "http://20.236.233.187:8080/geo_nocount2?CQL_FILTER=(dtg+during+{}/{})".format(startT,endT)

        payload={}
        headers = {
        'accept': 'application/json',
        'access_token': 'akljnv13bvi2vfo0b0bw'}
        response = requests.request("GET", url, headers=headers, data=payload, timeout=30)
        # result = response.text
        # size = response.headers['content-length']
        print("--- %s seconds in step ---" % (time.time() - starttime))
        # print(size)
        logging.info("ID: {} - Request GeoMesa API successful: {}".format(id_session,response.text))
        return response.text
    except Exception as e:
        logging.error("ID: {} - Request GeoMesa API error. Please check stack trace".format(id_session))
        logging.exception(e)


if __name__ == "__main__":
    starttime = time.time()
    main()
