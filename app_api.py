from fastapi import FastAPI, Depends
from console_logging.console import Console
import requests
import auth, functions
from fastapi.security.api_key import APIKey
from pydantic import BaseModel
import time


console = Console()
app = FastAPI()

class param(BaseModel):
    count: int
    CQL_FILTER: str

# @app.post("/geo") #> Get method with param
# async def fetch_data(item: param ,api_key: APIKey = Depends(auth.get_api_key)):
#     result = geo(item.count, item.CQL_FILTER)
#     return result

# @app.get("/geo") #> Get method with param
# async def fetch_data(count: int, CQL_FILTER: str ,api_key: APIKey = Depends(auth.get_api_key)):
#     result = geo(count, CQL_FILTER)
#     return result

@app.get("/geo_nocount")
async def fetch_data(CQL_FILTER: str ,api_key: APIKey = Depends(auth.get_api_key)):
    result = functions.geo_nocount(CQL_FILTER)
    return result

@app.get("/geo_nocount2")
async def fetch_data(CQL_FILTER: str ,api_key: APIKey = Depends(auth.get_api_key)):
    result = functions.geo_nocount2(CQL_FILTER)
    return result