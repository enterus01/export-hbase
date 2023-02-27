from fastapi import FastAPI, Depends
import auth, functions
from fastapi.security.api_key import APIKey
from pydantic import BaseModel

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
async def fetch_data(CQL_FILTER: str, _year: str, _month: str, _day: str, api_key: APIKey = Depends(auth.get_api_key)):
    result = functions.geo_nocount2(CQL_FILTER, _year, _month, _day)
    return result