## only necessary for local development
# from dotenv import load_dotenv
# load_dotenv() 

import os
db_user = os.getenv("DB_USERNAME")
db_passwd = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_DATABASE")
import sqlalchemy
import pandas as pd


from datetime import date, datetime, timedelta
from enum import Enum
from typing import Union
from fastapi import FastAPI, Path
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

class LeaderboardResponse(BaseModel):
    users: list[str]
    retired_nct: list[float]

    class Config:
        #orm_mode = True
        schema_extra = {
            "example": {
                "users": ["0x02692494f9eb7eb1967510237bdcf821fa646ce2","0xc05a33b9e3b1fbc1ef480cf701bfd11ed53eb0bc","vb.eth"],
                "retired_nct": [4924.5, 4451.7, 4301.3]
            }
        }

### Database stuff ###############################
url = f"mysql+pymysql://{db_user}:{db_passwd}@{db_host}/{db_name}"
engine = sqlalchemy.create_engine(url, connect_args={"ssl":{"ssl_ca":"/etc/ssl/cacert.pem"}})

def get_leaderboard():
    exec_string = f"""SELECT COALESCE(u.uname, e.ens, nct.leaderboard_beneficiary) as beneficiary, sum(nct.amount) as retired_nct, Count(*) as tx_counter 
        FROM t_nct_retired nct
        LEFT JOIN t_ens e ON nct.leaderboard_beneficiary = e.address
        LEFT JOIN t_users u ON nct.leaderboard_beneficiary = u.wallet_pub 
        GROUP BY 1
        ORDER BY retired_nct DESC
        LIMIT 20;"""
    
    df = pd.read_sql(exec_string, engine.connect())

    users = df['beneficiary'].to_list()
    retired_nct = df['retired_nct'].to_list()

    leaderboard_response = LeaderboardResponse(users= users, retired_nct= retired_nct)
    return leaderboard_response

def get_leaderboard_filtered(q_type:str, year:int=None, quarter:int=None, month:int=None):
    if q_type == 'monthly':
        q_where = f'WHERE YEAR(nct.datetime) = {year} AND MONTH(nct.datetime) = {month}'
    elif q_type == 'quarterly':
        q_where = f'WHERE YEAR(nct.datetime) = {year} AND QUARTER(nct.datetime) = {quarter}'
    elif q_type == 'yearly':
        q_where = f'WHERE YEAR(nct.datetime) = {year}'
    
    exec_string = f"""
        SELECT 
            COALESCE(u.uname, e.ens, nct.leaderboard_beneficiary) as beneficiary
            ,SUM(nct.amount) as retired_nct
            ,COUNT(*) as tx_counter 
        FROM t_nct_retired nct
        LEFT JOIN t_ens e ON nct.leaderboard_beneficiary = e.address
        LEFT JOIN t_users u ON nct.leaderboard_beneficiary = u.wallet_pub 
        {q_where}
        GROUP BY 1
        ORDER BY retired_nct DESC
        LIMIT 20;
    """
    
    df = pd.read_sql(exec_string, engine.connect())

    users = df['beneficiary'].to_list()
    retired_nct = df['retired_nct'].to_list()

    leaderboard_response = LeaderboardResponse(users= users, retired_nct= retired_nct)
    return leaderboard_response
######################################################################

tags_metadata = [
    {
        "name": "leaderboard",
        "description": "Data, data, data"
    },
    {
        "name": "archive",
        "description": "-"
    }
]


app = FastAPI(
    title = "Toucant - NCT Leaderboard",
    description = "Toucan leaderboard",
    version = "0.0.1",
    openapi_tags = tags_metadata
)

origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:5173",
    "https://www.toucanleader.xyz/"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/leaderboard", response_model= LeaderboardResponse, tags=['archive'])
async def get_path():
    return get_leaderboard()

@app.get("/monthly/{year}/{month}", response_model= LeaderboardResponse, tags=['leaderboard'])
async def leaderboard_monthly(month: int = Path(title="month of year", ge=1, le=12), year: int = Path(title="Year", ge=2022, le=2025) ):
    return get_leaderboard_filtered(q_type='monthly', year=year, month=month)

@app.get("/quarterly/{year}/{quarter}", response_model= LeaderboardResponse, tags=['leaderboard'])
async def leaderboard_quarterly(quarter: int = Path(title="quarter of year", ge=1, le=4), year: int = Path(title="Year", ge=2022, le=2025) ):
    return get_leaderboard_filtered(q_type='quarterly', year=year, quarter=quarter)

@app.get("/yearly/{year}", response_model= LeaderboardResponse, tags=['leaderboard'])
async def leaderboard_yearly(year: int = Path(title="Year", ge=2022, le=2025) ):
    return get_leaderboard_filtered(q_type='yearly', year=year)