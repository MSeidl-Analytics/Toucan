## only necessary for local development
# from dotenv import load_dotenv
# load_dotenv() 

import os
sql_pw = os.environ["SQL_PW"]
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
                "users": ["0x02692494f9eb7eb1967510237bdcf821fa646ce2","0xc05a33b9e3b1fbc1ef480cf701bfd11ed53eb0bc","tableland.eth"],
                "retired_nct": [4924.5, 4451.7, 4301.3]
            }
        }

### Database stuff ###############################
engine = sqlalchemy.create_engine(f'mssql+pymssql://web3_admin:{sql_pw}@dev-web3.database.windows.net/sap')

def get_leaderboard():
    exec_string = f"""with temp as(
            SELECT fromAddress as 'user', sum(value) as retiredNCT
            FROM toucan_nct nct
            WHERE toAddress = '0x0000000000000000000000000000000000000000'
            GROUP BY fromAddress
        )
        SELECT TOP 20 COALESCE(ens, "user") as 'user', retiredNCT
        FROM temp nct
        LEFT JOIN toucan_ens ens ON nct."user" = ens.address
        ORDER BY retiredNCT DESC;"""
    
    df = pd.read_sql(exec_string, engine.connect())

    users = df['user'].to_list()
    retired_nct = df['retiredNCT'].to_list()

    leaderboard_response = LeaderboardResponse(users= users, retired_nct= retired_nct)
    return leaderboard_response
######################################################################

# tags_metadata = [
#     {
#         "name": "alerts",
#         "description": "Create and delete alerts",
#     },
#     {
#         "name": "analytics",
#         "description": "Data, data, data"
#     },
#     {
#         "name": "others",
#         "description": "-"
#     }
# ]


app = FastAPI(
    title = "Toucant - NCT Leaderboard",
    #description = "ImX tooling ...",
    version = "0.0.1"
    #openapi_tags = tags_metadata
)

origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:5173"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/leaderboard", response_model= LeaderboardResponse)
async def get_path():
    return get_leaderboard()