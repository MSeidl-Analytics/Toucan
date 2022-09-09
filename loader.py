from types import NoneType
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from dotenv import load_dotenv
load_dotenv() 
import os
alchemy_key = os.getenv("ALCHEMY")
db_user = os.getenv("DB_USERNAME")
db_passwd = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_DATABASE")

from requests.adapters import Retry
import urllib3
from datetime import datetime
import time
import sys
import traceback
import json
import sqlalchemy
import uuid
import pandas as pd
from pangres import upsert

from web3 import Web3
from ens import ENS

class loader:

    def __init__(self):
        self.url = f"mysql+pymysql://{db_user}:{db_passwd}@{db_host}/{db_name}"
        self.engine = sqlalchemy.create_engine(self.url, connect_args={"ssl":{"ssl_ca":"/etc/ssl/cacert.pem"}})

        self.http = urllib3.PoolManager()

        self.db_update_nct_contracts()
        self.last_nct_load =self.get_latest_nct_timestamp()

    def graph_api_call(self, url:str, query:str):
        payload = json.dumps({'query':query})

        response = self.http.request(method='POST', url=url, body=payload, retries=Retry(10))
        return json.loads(response.data)

    def graph_api_load(self, url:str, query:str, path:str, _pageKey:int=0):
        start = True

        while True:
            try:
                query_str = query.replace('VAR_PAGE_KEY', str(_pageKey))
                # ## DEBUG
                # print(query_str)
                response_json = self.graph_api_call(url, query_str)

                dfTemp = pd.json_normalize(response_json['data'], record_path=path, sep='_')
                if start == True:
                    df = dfTemp
                    start = False
                else:
                    df = pd.concat([df,dfTemp])
                
                if dfTemp.shape[0] == 500:
                    _pageKey = df['timestamp'].max()
                    time.sleep(0.2)                
                else:
                    return df
                
            except Exception as e:
                print("unexpected error")
                print(traceback.format_exc())
                return df

    def get_latest_nct_timestamp(self):
        try:
            exec_string = f"""SELECT Max(timestamp) as max from t_nct_retired""" 
            df = pd.read_sql(exec_string, self.engine.connect())
            timestamp = df['max'].values[0]
            return timestamp
        except:
            return 0

    def get_nct_contracts(self):
        url = 'https://api.thegraph.com/subgraphs/name/toucanprotocol/matic'
        query_nct_redeems = '''
        {
            redeems (
                first:500
                orderBy:timestamp, 
                orderDirection:asc
                where: {
                pool: "0xd838290e877e0188a4a44700463419ed96c16107"
                timestamp_gt: VAR_PAGE_KEY
                }
            ){
                token{id}
                timestamp
            }
        }
        '''
        path = 'redeems'

        df = self.graph_api_load(url, query_nct_redeems, path)
        if df.shape[0] > 0:
            self.nct_contracts = json.dumps(df['token_id'].unique().tolist())
            df.drop_duplicates(subset=['token_id'], inplace=True)
            return df[['token_id']]
        else:
            return None

    def get_nct_retirements(self):
        url = 'https://api.thegraph.com/subgraphs/name/toucanprotocol/matic'
        query_nct_tx = '''
        {
        retirements(
                first:500
                orderBy: timestamp
                orderDirection: asc
                where: {
                    token_: {address_in: ''' + self.nct_contracts + '''}
                    timestamp_gt: VAR_PAGE_KEY
                }
            ){
                id
                timestamp
                amount
                creator {id}
                certificate{beneficiary{id}}
                creationTx
                token{
                    address
                    name
                }
        }
        } 
        '''
        path = 'retirements'
        df = self.graph_api_load(url, query_nct_tx, path, self.last_nct_load) ##proper sql append isn't implemented yet
        #df = self.graph_api_load(url, query_nct_tx, path)
        if df.shape[0] > 0:
            ## transform df
            df['datetime'] = df['timestamp'].astype('int').astype("datetime64[s]")  
            df['timestamp'] = df['timestamp'].astype('int')
            self.last_nct_load = df['timestamp'].max()
            df['amount'] = df['amount'] + '.0'
            df['amount'] = df['amount'].apply(pd.to_numeric) / 1e18

            df['certificate_beneficiary_id'] = df['certificate_beneficiary_id'].replace('0x0000000000000000000000000000000000000000', None)
            df['leaderboard_beneficiary'] = df['certificate_beneficiary_id'].combine_first(df['creator_id'])
            print(f" new nct retirements: {df.shape[0]}")
            return df[['id', 'timestamp', 'datetime', 'amount', 'creator_id', 'certificate_beneficiary_id', 'leaderboard_beneficiary', 'token_name', 'token_address', 'creationTx']]
        else:
            print(f" new nct retirements: 0")
            return None

    def db_update_nct_contracts(self):
        df = self.get_nct_contracts()
        if type(df) == pd.DataFrame:
            #df.to_sql(name='t_nct_contracts', con=self.engine, if_exists='replace', index=False, chunksize=10000)
            df.set_index('token_id', inplace=True)
            upsert(con=self.engine,
                df=df,
                table_name='t_nct_contracts',
                if_row_exists='ignore',
                # dtype=dtype,
                create_table=False)

    def db_update_nct_retirements(self):
        df = self.get_nct_retirements()
        if type(df) == pd.DataFrame:
            df.set_index('id', inplace=True)
            upsert(con=self.engine,
                df=df,
                table_name='t_nct_retired',
                if_row_exists='ignore',
                # dtype=dtype,
                create_table=False)
            #df.to_sql(name='t_nct_retired', con=self.engine,if_exists='replace', index=False, chunksize=10000)

    # # lookup ens names for all distinct addresses and write them to DB
    def db_update_ens(self):
        w3 = Web3(Web3.HTTPProvider(f'https://eth-mainnet.g.alchemy.com/v2/{alchemy_key}'))
        ns = ENS.fromWeb3(w3)
        exec_string = "SELECT DISTINCT leaderboard_beneficiary as address FROM t_nct_retired"
        addr_df = pd.read_sql_query(exec_string, con=self.engine)

        ens_df = pd.DataFrame(columns=['address', 'ens'])
        for addr in addr_df['address']:
            ens_name = ns.name(addr)
            if ens_name is not None:
                #print(ens_name)
                new_row = {'address':addr, 'ens':ens_name}
                ens_df = ens_df.append(new_row, ignore_index=True)
        if ens_df.shape[0] > 0:
            ens_df.to_sql(name='t_ens', con=self.engine, if_exists='replace', index=False, chunksize=10000)


    def run(self):
        ## update nct retirements every 10s
        ## update ens mappings and nct contracts every 60 minutes
        counter = 0
        while True:
            self.db_update_nct_retirements()
            print(f'nct updated - {datetime.now()}')
            counter += 1
            time.sleep(10)

            if counter == 360:
                self.db_update_nct_contracts()
                print(f'nct contracts updated - {datetime.now()}')
                self.db_update_ens()
                print(f'ens updated - {datetime.now()}')
                counter = 0

def main():
    myloader = loader()
    myloader.run()

if __name__ == "__main__":
    main()