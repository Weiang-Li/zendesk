import requests
import pandas as pd
from pandas.io.json import json_normalize
from sqlalchemy import *
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import os

pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 100)

env_path = Path(".") / "credentials.env"
load_dotenv(dotenv_path=env_path)
user = os.environ.get('user')
host = os.environ.get('host')
password = os.environ.get('pwd')
database = os.getenv("database")

# Set the request parameters
username = 'your username' + '/token'
pwd = os.environ.get('zendesk_token')


def fix_df(df):
    df = df.rename(str.lower, axis="columns")
    df.columns = df.columns.str.replace("/", "_")
    df.columns = df.columns.str.strip().str.replace("\s+", "_")
    df.columns = df.columns.str.replace("[()]", "_")
    df.columns = df.columns.str.replace(".", "")
    df["last_run"] = str(datetime.now().strftime("%Y-%m-%d %I:%M:%S %p"))
    return df


allStatus = ['open', 'pending', 'closed', 'solved', 'new']
df = pd.DataFrame()
dates = pd.date_range("06-01-2019", "04-09-2021")
for status in allStatus:
    for date in dates:
        date = date.strftime('%Y-%m-%d')
        url = f'https://your organization.zendesk.com/api/v2/search.json?type:ticket&query=status:{status} created>={date}T00:00:00Z created<={date}T23:59:59Z'
        response = requests.get(url, auth=(username, pwd))
        raw = response.json()
        data = pd.DataFrame(raw)
        tickets = json_normalize(data['results'])
        df = pd.concat([df, tickets])

engine = create_engine(
    f'{database}://{user}:{password}@{host}/postgres')  # databasename://username:password@host/databasename
con = engine.connect()

fix_df(df).to_sql('zendesk', con, if_exists='replace', index=bool)
