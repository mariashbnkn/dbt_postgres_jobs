
import clickhouse_connect
import os
from dotenv import load_dotenv

load_dotenv()

def get_client():
    client = clickhouse_connect.get_client(
        host=os.getenv('CLICK_HOST'),
        username=os.getenv('CLICK_USER'), 
        password=os.getenv('CLICK_PASSWORD')
        )

    return client