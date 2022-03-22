from google.cloud import bigquery
import json
import pandas as pd
import requests
import datetime

# General functions
def bigquery_client(project_id):
    # This function connects to bigquery client
    # Use the credentials in other libraries, such as the Google BigQuery
    # client library.
    client = bigquery.Client(project=project_id)
    return client


def get_request(url, header):
    # This function perform get request from site and load the response as json
    page = requests.get(url)
    if page.status_code == 200:
        print("Successfully pulled data...")
        resp = json.loads(page.text)
        return resp
    else:
        print("Unsuccessful in pulling data...")
        return None


def get_crypto_com_tickers(result):
    # This function gets the tickers from crypto.com
    list_columns, list_values = [], []

    ingest_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Get the columns
    columns = [x for x in result['tickers'][0]]
    columns = columns + ['ingested_at']

    # Get the values
    for row in result['tickers']:
        values = []
        for x in row:
            values = values + [row[x]]
        values = values + [ingest_at]
        list_values = list_values + [values]

    df = pd.DataFrame(data=list_values, columns=columns)
    return df


def get_crypto_com_tickers(result):
    # This function gets the tickers from crypto.com
    list_columns, list_values = [], []

    ingest_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Get the columns
    columns = [x for x in result['tickers'][0]]
    columns = columns + ['ingested_at']

    # Get the values
    for row in result['tickers']:
        values = []
        for x in row:
            values = values + [row[x]]
        values = values + [ingest_at]
        list_values = list_values + [values]

    df = pd.DataFrame(data=list_values, columns=columns)
    return df


def get_crypto_com_tickers_historical(result):
    # This function gets the tickers from crypto.com
    # Get the columns
    columns = ['timestamp', 'vol_btc', 'ingest_at']
    ingest_at = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df = pd.DataFrame(columns=columns)

    # Get the values
    for row in result:
        df = df.append({"timestamp": row[0], "vol_btc": row[1], "ingest_at": ingest_at}, ignore_index=True)

    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df


def insert_into_bigquery(client, project_id, dataset_id, table_name, dataframe):
    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True
    )

    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

def upload(url, table_name):
    header = "accept: application/json"
    project_id = "cryptoprojectcha"
    dataset_id = "raw_coingecko"
    client = bigquery_client(project_id)
    res = get_request(url, header)
    # Extract based on table_name
    if table_name == "raw_cronos_details":
        dataframe = get_crypto_com_tickers(res)
    else:
        dataframe = get_crypto_com_tickers_historical(res)
    insert_into_bigquery(client, project_id, dataset_id, table_name, dataframe)

def main(event, context):
    data = {"https://api.coingecko.com/api/v3/exchanges/crypto_com": "raw_cronos_details",
            "https://api.coingecko.com/api/v3/exchanges/crypto_com/volume_chart?days=2": "raw_cronos_tickers",
            "https://api.coingecko.com/api/v3/exchanges/crypto_com": "raw_cronos_general"}

    for key, value in data.items():
        url = key
        table_name = value
        upload(url, table_name)