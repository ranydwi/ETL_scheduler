# TMDB Trending Movies ETL Flows
# This process contain 3 flows with 3 task (Extract, Transform, Load) for each flows
# flow 1. Updating Trending movies
# flow 2. Updating Movie Genre
# flow 3. Updating Movie Detail 

# Schema.
# 1. Setiap hari Prefect akan mengambil data trending movies
# 2. Data Trending akan ditambahkan pada table `trending`
# 3. Sistem akan mengambil ID Movie dari table `trending`. Data tersebut akan digunakan untuk update Movie Detail dan Movie Genre
# 4. Ambil detail film dari semua film yang ada pada table `trending` lalu update ke BigQuery

# /*********** Library used **************/
import os
import pandas as pd
import pandas_gbq
import datetime
import requests
from prefect import flow, task
from prefect_gcp import GcpCredentials
from google.oauth2 import service_account
from zoneinfo import ZoneInfo


# //*********** Project Setup ***********/

TMDB_API_KEY = os.getenv("TMDB_API_KEY") 
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
BASE_URL = os.getenv("BASE_URL")
# Load BigQuery Credentials
CREDENTIALS = GcpCredentials.load("bigquery-credentials").get_credentials_from_service_account()
tz = ZoneInfo("Asia/Jakarta")


# /*********** Predefined Task ***********/

# --- Task for Extraction (single request)
@task(name="data-extraction", tags=['extract'], log_prints=True)
def get_data(endpoint:str) -> dict:
    """Make a single request from endpoint URL.

    Args:
        endpoint (str): Endpoint URL.

    Returns:
        dict: Response.
    """
    header = {
        'accept': 'application/json',
        'Authorization': f'Bearer {TMDB_API_KEY}'
    }
    url = BASE_URL + endpoint
    return requests.get(url, headers=header).json()

# --- Task for Extraction (bulk request)
@task(name="data-bulk-extraction", tags=['extract'], log_prints=True)
def get_bulk_data(bulk_request:list, endpoint:str):
    """_summary_

    Args:
        bulk_response (list): List of request.
        endpoint (str): Endpoint URL.

    Returns:
        list: list of Response.
    """
    list_response = []
    for request in bulk_request:
        endpoint_url = endpoint + str(request)
        list_response.append(get_data(endpoint_url))
    return list_response

# --- Task for transformation (template function)
@task(name="data-transform", tags={"transform"}, log_prints=True)
def transform_data(response:list[dict], func_transform=None):
    """Template function for transformation

    Args:
        response (list[dict]): List of response
        func_transform (_type_, optional): Custome transformation function. Defaults to None.

    Returns:
        _type_: _description_
    """
    if func_transform is not None:
        return func_transform(response)
    else:
        return pd.DataFrame(response)

# --- Task for load to BigQuery
@task(name="load-to-gbq", tags={"load"}, log_prints=True)
def load_to_gbq(table:pd.DataFrame, table_name:str, if_exist:str="replace"):
    """Load to Google BigQuery

    Args:
        table (pd.DataFrame): Pandas DataFrame.
        table_name (str): Name of table in BigQuery
        if_exist (str, optional): Behavior when the destination table exists. Defaults to "replace".
    """
    pandas_gbq.to_gbq(
        dataframe=table,
        destination_table=f"{DATASET_ID}.{table_name}",
        project_id=PROJECT_ID,
        if_exists=if_exist,
        credentials=CREDENTIALS
    )

# --- Task for read BigQuery table
@task(name="read-gbq", tags={"load"}, log_prints=True)
def read_gbq(table_name:str):
    """Read BigQuery Table

    Args:
        table_name (str): BigQuery Table's name.

    Returns:
        _type_: _description_
    """
    return pandas_gbq.read_gbq(
        query_or_table=f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`",
        project_id=PROJECT_ID,
        credentials=CREDENTIALS
    )


# /*********** Customize Transformation Task ***********/

# --- Custome transformation for Trending movies
def transform_trending_movies(response:list[dict]):
    df = pd.DataFrame(response["results"]) # convert to DataFrame
    
    # Create Movie Genre table and load to BigQuery
    movie_genre = df[['id', 'genre_ids']].explode('genre_ids')
    load_to_gbq(movie_genre, "movie_genre")
    # movie_genre.to_csv(f"movie_genre.csv") #test-function

    df = df.drop(columns=['original_title', 'media_type', 'genre_ids']) # Drop unnecesary column
    df['release_date'] = pd.to_datetime(df['release_date']) # fixing datetime object
    df['rank'] = df['popularity'].rank(ascending=False) # add ranking based on popularity
    df['trend_date'] = datetime.datetime.now(tz=tz).date() # add ranking date
    return df

# --- Custome transformation for Movie Details
def transform_movie_details(response:list[dict]):
    df = pd.DataFrame(response) # convert to DataFrame
    from_bigquery = read_gbq("movies") # Retrive table detail from BigQuery
    # Cleaning process
    df = df.drop(columns=['genres']) # drop genres
    df['release_date'] = pd.to_datetime(df['release_date']) # fixing datetime object

    df = pd.concat([df, from_bigquery], ignore_index=True)
    df = df.drop_duplicates(subset='id',keep='last')
    return df


# /*********** ETL Flows **************/
@flow(name="etl-flows", flow_run_name="etl-subflow-{name}",log_prints=True)
def etl_flow(name:str, endpoint:str, bulk:bool=False, bulk_response:list=[], func_transform:object=None, method:str='replace'):
    """Subflow process for running basic ETL 

    Args:
        name (str): Name of table
        endpoint (str): endpoint url
        bulk (bool, optional): `True` if do the bulk request. Defaults to False.
        bulk_response (list, optional): Bulk response. Defaults to [].
        func_transform (object, optional): custome function. Defaults to None.
        method (str, optional):  Behavior when the destination table exists. Defaults to 'replace'.
    """
    if bulk:
        data = get_bulk_data(bulk_response, endpoint)
    else:
        data = get_data(endpoint)
    data = transform_data(response=data, func_transform=func_transform)
    load_to_gbq(table=data, table_name=name, if_exist=method)


@flow(name="etl-flows", flow_run_name="etl-mainflow-{timestamp}",log_prints=True)
def tmdb_etl_mainflow(timestamp=datetime.datetime.now(tz=tz)):
    """Mainflow process for running basic ETL 

    Args:
        timestamp (_type_, optional): _description_. Defaults to datetime.datetime.now().
    """
    # Flow 1. Update Trending Movies
    etl_flow(name="trending", endpoint="/trending/movie/day", func_transform=transform_trending_movies, method="append")
    
    # Flow 2. Update Movie Genre
    # Read movieId from Trending Movies
    trending = read_gbq("trending")
    list_id = trending['id'].unique().tolist()
    etl_flow(name="movies", endpoint='/movie/', func_transform=transform_movie_details, bulk=True, bulk_response=list_id, method="replace")


if __name__=="__main__":
    tmdb_etl_mainflow()