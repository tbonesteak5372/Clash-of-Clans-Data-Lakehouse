"""
Script that scrapes API Clash of Clans Data and 
stores the data into a MinIO bucket

"""

import requests
import datetime
import logging
import os
from dotenv import load_dotenv
import json
import pandas as pd
import boto3

"""
def get_env_vars():

    Function that loads env file from the root of the project
    Args: None

    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # loads env from root of the project
    logging.info("Loading .env from:", ROOT)
    load_dotenv(os.path.join(ROOT, ".env"))
"""

def get_season():
    """
    Function that derives the current season string that is compatiable with the API URL
    Args: None
    """
    try:
        # This grabs the current year and month
        now = datetime.datetime.now()
        year = now.year
        month = now.month - 4
        season = f"{year}-{month:02d}"
        logging.info(f"Clash of Clans League Ranking Stats from year: {year}, and month: {month} will be pulled from the API")

    except Exception as e:
        logging.error(f"There was an issue getting the current season... Error: {e}")

    return season


def scrape_legends_league_data(season: str) -> list:
    """
    Function that sends that sends an HTTP request to CoC Endpoint,
    parses the JSON payload and stores data of players within a list
    Args: season: Current season, for example: '07-25'
    """

    # try to get API token from env file
    try:
        token = os.getenv("TOKEN")

        if token is None:
            logging.error("Token was unsucessfully grabbed for .env file")
    except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            raise 
    
    # handles request to Clash of Clans API
    headers = {
    'authorization': 'Bearer ' + token,
    'Accept': 'application/json'
    }
    
    after = None  # Keeps track of where the API left off
    players = []  # Keeps track of players fetched
    try:

        i = 1

        logging.info("Starting to pull data from the API ...")

        while True:
            url = f'https://api.clashofclans.com/v1/leagues/29000022/seasons/{season}?limit=25000'  # Base URL

            if after != None:
                url += f"&after={after}"  # Adds cursor to URL

            response = requests.get(url=url, headers=headers)  # Sends HTTP request to endpoint
            logging.info("Getting data from the API ...")

            if response.status_code == 200:  # If request was a sucess
                logging.info("Sucessfully reached the endpoint!")
                data = response.json()  # Makes response pythonic
                players.extend(data.get("items", []))  # Add each player from items to players

                logging.info(f"Finished batch number {i}. The current amount of players fetched from API is: {len(players)}")

                after = data.get("paging", {}) \
                    .get("cursors", {}) \
                    .get("after")  # Gets cursor for each page

                if after == None:  # Once done fetching all records break the infinite while loop
                    logging.info(f"Fetch from API Complete. Total amount of players in Legends League is: {len(players)}")
                    break  # end while loop

            else:
                logging.error(f"Error: {response.status_code} .. {response.text}")
                break

            i += 1

    except Exception as e:
        logging.error(f"Error getting data from the API : {e}")

    return players


def create_parquet_file(players: list, season: str):

    """
    Function that creates a DataFrame and stores the data as a parquet file, then uploads file to MinIO using a boto3 client.
    Args: 
        players: List of players and their stats that will be converted to a pandas DataFrame
        season: Current season formated as a string.
    """

    current_directory = os.path.dirname(os.path.abspath(__file__))
    file_name = f"coc_data_{season}.parquet"
    file_path = os.path.join(current_directory, file_name)

    if len(players) > 0:
        # covert to a pandas df and save as a parquet
        df = pd.json_normalize(players)
        df.to_parquet(file_path)

        load_dotenv()

        aws_access = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        region = os.getenv("AWS_REGION")

        try:
            # Create S3 client for MinIO
            s3_client = boto3.client(
                's3',
                endpoint_url='http://host.docker.internal:9000',
                aws_access_key_id=aws_access,
                aws_secret_access_key=aws_secret,
                region_name=region
            )

            s3_client.upload_file(
            Filename=file_path,
            Bucket="warehouse",
            Key=f"bronze/{file_name}"
            )

        except Exception as e:
            logging.error(f"Error loading parquet data to MinIO ... {e}")

def main():
    """Function that runs the entire process."""
    season = get_season()
    players = scrape_legends_league_data(season)
    create_parquet_file(players, season)

if __name__ == "__main__":
    main()