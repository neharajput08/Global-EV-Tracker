import requests
import pandas as pd
from modules.connector import MyBigQuery, SlackBot, MyBucket
from modules import DATASET_NAME, UNIQUE_FIELDS, JOB_CONFIG, BUCKET_NAME
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

FILE_PATH_FI = 'data/finland/all_data.csv'
FINLAND = 'rugged-baton-283921.globalECC.finland'
TABLE_NAME_FI = 'finland'
DESTINATION_BLOB_NAME_FI = 'finland'

class Finland:

    def __init__(self) -> None:
        """
        Initialises the Finland class.

        :return: None
        """  
        self.bq = MyBigQuery()
        self.slack = SlackBot(token_file = './credentials/slack.json',
                              slack_channel = '#global-ecc-scraper')
        return None

    def print_and_send(self,
                       text : str) -> None:
        """
        Prints the provided text with the prefix "FI" (the two-letter country code for Finland) and sends it to the "global-ecc-scraper" Slack channel.

        :param text: The text to be printed and sent.
        """
        text = 'FI - ' + text
        print(text)
        self.slack.send_log(text)
        return None
    
    def make_request(self) -> None:
        """
        Retrieves and processes data up to the most recent date.

        :return: None.
        """
        # make a request to retrieve the relevant data in CSV format
        url = "https://trafi2.stat.fi:443/PXWeb/sq/d0f731e4-7a84-444e-9abb-af9aeb2ca1f2"
        response = requests.get(url)
        if response.status_code == 200:
            # save the response data to a locally stored CSV file
            with open(FILE_PATH_FI, "wb") as csv_file:
                csv_file.write(response.content)
            self.print_and_send(f"successfully retrieved data...\n\n")

            # upload the CSV file to the global_ecc bucket
            bucket = MyBucket(bucket_name = BUCKET_NAME)
            bucket.upload_file_to_bucket(path_file = FILE_PATH_FI,
                                         destination_blob_name = DESTINATION_BLOB_NAME_FI)
        else:
            self.print_and_send(f"failed to retrieve data...\n\n")
        return None

    def clean_data(self) -> pd.DataFrame:
        """
        Cleans and filters the data.

        :return: A Pandas DataFrame containing the cleaned data.
        """
        # read the contents of the locally stored CSV file
        df = pd.read_csv(FILE_PATH_FI)
        # remove the CSV file
        os.system(f'rm {FILE_PATH_FI}')
        # melt the DataFrame
        df = pd.melt(df, 
                     id_vars = ["Month", "Year", "Driving power"], 
                     var_name = "make", 
                     value_name = "registrations")
        # add a date column
        df["date"] = df["Year"].astype(str) + "-" + df["Month"].astype(str) + "-01"
        df["date"] = pd.to_datetime(df["date"], 
                                    format = "%Y-%B-%d")
        # get rid of unnecessary columns
        df = df.loc[:, ["Driving power", "make", "registrations", "date"]]
        # rename the columns
        df = df.rename(columns = {"Driving power" : "fuelType",
                                  "make" : "make",
                                  "registrations" : "registrations",
                                  "date" : "date"})
        # modify the registrations column by replacing "-" with "0"
        df["registrations"] = pd.to_numeric(df["registrations"].replace("-", "0"))
        df = df.loc[df["registrations"] != 0]
        return df
    
    def data_to_BQ(self) -> None:
        """
        Uploads the data to BigQuery.

        :return: None
        """
        df = self.clean_data()
        # get the current date in the format '%Y-%m' 
        current_date = datetime.now()
        # get the date of the previous month in the format '%Y-%m-01'
        prev_month_date = current_date - relativedelta(months = 1)
        prev_month_date_format = prev_month_date.strftime('%Y-%m') + "-01"
        if str(df["date"].max()).split()[0] != str(prev_month_date_format):
            self.print_and_send(f"data for {prev_month_date_format} has not been released yet...\n\n")
        else:
            # drop the BigQuery table if it already exists
            try:
                self.bq.bq_client.query(f"""DROP TABLE `{FINLAND}`""").result()
                self.print_and_send(f'{FINLAND} dropped!\n\n')
            except:
                pass
            # push the DataFrame to BigQuery
            try:
                self.bq.append_from_df(table_name = TABLE_NAME_FI,
                                       df = df,
                                       dataset_name = DATASET_NAME,
                                       job_config = JOB_CONFIG)
                self.print_and_send(f'{FINLAND} updated!\n\n')
            except Exception as e:
                self.print_and_send(f"{e} : failed to update {FINLAND}...\n\n")
        return None
