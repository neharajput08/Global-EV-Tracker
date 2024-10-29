import requests
import pandas as pd
from modules.connector import MyBigQuery, SlackBot, MyBucket
import os
import io
from modules import  DATASET_NAME, BUCKET_NAME, UNIQUE_FIELDS_2, JOB_CONFIG_2
from google.cloud import bigquery
from dateutil.relativedelta import relativedelta

FUEL_TYPE_DICT_CH = {"Benzin" : "petrol",
                     "Diesel" : "diesel",
                     "Hybrid HEV + MHEV" : "FHEV",
                     "Plug-In PHEV* + REX" : "PHEV",
                     "Elektrisch/BEV" : "BEV",
                     "CNG" : "other",
                     "Wasserstoff/Elektrisch" : "other",
                     "Diverse" : "other",
                     "Elektrisch" : "BEV"}

SWITZERLAND_MAKE = 'rugged-baton-283921.globalECC.switzerland_make'
SWITZERLAND_FT = 'rugged-baton-283921.globalECC.switzerland_fueltype'
TABLE_NAME_CH_MAKE = 'switzerland_make'
TABLE_NAME_CH_FT = 'switzerland_fueltype'
UNIQUE_FIELDS_CH_MAKE = ['make', 'registrations', 'date']
JOB_CONFIG_CH_MAKE = bigquery.LoadJobConfig(schema = [bigquery.SchemaField('make', 'STRING'),
                                                      bigquery.SchemaField('registrations', 'FLOAT'),
                                                      bigquery.SchemaField('date', 'DATE')])
DESTINATION_BLOB_NAME_CH = 'switzerland'

class Switzerland:

    def __init__(self,
                 date : str) -> None:
        """
        Initialises the Switzerland class.

        :return: None
        """  
        self.bq = MyBigQuery()
        self.slack = SlackBot(token_file = './credentials/slack.json',
                              slack_channel = '#global-ecc-scraper')
        self.date = date
        return None
    
    def print_and_send(self,
                       text : str) -> None:
        """
        Prints the provided text with the prefix "CH" (the two-letter country code for Switzerland) and sends it to the "global-ecc-scraper" Slack channel.

        :param text: The text to be printed and sent.
        """
        text = 'CH - ' + text
        print(text)
        self.slack.send_log(text)
        return None
    
    def make_request(self) -> None:
        """
        Retrieves and processes data for the specified date.

        :return: None.
        """
        # make a request to retrieve the relevant data in Excel format
        headers = {'Referer' : 'https://www.auto.swiss/',
                   'Upgrade-Insecure-Requests' : '1',
                   'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                   'sec-ch-ua' : '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                   'sec-ch-ua-mobile' : '?0',
                   'sec-ch-ua-platform' : '"macOS"'}
        one_month_after = (pd.to_datetime(self.date) + relativedelta(months = 1)).strftime('%Y-%m')
        url = f"https://www.auto.swiss/wp-content/uploads/{one_month_after.split('-')[0]}/{one_month_after.split('-')[1]}/MOFISPW{self.date.split('-')[0]}_{self.date.split('-')[1].lstrip('0')}.xlsx"
        response = requests.get(url, 
                                headers = headers)
        if response.status_code == 200:
            excel_data = io.BytesIO(response.content)
            excel_file = pd.ExcelFile(excel_data)
            sheet_names = excel_file.sheet_names
            last_sheet_name = sheet_names[-1]
            df = pd.read_excel(excel_data, 
                               sheet_name = last_sheet_name, 
                               header = 8)
            # save the response data to a locally stored Excel file
            with open(f'data/switzerland/{self.date}.xlsx', 'wb') as file:
                file.write(response.content)
            self.print_and_send(f"successfully retrieved data for {self.date}...\n\n")

            # upload the Excel file to the global_ecc bucket
            bucket = MyBucket(bucket_name = BUCKET_NAME)
            bucket.upload_file_to_bucket(path_file = f'data/switzerland/{self.date}.xlsx',
                                         destination_blob_name = DESTINATION_BLOB_NAME_CH)
            # remove the Excel file
            os.system(f'rm data/switzerland/{self.date}.xlsx')
        else:
            self.print_and_send(f"failed to retrieve data for {self.date}...\n\n")
        return df
    
    def clean_make_data(self) -> pd.DataFrame:
        """
        Cleans and filters the make data.

        :return: A Pandas DataFrame containing the cleaned make data.
        """
        df = self.make_request()
        # get rid of unnecessary rows and columns
        index_total = df.index[df["Marken / marques"] == "Total"].tolist()[0]
        make_df = df.iloc[:index_total, [0, 3]]
        # rename the columns
        make_df.columns = ["make",
                           "registrations"]
        # add a date column
        full_date = self.date + '-01'
        make_df['date'] = pd.to_datetime(full_date)
        return make_df

    def clean_fuel_type_data(self) -> pd.DataFrame:
        """
        Cleans and filters the fuel type data.

        :return: A Pandas DataFrame containing the cleaned fuel type data.
        """
        df = self.make_request()
        # get rid of unnecessary rows and columns
        index_benzin = df.index[df["Marken / marques"] == "Benzin"].tolist()[0]
        fuel_type_df = df.iloc[index_benzin : index_benzin + 8, [0, 3]]
        # rename the columns
        fuel_type_df.columns = ["fuelType",
                                "registrations"]
        # map the fuel types to their corresponding categories
        fuel_type_df["fuelType"] = fuel_type_df["fuelType"].map(FUEL_TYPE_DICT_CH)
        # aggregate the DataFrame by fuel type
        fuel_type_df = fuel_type_df.groupby("fuelType", 
                                            as_index = False)\
                                   .agg({"registrations" : "sum"})
        # add a date column
        full_date = self.date + '-01'
        fuel_type_df['date'] = pd.to_datetime(full_date)
        return fuel_type_df
        
    def make_data_to_BQ(self) -> None:
        """
        Uploads the make data to BigQuery.

        :return: None
        """
        # push the make DataFrame to BigQuery
        try:
            self.bq.append_from_df(table_name = TABLE_NAME_CH_MAKE,
                                   df = self.clean_make_data(),
                                   dataset_name = DATASET_NAME,
                                   unique_fields = UNIQUE_FIELDS_CH_MAKE,
                                   job_config = JOB_CONFIG_CH_MAKE)
            self.print_and_send(f'{SWITZERLAND_MAKE} updated with data from {self.date}!\n\n')
        except:
            self.print_and_send(f"failed to update {SWITZERLAND_MAKE} with data from {self.date}...\n\n")
        return None
    
    def fuel_type_data_to_BQ(self) -> None:
        """
        Uploads the fuel type data to BigQuery.

        :return: None
        """
        # push the fuel type DataFrame to BigQuery
        try:
            self.bq.append_from_df(table_name = TABLE_NAME_CH_FT,
                                   df = self.clean_fuel_type_data(),
                                   dataset_name = DATASET_NAME,
                                   unique_fields = UNIQUE_FIELDS_2,
                                   job_config = JOB_CONFIG_2)
            self.print_and_send(f'{SWITZERLAND_FT} updated with data from {self.date}!\n\n')
        except:
            self.print_and_send(f"failed to update {SWITZERLAND_FT} with data from {self.date}...\n\n")
        return None
