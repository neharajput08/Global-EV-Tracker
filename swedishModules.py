import requests
import pandas as pd
from modules.connector import MyBigQuery, SlackBot, MyBucket
from modules import  DATASET_NAME, UNIQUE_FIELDS, JOB_CONFIG, BUCKET_NAME
import os
from typing import List
import numpy as np

MONTH_DICT_SE = {'01' : 'januari',
                 '02' : 'februari',
                 '03' : 'mars',
                 '04' : 'april',
                 '05' : 'maj',
                 '06' : 'juni',
                 '07' : 'juli',
                 '08' : 'augusti',
                 '09' : 'september',
                 '10' : 'oktober',
                 '11' : 'november',
                 '12' : 'december'}
FUEL_TYPE_DICT_SE = {'Bensin' : 'petrol',
                     'Diesel' : 'diesel',
                     'EL' : 'BEV',
                     'Elhybrid' : 'FHEV',
                     'Etanol' : 'other',
                     'Gas' : 'other',
                     'Laddhybrid' : 'PHEV',
                     'Ospec.' : 'other'}

SWEDEN = 'rugged-baton-283921.globalECC.sweden'
TABLE_NAME_SE = 'sweden'
DESTINATION_BLOB_NAME_SE = 'sweden'

class Sweden:

    def __init__(self,
                 date : str) -> None:
        """
        Initialises the Sweden class.

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
        Prints the provided text with the prefix "SE" (the two-letter country code for Sweden) and sends it to the "global-ecc-scraper" Slack channel.

        :param text: The text to be printed and sent.
        """
        text = 'SE - ' + text
        print(text)
        self.slack.send_log(text)
        return None
    
    def make_request(self,
                     fuelType : str):
        """
        Retrieves and processes data for the specified date.

        :return: A list of Pandas DataFrames. 
        """
        year = self.date[:4]
        month = MONTH_DICT_SE[self.date[-2:]]
        # make a request to retrieve the relevant data as a DataFrame
        headers = {'Accept' : 'application/json, text/plain, */*',
                   'Accept-Language' : 'en-GB,en-US;q=0.9,en;q=0.8',
                   'ActivityId' : 'eb9ee40e-fb6a-4cee-a8e0-6e5d43aa8f93',
                   'Connection' : 'keep-alive',
                   'Content-Type' : 'application/json;charset=UTF-8',
                   'Origin' : 'https://app.powerbi.com',
                   'Referer' : 'https://app.powerbi.com/',
                   'RequestId' : '28f3bcc9-6d6b-5b53-c697-64612333dbc4',
                   'Sec-Fetch-Dest' : 'empty',
                   'Sec-Fetch-Mode' : 'cors',
                   'Sec-Fetch-Site' : 'cross-site',
                   'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
                   'X-PowerBI-ResourceKey' : '813cce17-dcb9-4dfe-bb46-21436ef98cef',
                   'sec-ch-ua' : '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
                   'sec-ch-ua-mobile' : '?0',
                   'sec-ch-ua-platform' : '"macOS"'}
        params = {'synchronous' : 'true'}
        json_data = {
            'version': '1.0.0',
            'queries': [
                {
                    'Query': {
                        'Commands': [
                            {
                                'SemanticQueryDataShapeCommand': {
                                    'Query': {
                                        'Version': 2,
                                        'From': [
                                            {
                                                'Name': 'd',
                                                'Entity': 'DimModel',
                                                'Type': 0,
                                            },
                                            {
                                                'Name': 'm',
                                                'Entity': 'MeasuresTable',
                                                'Type': 0,
                                            },
                                            {
                                                'Name': 'd1',
                                                'Entity': 'DimDate',
                                                'Type': 0,
                                            },
                                            {
                                                'Name': 'd11',
                                                'Entity': 'DimVehicleType',
                                                'Type': 0,
                                            },
                                            {
                                                'Name': 'd2',
                                                'Entity': 'DimFuel',
                                                'Type': 0,
                                            },
                                            {
                                                'Name': 's',
                                                'Entity': 'Slice by',
                                                'Type': 0,
                                            },
                                        ],
                                        'Select': [
                                            {
                                                'Column': {
                                                    'Expression': {
                                                        'SourceRef': {
                                                            'Source': 'd',
                                                        },
                                                    },
                                                    'Property': 'Fabrikat',
                                                },
                                                'Name': 'DimModel.Fabrikat',
                                                'NativeReferenceName': 'Fabrikat',
                                            },
                                            {
                                                'Arithmetic': {
                                                    'Left': {
                                                        'Measure': {
                                                            'Expression': {
                                                                'SourceRef': {
                                                                    'Source': 'm',
                                                                },
                                                            },
                                                            'Property': 'Antal nyregistrerade',
                                                        },
                                                    },
                                                    'Right': {
                                                        'ScopedEval': {
                                                            'Expression': {
                                                                'Measure': {
                                                                    'Expression': {
                                                                        'SourceRef': {
                                                                            'Source': 'm',
                                                                        },
                                                                    },
                                                                    'Property': 'Antal nyregistrerade',
                                                                },
                                                            },
                                                            'Scope': [],
                                                        },
                                                    },
                                                    'Operator': 3,
                                                },
                                                'Name': 'Divide(MeasuresTable.Antal nyregistrerade, ScopedEval(MeasuresTable.Antal nyregistrerade, []))',
                                                'NativeReferenceName': 'Marknadsandel',
                                            },
                                            {
                                                'Column': {
                                                    'Expression': {
                                                        'SourceRef': {
                                                            'Source': 'd',
                                                        },
                                                    },
                                                    'Property': 'TopCodeName',
                                                },
                                                'Name': 'DimModel.TopCodeName',
                                                'NativeReferenceName': 'TopCodeName',
                                            },
                                            {
                                                'Measure': {
                                                    'Expression': {
                                                        'SourceRef': {
                                                            'Source': 'm',
                                                        },
                                                    },
                                                    'Property': 'Antal nyregistrerade',
                                                },
                                                'Name': 'MeasuresTable.Antal nyregistrerade',
                                                'NativeReferenceName': 'Antal',
                                            },
                                        ],
                                        'Where': [
                                            {
                                                'Condition': {
                                                    'In': {
                                                        'Expressions': [
                                                            {
                                                                'Column': {
                                                                    'Expression': {
                                                                        'SourceRef': {
                                                                            'Source': 'd1',
                                                                        },
                                                                    },
                                                                    'Property': 'MånadNamn',
                                                                },
                                                            },
                                                        ],
                                                        'Values': [
                                                            [
                                                                {
                                                                    'Literal': {
                                                                        'Value': f"'{month}'",
                                                                    },
                                                                },
                                                            ],
                                                        ],
                                                    },
                                                },
                                            },
                                            {
                                                'Condition': {
                                                    'In': {
                                                        'Expressions': [
                                                            {
                                                                'Column': {
                                                                    'Expression': {
                                                                        'SourceRef': {
                                                                            'Source': 'd1',
                                                                        },
                                                                    },
                                                                    'Property': 'År',
                                                                },
                                                            },
                                                        ],
                                                        'Values': [
                                                            [
                                                                {
                                                                    'Literal': {
                                                                        'Value': f"'{year}'",
                                                                    },
                                                                },
                                                            ],
                                                        ],
                                                    },
                                                },
                                            },
                                            {
                                                'Condition': {
                                                    'In': {
                                                        'Expressions': [
                                                            {
                                                                'Column': {
                                                                    'Expression': {
                                                                        'SourceRef': {
                                                                            'Source': 'd11',
                                                                        },
                                                                    },
                                                                    'Property': 'Fordonsslag',
                                                                },
                                                            },
                                                        ],
                                                        'Values': [
                                                            [
                                                                {
                                                                    'Literal': {
                                                                        'Value': "'Personbil'",
                                                                    },
                                                                },
                                                            ],
                                                        ],
                                                    },
                                                },
                                            },
                                            {
                                                'Condition': {
                                                    'In': {
                                                        'Expressions': [
                                                            {
                                                                'Column': {
                                                                    'Expression': {
                                                                        'SourceRef': {
                                                                            'Source': 'd2',
                                                                        },
                                                                    },
                                                                    'Property': 'Drivmedelklass',
                                                                },
                                                            },
                                                        ],
                                                        'Values': [
                                                            [
                                                                {
                                                                    'Literal': {
                                                                        'Value': f"'{fuelType}'",
                                                                    },
                                                                },
                                                            ],
                                                        ],
                                                    },
                                                },
                                            },
                                            {
                                                'Condition': {
                                                    'In': {
                                                        'Expressions': [
                                                            {
                                                                'Column': {
                                                                    'Expression': {
                                                                        'SourceRef': {
                                                                            'Source': 's',
                                                                        },
                                                                    },
                                                                    'Property': 'Slice by Fields',
                                                                },
                                                            },
                                                        ],
                                                        'Values': [
                                                            [
                                                                {
                                                                    'Literal': {
                                                                        'Value': "'''MeasuresTable''[Antal nyregistrerade]'",
                                                                    },
                                                                },
                                                            ],
                                                        ],
                                                    },
                                                },
                                            },
                                            {
                                                'Condition': {
                                                    'Between': {
                                                        'Expression': {
                                                            'Column': {
                                                                'Expression': {
                                                                    'SourceRef': {
                                                                        'Source': 'd1',
                                                                    },
                                                                },
                                                                'Property': 'Datum',
                                                            },
                                                        },
                                                        'LowerBound': {
                                                            'DateSpan': {
                                                                'Expression': {
                                                                    'DateAdd': {
                                                                        'Expression': {
                                                                            'Now': {},
                                                                        },
                                                                        'Amount': -60,
                                                                        'TimeUnit': 2,
                                                                    },
                                                                },
                                                                'TimeUnit': 2,
                                                            },
                                                        },
                                                        'UpperBound': {
                                                            'DateSpan': {
                                                                'Expression': {
                                                                    'DateAdd': {
                                                                        'Expression': {
                                                                            'Now': {},
                                                                        },
                                                                        'Amount': -1,
                                                                        'TimeUnit': 2,
                                                                    },
                                                                },
                                                                'TimeUnit': 2,
                                                            },
                                                        },
                                                    },
                                                },
                                            },
                                        ],
                                        'OrderBy': [
                                            {
                                                'Direction': 2,
                                                'Expression': {
                                                    'Measure': {
                                                        'Expression': {
                                                            'SourceRef': {
                                                                'Source': 'm',
                                                            },
                                                        },
                                                        'Property': 'Antal nyregistrerade',
                                                    },
                                                },
                                            },
                                        ],
                                    },
                                    'Binding': {
                                        'Primary': {
                                            'Groupings': [
                                                {
                                                    'Projections': [
                                                        0,
                                                        1,
                                                        3,
                                                    ],
                                                },
                                            ],
                                        },
                                        'DataReduction': {
                                            'DataVolume': 4,
                                            'Primary': {
                                                'Window': {
                                                    'Count': 1000,
                                                },
                                            },
                                        },
                                        'SuppressedJoinPredicates': [
                                            1,
                                        ],
                                        'Version': 1,
                                    },
                                },
                            },
                        ],
                    },
                    'QueryId': '',
                },
            ],
            'cancelQueries': [],
            'modelId': 1876611,
        }
        response = requests.post('https://wabi-north-europe-k-primary-api.analysis.windows.net/public/reports/querydata',
                                 params = params,
                                 headers = headers,
                                 json = json_data)
        if response.status_code == 200:
            self.print_and_send(f"successfully retrieved {fuelType} data for {self.date}...\n\n")
            try:
                data = response.json()['results'][0]['result']['data']['dsr']['DS'][0]['PH'][0]
                makes = []
                registrations = []
                for entry in data['DM0'][1:]:
                    if entry.get('C'):
                        make = entry['C'][0]
                        makes.append(make)
                        registration = entry['C'][-1][:-1] if entry['C'][-1].endswith('L') else None
                        registrations.append(registration)
                first_row_make = data['DM0'][0]['C'][0]
                makes.append(first_row_make)
                first_row_registrations = data['DM0'][0]['C'][2][:-1]
                registrations.append(first_row_registrations)
                df = pd.DataFrame({'make' : makes, 
                                   'registrations' : registrations,
                                   'fuelType' : [fuelType] * len(makes),
                                   'date' : [self.date + '-01'] * len(makes)})
                # convert None values in the registrations column to NaN
                df['registrations'] = df['registrations'].apply(lambda x : np.nan if x is None else x)
                # convert values in the registrations column into numeric data types
                df['registrations'] = pd.to_numeric(df['registrations'], 
                                                    errors = 'coerce')
                # forward fill missing values in the registrations column
                df['registrations'] = df['registrations'].fillna(method = 'ffill')
                # map the fuel types to their corresponding categories
                df['fuelType'] = df['fuelType'].map(FUEL_TYPE_DICT_SE)
                # convert the date column to a datetime format
                df['date'] = pd.to_datetime(df['date'])
            except:
                return None
        else:
            self.print_and_send(f"failed to retrieve {fuelType} data for {self.date}...\n\n")
        return df

    def data_to_BQ(self) -> None:
        """
        Uploads the data to BigQuery.

        :return: None
        """
        df_list = []
        for key in FUEL_TYPE_DICT_SE:
            df = self.make_request(fuelType = key)
            df_list.append(df)
        df = pd.concat(df_list, 
                       ignore_index = True)
        # aggregate the DataFrame by make, fuel type, and date
        df = df.groupby(['make', 'fuelType', 'date'], as_index = False)['registrations'].sum()

        # save the DataFrame as a CSV file
        df.to_csv(f'data/sweden/{self.date}.csv')
        # upload the CSV file to the global_ecc bucket
        bucket = MyBucket(bucket_name = BUCKET_NAME)
        bucket.upload_file_to_bucket(path_file = f'data/sweden/{self.date}.csv',
                                     destination_blob_name = DESTINATION_BLOB_NAME_SE)
        # remove the CSV file
        os.system(f'rm data/sweden/{self.date}.csv')
        
        # push the DataFrame to BigQuery
        try:
            self.bq.append_from_df(table_name = TABLE_NAME_SE,
                                   df = df,
                                   dataset_name = DATASET_NAME,
                                   unique_fields = UNIQUE_FIELDS,
                                   job_config = JOB_CONFIG)
            self.print_and_send(f'{SWEDEN} updated with data from {self.date}!\n\n')
        except:
            self.print_and_send(f"failed to update {SWEDEN} with data from {self.date}...\n\n")
        return None