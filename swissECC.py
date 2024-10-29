import os
from modules.swissModules import Switzerland
import argparse
from datetime import datetime
from dateutil.relativedelta import relativedelta
from modules.connector import SlackBot

current_date = datetime.now()
previous_date = current_date - relativedelta(months = 1)
year = previous_date.strftime('%Y')
month = previous_date.strftime('%m')

# create a command-line argument parser
# by default, if no year and month are provided, the parser uses the month preceding the current month
parser = argparse.ArgumentParser(description = 'This script retrieves Swiss ECC data from Auto-Schweiz.')
parser.add_argument('--year', 
                    help = "Choose a valid year (e.g. '2023').", 
                    default = year,
                    type = str)
parser.add_argument('--month', 
                    choices = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'], 
                    help = "Choose a valid month (e.g. '10').", 
                    default = month,
                    type = str)
args = parser.parse_args()
year = str(args.year)
month = str(args.month)

# make a subdirectory called switzerland within a directory called data
path = os.path.join('data', 
                    'switzerland')
os.makedirs(path,
            exist_ok = True)

slack = SlackBot(slack_channel = '#global-ecc-scraper')
def print_and_send(text : str,
                   slack = slack) -> None:
    text = 'CH - ' + text
    print(text)
    slack.send_log(text)
    return
date = year + '-' + month
if date > (datetime.now() - relativedelta(months = 1)).strftime('%Y-%m'):
    print_and_send(f"data for {date} has not been released yet...\n\n")
else:
    switzerland_instance = Switzerland(date = date)
    switzerland_instance.make_data_to_BQ()
    switzerland_instance.fuel_type_data_to_BQ()