import gspread
import warnings
import time
import pandas as pd

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from oauth2client.service_account import ServiceAccountCredentials
from SMAFunctions import  pauseMe, update_sum_formulas_in_row_TOTAL_company, step1_v2_commonCampaignSheetCreate, step2_iterateExport
from SMAGoogleAPICalls import total_row_format, campaign_format_dates
from SMA_Constants import fb_campaigns, google_campaigns,interim_campaigns_sheet_name,commonExportedCampaignsSheet  
# Suppress only DeprecationWarnings
warnings.filterwarnings('always')

# Use the JSON key file you downloaded to set up the credentials
scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(
    'sma-automatization-d95cdc6c39de.json', scope)
client = gspread.authorize(creds)
SERVICE_ACCOUNT_FILE = 'sma-automatization-d95cdc6c39de.json'
Google_workbook = '1XYa7prf5npKZw5OKmGzizXsUPhbL84o0vLxGKZab1c4'
Google_doc_sheet_id = 231244777
credentials = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=['https://www.googleapis.com/auth/spreadsheets']
)
print(f'LINE 28 !')
# Build the service client
service = build('sheets', 'v4', credentials=credentials)
# Open the spreadsheet
spreadsheet = client.open_by_url(
    'https://docs.google.com/spreadsheets/d/1XYa7prf5npKZw5OKmGzizXsUPhbL84o0vLxGKZab1c4/edit#gid=231244777')
print(f'LINE 34 - after open Google spreadsheet !')

# ------------------ STEP-1 -------------------------------

# step1_v2_commonCampaignSheetCreate(spreadsheet)
# pauseMe(1)

# ---------------- STEP-2 ---------------------------------

# #Reload values from the worksheet where your COMMON campaign data is stored
# campaign_exp_sheet = spreadsheet.worksheet(commonExportedCampaignsSheet)

# # Access the 'Interim' sheet with predefined structure
# # print(f'Read from InetromSHeet - {interim_campaigns_sheet_name} \n')
# interim_campaigns_sheet = spreadsheet.worksheet(interim_campaigns_sheet_name)

# step2_iterateExport(campaign_exp_sheet, interim_campaigns_sheet)

# ---------------- STEP-3 ---------------------------------

interim_campaigns_sheet = spreadsheet.worksheet(interim_campaigns_sheet_name)
print(f' LINE 129 interim_campaigns_sheet.row_count={interim_campaigns_sheet.row_count}\n -------------------')

# for date_row in range(3, interim_campaigns_sheet.row_count+1):
for date_row in range(3, 4):
    update_sum_formulas_in_row_TOTAL_company('FB',interim_campaigns_sheet, date_row)
    # update_sum_formulas_in_row_TOTAL_company('Google',interim_campaigns_sheet, date_row)
    # time.sleep(1)
    # total_row_format(date_row, Google_workbook, service)
    # time.sleep(1)
    # campaign_format_dates(date_row, Google_workbook, service)