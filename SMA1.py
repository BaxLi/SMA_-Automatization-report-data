import gspread
import warnings
import time
import pandas as pd

# from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# from oauth2client.service_account import ServiceAccountCredentials
from SMAFunctions import (fillInterimCampaignsDataColumn, pauseMe, step1_v2_commonCampaignSheetCreate, step2_iterateExport, 
                          create_weeks_summary_sheet, restructure_to_weekly, step2_Totals_Calc, step_Campaign_totals, 
                          column_letter_to_index)
from SMAGoogleAPICalls import total_summary_section_format, campaign_format_dates
from SMA_Constants import (creds, CREDENTIALS, workbook_url, interim_campaigns_sheet_name, commonExportedCampaignsSheet, 
                           FB_TOTAL_COL, GOOGLE_TOTAL_COL, Google_workbook, google_campaigns, fb_campaigns)

# Suppress only DeprecationWarnings
warnings.filterwarnings('always')
client = gspread.authorize(creds)
print(f'LINE 16  -------- >>>      START ! \n')
# Build the service client
service = build('sheets', 'v4', credentials=CREDENTIALS)
# Open the spreadsheet
spreadsheet = client.open_by_url(workbook_url)
print(f'LINE 22 - after open Google spreadsheet !')

# # ------------------ STEP-1 - Collect data into campaign_exp_sheet -------------------------------

# step1_v2_commonCampaignSheetCreate(spreadsheet)
# pauseMe('SMA1 - STEP-1 \n')

# # ---------------- STEP-2  Manipulate InterimCampaigns sheet---------------------------------

#Reload values from the worksheet where your COMMON campaign data is stored
# campaign_exp_sheet = spreadsheet.worksheet(commonExportedCampaignsSheet)
# Access the 'Interim' sheet with predefined structure
# interim_campaigns_sheet = spreadsheet.worksheet(interim_campaigns_sheet_name)
# fillInterimCampaignsDataColumn(interim_campaigns_sheet, campaign_exp_sheet ) 
# step2_iterateExport(campaign_exp_sheet, interim_campaigns_sheet)
# time.sleep(3)
# step_Campaign_totals(interim_campaigns_sheet, FB_TOTAL_COL, GOOGLE_TOTAL_COL) #Calculate FB TOTALS 
# time.sleep(3)
# # pauseMe(334)
# step_Campaign_totals(interim_campaigns_sheet, GOOGLE_TOTAL_COL) #Calculate GOOGLE TOTALS
# time.sleep(3)
# # pauseMe(334)
# step2_Totals_Calc(interim_campaigns_sheet) #Calculate TOTAL summary 
# time.sleep(3)

# pauseMe('SMA1 - STEP-2 \n')

# ---------------- STEP-3 ---------------------------------

interim_campaigns_sheet = spreadsheet.worksheet(interim_campaigns_sheet_name)
# print(f' LINE 129 interim_campaigns_sheet.row_count={interim_campaigns_sheet.row_count}\n -------------------')

# time.sleep(5)
# for date_row in range(3, interim_campaigns_sheet.row_count+1):
#     print(f'{date_row}')
#     total_summary_section_format(date_row, Google_workbook, service) #Total summary formatted
#     time.sleep(1)
#     total_summary_section_format(date_row, Google_workbook, service,FB_TOTAL_COL) #Total FB summary formatted
#     time.sleep(1)
#     total_summary_section_format(date_row, Google_workbook, service,GOOGLE_TOTAL_COL) #Total GOOGLE summary formatted
#     time.sleep(1)
#     campaign_format_dates(date_row, Google_workbook, service, column_letter_to_index(FB_TOTAL_COL)+5,len(fb_campaigns)-1)
#     time.sleep(1)
#     campaign_format_dates(date_row, Google_workbook, service, column_letter_to_index(GOOGLE_TOTAL_COL)+5, len(google_campaigns)-1)

# pauseMe('SMA1 - STEP-3 \n')
# ---------------- STEP-4 FORMATTED TOTAL SHEET CREATE  ---------------------------------
# STEP 10 - format rows per week per month 
# restructure_to_weekly(interim_campaigns_sheet,spreadsheet,'TOTAL')
# pauseMe('SMA1 - STEP-4 \n')
# ---------------- STEP-5 CREATE TOTAL WEEKS Sheet with Graph  ---------------------------------

create_weeks_summary_sheet(spreadsheet, spreadsheet.worksheet('TOTAL'))
# pauseMe('SMA1 - STEP-5 \n')

