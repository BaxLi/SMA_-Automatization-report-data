import gspread
import warnings
import time
import pandas as pd

# from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# from oauth2client.service_account import ServiceAccountCredentials
from SMAFunctions import (fillInterimCampaignsDataColumn, pauseMe, step1_v2_commonCampaignSheetCreate, step2_iterateExport, 
                          create_weeks_summary_sheet, restructure_to_weekly, step2_Totals_Calc, step_Campaign_totals, 
                          column_letter_to_index, create_months_summary_sheet)
from SMAGoogleAPICalls import total_summary_section_format, campaign_format_dates
from SMA_Constants import (CREDENTIALS, workbook_url, interim_campaigns_sheet_name, commonExportedCampaignsSheet, 
                           FB_TOTAL_COL, GOOGLE_TOTAL_COL, GOOGLE_WORKBOOK, GOOGLE_CAMPAIGNS, FB_CAMPAIGNS,TOTAL_CAMPAIGNS_SHEET_NAME,
                           INTERIM_SHEET_DATA, TOTAL_TOTAL_COL, column_index_to_string)
# from SMA_Constants_dentalexcellence import (CREDENTIALS, workbook_url, interim_campaigns_sheet_name, commonExportedCampaignsSheet, 
#                            FB_TOTAL_COL, GOOGLE_TOTAL_COL, GOOGLE_WORKBOOK, GOOGLE_CAMPAIGNS, FB_CAMPAIGNS,TOTAL_CAMPAIGNS_SHEET_NAME,
#                            INTERIM_SHEET_DATA, TOTAL_TOTAL_COL, column_index_to_string)

# Suppress only DeprecationWarnings
warnings.filterwarnings('always')
print(f'LINE 16  -------- >>>      START ! \n')
# Build the service client
service = build('sheets', 'v4', credentials=CREDENTIALS)
# Open the spreadsheet
client = gspread.authorize(CREDENTIALS)
# client = gspread.authorize(creds)
spreadsheet = client.open_by_url(workbook_url)

print(f'LINE 22 - after open Google spreadsheet !')

# # ------------------ STEP-1 - Collect data into campaign_exp_sheet -------------------------------

step1_v2_commonCampaignSheetCreate(spreadsheet)
pauseMe('SMA1 - STEP-1 \n')

# # # # ---------------- STEP-2  Manipulate InterimCampaigns sheet---------------------------------

# # # Reload values from the worksheet where your COMMON campaign data is stored
campaign_exp_sheet = spreadsheet.worksheet(commonExportedCampaignsSheet)
# # # Access the 'Interim' sheet with predefined structure
interim_campaigns_sheet = spreadsheet.worksheet(interim_campaigns_sheet_name)

fillInterimCampaignsDataColumn(interim_campaigns_sheet, campaign_exp_sheet ) 
interim_campaigns_sheet = spreadsheet.worksheet(interim_campaigns_sheet_name)
time.sleep(1)
step2_iterateExport(campaign_exp_sheet, interim_campaigns_sheet)
time.sleep(1)
step_Campaign_totals(interim_campaigns_sheet, FB_TOTAL_COL, GOOGLE_TOTAL_COL) #Calculate FB TOTALS 
time.sleep(1)
# pauseMe(334)
step_Campaign_totals(interim_campaigns_sheet, GOOGLE_TOTAL_COL) #Calculate GOOGLE TOTALS
time.sleep(1)
# pauseMe(334)
step2_Totals_Calc(interim_campaigns_sheet) #Calculate TOTAL summary 
time.sleep(1)

pauseMe('SMA1 - STEP-2 \n')

# # ---------------- STEP-3 FORMATING STEP !!! ---------------------------------

interim_campaigns_sheet = spreadsheet.worksheet(interim_campaigns_sheet_name)
column_b_values = interim_campaigns_sheet.get_values('B3:B')
print(f'{interim_campaigns_sheet.id} interim_campaigns_sheet.row_count={interim_campaigns_sheet.row_count}   COL B len={len(column_b_values)}')
for date_row in range(3, len(column_b_values)+INTERIM_SHEET_DATA):
#  Check if the current cell value in column B is not None
    if date_row - 3< len(column_b_values) and column_b_values[ date_row - 3][0] is not None:  # Corrected the syntax and indexing here
        # print(f'value= {column_b_values[ date_row - 3][0]} - previously processed ...')
        continue
    print(f'{date_row} PROCESSING column {column_index_to_string(column_letter_to_index(FB_TOTAL_COL)+5)} < - > {column_index_to_string(column_letter_to_index(FB_TOTAL_COL)+(len(FB_CAMPAIGNS))*7)} ')
    total_summary_section_format(date_row, GOOGLE_WORKBOOK, service, TOTAL_TOTAL_COL, interim_campaigns_sheet.id) #Total summary formatted
    time.sleep(1)
    total_summary_section_format(date_row, GOOGLE_WORKBOOK, service,FB_TOTAL_COL,interim_campaigns_sheet.id) #Total FB summary formatted
    time.sleep(1)
    total_summary_section_format(date_row, GOOGLE_WORKBOOK, service,GOOGLE_TOTAL_COL,interim_campaigns_sheet.id) #Total GOOGLE summary formatted
    time.sleep(1)
    campaign_format_dates(date_row, GOOGLE_WORKBOOK, service, column_letter_to_index(FB_TOTAL_COL)+5,len(FB_CAMPAIGNS), interim_campaigns_sheet.id)
    time.sleep(1)
    campaign_format_dates(date_row, GOOGLE_WORKBOOK, service, column_letter_to_index(GOOGLE_TOTAL_COL)+5, len(GOOGLE_CAMPAIGNS), interim_campaigns_sheet.id)
    # pauseMe('!!!!!!!!!!! CHEK phase - SMA1 - STEP-2-3 \n') 

pauseMe('SMA1 - STEP-3 \n')
interim_campaigns_sheet = spreadsheet.worksheet(interim_campaigns_sheet_name)
# # ---------------- STEP-4 FORMATTED TOTAL SHEET CREATE  ---------------------------------
# # STEP 10 - format rows per week per month 
restructure_to_weekly(interim_campaigns_sheet,spreadsheet,TOTAL_CAMPAIGNS_SHEET_NAME)
pauseMe('SMA1 - STEP-4 \n')
interim_campaigns_sheet = spreadsheet.worksheet(interim_campaigns_sheet_name)
# # ---------------- STEP-5 CREATE TOTAL WEEKS Sheet with Graph  ---------------------------------
print(' RUNNING step 5 ...')
create_weeks_summary_sheet(spreadsheet, spreadsheet.worksheet(TOTAL_CAMPAIGNS_SHEET_NAME))
pauseMe('SMA1 - STEP-5 \n')
interim_campaigns_sheet = spreadsheet.worksheet(interim_campaigns_sheet_name)

create_months_summary_sheet(spreadsheet, spreadsheet.worksheet(TOTAL_CAMPAIGNS_SHEET_NAME))
