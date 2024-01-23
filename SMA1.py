import gspread
import time
import warnings
import pandas as pd

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from oauth2client.service_account import ServiceAccountCredentials
from SMAFunctions import simplify_adset_name, parse_date, pauseMe, update_sum_formulas_in_row
from SMAGoogleAPICalls import total_row_format, campaign_format_dates
# Suppress only DeprecationWarnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Use the JSON key file you downloaded to set up the credentials
scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(
    'sma-automatization-d95cdc6c39de.json', scope)
client = gspread.authorize(creds)
SERVICE_ACCOUNT_FILE = 'sma-automatization-d95cdc6c39de.json'
WB = '1XYa7prf5npKZw5OKmGzizXsUPhbL84o0vLxGKZab1c4'
Page_InterimFB = 231244777
credentials = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=['https://www.googleapis.com/auth/spreadsheets']
)
# Build the service client
service = build('sheets', 'v4', credentials=credentials)
# Open the spreadsheet
spreadsheet = client.open_by_url(
    'https://docs.google.com/spreadsheets/d/1XYa7prf5npKZw5OKmGzizXsUPhbL84o0vLxGKZab1c4/edit#gid=231244777')

exp_sheet = spreadsheet.worksheet('Facebook Export Data')
fb_sheet = spreadsheet.worksheet('InterimFB')

fb_campaigns = [
    "BAU | Control_AdSet",
    "BAU | DC Type",
    "BAU | LLAs",
    "BAU | RTG",
    "BAU | Lead Generation",
    "BAU | PPE",
    "BAU | Page Likes",
    "nBAU"
]

# Get the first row of the exp_sheet
first_row_values = exp_sheet.row_values(1)

# Prepare a dictionary of terms to replace
replacements = {'Amount spent': 'Ad Spend', 'Contacts (website)': 'Total Leads'}

# Create a new list with the replaced terms
new_first_row_values = [replacements.get(value, value) for value in first_row_values]

# Update the first row with the new values if changes have been made
if first_row_values != new_first_row_values:
    exp_sheet.update('A1', [new_first_row_values])
    # Reload the worksheet to reflect the header changes
    exp_sheet = spreadsheet.worksheet('Facebook Export Data')

# Fetch data into DataFrame
data = pd.DataFrame(exp_sheet.get_all_records())
# print(data.to_string(index=False))
# Convert numerical columns to float and fill missing values
numeric_columns = ['Ad Spend', 'Total Leads',
                   'Post comments', 'Impressions']
for column in numeric_columns:
    data[column] = pd.to_numeric(data[column], errors='coerce').fillna(0)

# Process data
consolidated_data = []

# Function to determine the campaign based on AdSet name


def determine_campaign(adset_name):
    for campaign in fb_campaigns:
        if campaign in adset_name:
            return campaign
    return 'Other'  # Fallback category


# Add a column for the determined campaign
data['Determined Campaign'] = data['AdSet name'].apply(determine_campaign)

# Group and aggregate data by Date and the determined campaign
grouped = data.groupby(['Date', 'Determined Campaign']).agg({
    'Ad Spend': 'sum',
    'Total Leads': 'sum',
    'Post comments': 'sum',
    'Impressions': 'sum'
}).reset_index()

# Convert the DataFrame to a format suitable for gspread
final_data_list = [grouped.columns.tolist()] + grouped.values.tolist()


# Check if 'campaign_exp_sheet' exists or create it
try:
    campaign_exp_sheet = spreadsheet.worksheet('campaign_exp_sheet')
except gspread.exceptions.WorksheetNotFound:
    # If not found, create a new worksheet
    campaign_exp_sheet = spreadsheet.add_worksheet(
        title='campaign_exp_sheet', rows="100", cols="20")

# Write the data to the new sheet
campaign_exp_sheet = spreadsheet.worksheet('campaign_exp_sheet')
campaign_exp_sheet.clear()  # Clear existing data
campaign_exp_sheet.update('A1', final_data_list)  # Update with new data

print(f'Finish row data refactoring')

#Reload values from the worksheet where your campaign data is stored
campaign_exp_sheet = spreadsheet.worksheet('campaign_exp_sheet')

# Now, retrieve the data from the sheet and store it in the 'campaign_exp_data' DataFrame
campaign_exp_data = pd.DataFrame(campaign_exp_sheet.get_all_records())
# print(campaign_exp_data.columns)  # This will print out all column names

# Access the 'InterimFB' sheet
interim_fb_sheet = spreadsheet.worksheet('InterimFB')

# Retrieve headers to find the correct column for each campaign metric
header_row = interim_fb_sheet.row_values(1)  # Campaign names

# Assuming the date column is the first one in 'InterimFB'
dates_column = interim_fb_sheet.col_values(1)[2:]  # Start from row 3 to skip header rows

# Iterate over each row in 'campaign_exp_data'
for index, row in campaign_exp_data.iterrows():
    # Find the row number for the current date in 'InterimFB'
    
    date = row['Date']
    if date in dates_column:
        date_row_number = dates_column.index(date) + 3  # Offset for header rows
    else:
        # Add new row at the end with the date
        date_row_number = len(dates_column) + 3  # New row number
        if (date_row_number>interim_fb_sheet.row_count):
            interim_fb_sheet.append_row([date])  # Append the new date
            dates_column.append(date)  # Update local date list
        else:
            cell_address = gspread.utils.rowcol_to_a1(date_row_number, 1)
            interim_fb_sheet.update_acell(cell_address, row['Date'])

    # Determine the campaign from the 'AdSet name'
    determined_campaign = row['Determined Campaign'] 
    print(f'\n determined_campaign={determined_campaign} date_row_number={date_row_number}\n ============')
    if not determined_campaign:
         # If no matching campaign is found, skip this iteration
        raise ValueError(f"LINE 160 Campaign not found in INTERIM sheet {determined_campaign}.")

    # Prepare the update payload for each metric
    ad_spend = row['Ad Spend']
    impressions = row['Impressions'] if row['Impressions'] else 0
    leads = row['Total Leads'] if row['Total Leads'] else 0
    comments = row['Post comments'] if row['Post comments'] else 0

    # Find the column index for this campaign's "Ad Spend"
    print(f'determined_campaign={header_row.index(determined_campaign)}\n')
    start_column_index=header_row.index(determined_campaign)+1 
    adspend_cell = gspread.utils.rowcol_to_a1(date_row_number,start_column_index)  # 1-based indexing
    impressions_col_cell=gspread.utils.rowcol_to_a1(date_row_number,start_column_index+1)
    leads_col_cell = gspread.utils.rowcol_to_a1(date_row_number,start_column_index+2)
    total_comments_cell = gspread.utils.rowcol_to_a1(date_row_number,start_column_index+3)
    print(f'{start_column_index} adspend_cell={adspend_cell} - impressions={impressions_col_cell} - total_comments_cell= {total_comments_cell} \n')

    # Calculate formulas for 'Total CPL' and 'cpComments'
    total_cpl_formula = f"=IF({leads_col_cell}<>0,{adspend_cell}/{leads_col_cell},0)"
    cp_comments_formula = f"=IFERROR({adspend_cell}/{total_comments_cell},0)"
    percent_of_spend_formula = f"={adspend_cell}/B{date_row_number}"

    # Construct the update values
    update_values = [
        ad_spend,
        impressions,
        leads,
        comments,
        total_cpl_formula,
        cp_comments_formula,
        percent_of_spend_formula
    ]

    # Calculate the range to be updated
    start_cell = adspend_cell
    end_cell = gspread.utils.rowcol_to_a1(date_row_number, start_column_index + 6)  # Assuming there are 7 metrics to update

    # Update the 'InterimFB' sheet
    fb_sheet.update(f'{start_cell}:{end_cell}', [update_values], value_input_option='USER_ENTERED')
    # raise ValueError(f"LINE 190  INTERIM sheet updated date_row_number={date_row_number}")

# pauseMe(33)


for date_row in range(3, fb_sheet.row_count):
    update_sum_formulas_in_row(fb_sheet, date_row)
    total_row_format(date_row)
    campaign_format_dates(date_row)
