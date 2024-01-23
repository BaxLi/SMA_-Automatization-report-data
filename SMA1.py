import gspread
import time
import warnings

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from oauth2client.service_account import ServiceAccountCredentials
from SMAFunctions import simplify_adset_name, parse_date, pauseMe, update_sum_formulas_in_row
from SMAGoogleAPICalls import total_row_format
# Suppress only DeprecationWarnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Use the JSON key file you downloaded to set up the credentials
scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('sma-automatization-d95cdc6c39de.json', scope)
client = gspread.authorize(creds)
SERVICE_ACCOUNT_FILE = 'sma-automatization-d95cdc6c39de.json'
Page_InterimFB=231244777
credentials = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, 
    scopes=['https://www.googleapis.com/auth/spreadsheets']
)

# Build the service client
service = build('sheets', 'v4', credentials=credentials)
fb_campaigns= [
    "BAU | Control_AdSet",
    "BAU | DC Type",
    "BAU | LLAs",
    "BAU | RTG",
    "BAU | Lead Generation",
    "BAU | PPE",
    "BAU | Page Likes",
    "nBAU"
]
# MAPPED columns
DT='Date'
AS='Amount spent'
LD='Leads'
PC='Post comments'
ADS='Ad Spend'
IMP='Impressions'
TOTALCPL='Total CPL'
C_name='AdSet name'

# Open the spreadsheet and the two sheets
spreadsheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1XYa7prf5npKZw5OKmGzizXsUPhbL84o0vLxGKZab1c4/edit#gid=231244777')
WB='1XYa7prf5npKZw5OKmGzizXsUPhbL84o0vLxGKZab1c4'
exp_sheet = spreadsheet.worksheet('Facebook Export Data')
fb_sheet = spreadsheet.worksheet('InterimFB')
print('LINE 16 step-1 \n')

# Function to find or create a column for the AdSet
def find_or_create_adset_column(adset_name, sheet):
    time.sleep(1)
    # Get all the headers using the formula option to ensure we get the actual content
    headers = sheet.row_values(1, value_render_option='FORMULA')
    print(f'LINE 30 HEADERS\n {headers} \n -=-=-=-=-=-=-=-=-=-=-=')
    start_col_idx = 7  # Assuming headers start at column 'G'

    # Check if the adset_name is already in the headers
    if adset_name in headers[start_col_idx -1:]:
        # If found, return its column index
        res=headers.index(adset_name)
        # print(f'RC END Merge  coumn={gspread.utils.rowcol_to_a1(1, res)}\n\n')
        return res+1
    else:
        # If not found, add new columns and headers
        # Calculate the index to insert the new columns
        COLUMNS_TO_ADD=6
        insert_index = sheet.col_count+1 # to insert after the last header
        print(f'insert_index={insert_index} sheet_col_count={sheet.col_count}')

        sheet.add_cols(COLUMNS_TO_ADD)

        # Build the range string in A1 notation, e.g., "X1:AC1000"
        range_string = f"{gspread.utils.rowcol_to_a1(1, insert_index)}:{gspread.utils.rowcol_to_a1(sheet.row_count, sheet.col_count)}"

        # Create a list of None values for each cell in the range
        empty_values = [['' for _ in range(COLUMNS_TO_ADD)] for _ in range(sheet.row_count)]

        # Update the range with None values
        print(f'range_string={range_string}\n')
        sheet.update(range_name=range_string, values=empty_values, value_input_option='RAW')
        sheet.update(gspread.utils.rowcol_to_a1(1, insert_index),adset_name)
        request_body = {
                        "requests": [
                            {
                                "mergeCells": {
                                    "range": {
                                        "sheetId": Page_InterimFB,  # Replace with the actual sheet ID
                                        "startRowIndex": 0,
                                        "endRowIndex": 1,
                                        "startColumnIndex": insert_index-1,  # Columns are zero-indexed, so column 5 is index 4
                                        "endColumnIndex": insert_index+5   # Merge up to (but not including) column 11
                                    },
                                    "mergeType": "MERGE_ALL"  # Other options: MERGE_COLUMNS, MERGE_ROWS
                                }
                            }
                        ]
                    }
        time.sleep(1)
        service.spreadsheets().batchUpdate(spreadsheetId=WB, body=request_body).execute()

        new_headers = ['Ad Spend', 'Total Leads', 'Total Comments', 'Total CPL', 'cpComments', '% of Spend']
        for i, header in enumerate(new_headers):
            # Calculate the cell for the current header
            cell = gspread.utils.rowcol_to_a1(2, insert_index + i)
            # Update the cell with the header
            sheet.update(cell, header)
            request_body = {
                    "requests": [
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": 231244777,  # Use the correct sheet ID
                                    "startRowIndex": 1,  # Since rows are zero-indexed
                                    "endRowIndex": 2,
                                    "startColumnIndex": insert_index - 1,  # Adjust as needed
                                    "endColumnIndex": insert_index + len(new_headers) - 1
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "horizontalAlignment": "CENTER"  # or "LEFT", "RIGHT"
                                    }
                                },
                                "fields": "userEnteredFormat.horizontalAlignment"
                            }
                        }
                    ]
                }
            # Execute the batch update request
            time.sleep(1)
            service.spreadsheets().batchUpdate(spreadsheetId=WB, body=request_body).execute()
        return insert_index

# Function to find or append a row for the given date
def find_or_append_date_row(date_string, sheet):
    # Parse the date string to a datetime object
    date_obj = parse_date(date_string)
    
    # If parsing was successful
    if date_obj:
        # Convert datetime object to the desired string format, e.g., "2024-01-12"
        formatted_date = date_obj.strftime('%Y-%m-%d')
        
        # Assuming the dates are in the A column
        date_col_values = sheet.col_values(1)
        # Convert all date strings in the column to datetime objects for comparison
        date_col_objs = [parse_date(d) for d in date_col_values if parse_date(d)]

        try:
            # Find the row index for the given date
            row_index = date_col_objs.index(date_obj) + 3  # +1 because spreadsheet rows are 1-indexed
            return row_index
        except ValueError:
            # Date not found, append a new row at the end for this date
            sheet.append_row([formatted_date])
            # Return the new row index
            return len(date_col_values) + 1
    else:
        # Return None or raise an error if the date could not be parsed
        return None

# Fetch all data from the 'Facebook Export Data' sheet
exp_data = exp_sheet.get_all_records()
# print('LINE 148 step-2 \n ------------------------------------------------')

# Process each row in the export data
for row in exp_data:
    adset_name = simplify_adset_name(row[C_name])
    date = parse_date(row['Date'])
    if not date:  # Skip the row if the date couldn't be parsed (headers, etc.)
        continue
    formatted_date = date.strftime('%Y-%m-%d')

    # Check if the AdSet column exists, if not create it
    adset_col = find_or_create_adset_column(adset_name, fb_sheet)
    # print(f'LINE 160 ----    Found ad {adset_name} => at column {adset_col} \n')

    # Check if the date row exists, if not append it
    date_row = find_or_append_date_row(formatted_date, fb_sheet)
    print(f'Line 164 ---- Found date={date}  at row=> {date_row} \n')

    ad_spend = row[AS]
    leads = row[LD]
    if leads == '':
        leads = 0
    comments = row[PC]
    if comments == '':
        comments = 0
    
    start_cell = gspread.utils.rowcol_to_a1(date_row, adset_col)
    end_cell = gspread.utils.rowcol_to_a1(date_row, adset_col + 5)
    leads_col_cell=gspread.utils.rowcol_to_a1(date_row, adset_col+1)
    total_cpl_cell=gspread.utils.rowcol_to_a1(date_row, adset_col+2)
    

    total_cpl_formula = f"=IF({leads_col_cell}<>0,{start_cell}/{leads_col_cell},0)"
    cp_comments=f"=IFERROR({start_cell}/{total_cpl_cell},0)"
    of_spend=f"={start_cell}/B{date_row}"
    update_values = [[float(ad_spend), int(leads), int(comments),total_cpl_formula, cp_comments, of_spend ]]
    
    # Calculate the range to be updated

    print(f'start_cell={start_cell}  {start_cell[0]}   end_cell={end_cell} \n {update_values}  ----------------- \n ')
    # Update using the new range format
    fb_sheet.update(f'{start_cell}:{end_cell}', update_values, value_input_option='USER_ENTERED')


for date_row in range(3, fb_sheet.row_count + 1):
    update_sum_formulas_in_row(fb_sheet, date_row)
    total_row_format(date_row)
