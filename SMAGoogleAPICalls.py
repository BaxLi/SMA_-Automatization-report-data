
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SERVICE_ACCOUNT_FILE = 'sma-automatization-d95cdc6c39de.json'
WB='1XYa7prf5npKZw5OKmGzizXsUPhbL84o0vLxGKZab1c4'
Page_InterimFB=231244777
credentials = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, 
    scopes=['https://www.googleapis.com/auth/spreadsheets']
)

# Build the service client
service = build('sheets', 'v4', credentials=credentials)

def rangeDef(date_row,startColIdx, endColIndex=None,sheet_id=Page_InterimFB):
    return {
                            "sheetId": sheet_id,  # Replace with your sheet ID
                            "startRowIndex": date_row - 1,  # Rows are zero indexed
                            "endRowIndex": date_row,
                            "startColumnIndex": startColIdx,  # Column B
                            "endColumnIndex": startColIdx + 1 if endColIndex is None else endColIndex
                        }
def create_format_request(range_def, number_format_type, pattern):
    return {
        "repeatCell": {
            "range": range_def,
            "cell": {
                "userEnteredFormat": {
                    "numberFormat": {
                        "type": number_format_type,
                        "pattern": pattern
                    }
                }
            },
            "fields": "userEnteredFormat.numberFormat"
        }
    }

def total_row_format(date_row, sheet_id=Page_InterimFB):
    requests = []
    adspend = create_format_request(rangeDef(date_row, 1), "CURRENCY", '"€"#,##0.00')
    impressions = create_format_request(rangeDef(date_row, 2), "NUMBER", "#0")
    totalleads = create_format_request(rangeDef(date_row, 3), "NUMBER", "#0")
    total_comments = create_format_request(rangeDef(date_row, 4),  "NUMBER", "#0")
    total_cpl = create_format_request(rangeDef(date_row, 5), "CURRENCY", '"€"#,##0.00')
    total_cpComments = create_format_request(rangeDef(date_row, 6), "CURRENCY", '"€"#,##0.00')
    # percentage_format5 = create_format_request(rangeDef(date_row, 5), "PERCENT", "0.00%")
    requests.extend([adspend, impressions,totalleads, total_comments, total_cpl, total_cpComments])
    request_body = {"requests": requests}
    service.spreadsheets().batchUpdate(spreadsheetId=WB, body=request_body).execute()


def campaign_format_dates(date_row, my_spreadsheet=WB):
    request_params = []
    startColumn = 7
    step = 7
    # Equal with FB campaigns
    number_of_sets = 8

    # Loop through each set of 7 columns
    for set_number in range(number_of_sets):
        column_offset = startColumn + (set_number * step)
        adspend = create_format_request(rangeDef(date_row, column_offset), "CURRENCY", '"€"#,##0.00')
        impressions = create_format_request(rangeDef(date_row, column_offset + 1), "NUMBER", "#0")
        total_leads = create_format_request(rangeDef(date_row, column_offset + 2), "NUMBER", "#0")
        total_comments = create_format_request(rangeDef(date_row, column_offset + 3), "NUMBER", "#0")
        total_cpl = create_format_request(rangeDef(date_row, column_offset + 4), "CURRENCY", '"€"#,##0.00')
        total_cpComments = create_format_request(rangeDef(date_row, column_offset + 5), "CURRENCY", '"€"#,##0.00')
        p_of_spend = create_format_request(rangeDef(date_row, column_offset + 6), "PERCENT", "0.00%")

        request_params.extend([adspend, impressions, total_leads, total_comments, total_cpl, total_cpComments, p_of_spend])

    request_body = {"requests": request_params}
    
    service.spreadsheets().batchUpdate(spreadsheetId=my_spreadsheet, body=request_body).execute()

