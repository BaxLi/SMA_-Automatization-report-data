
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
    currency_format1 = create_format_request(rangeDef(date_row, 1), "CURRENCY", '"€"#,##0.00')
    integer_format2 = create_format_request(rangeDef(date_row, 2), "NUMBER", "#0")
    integer_format3 = create_format_request(rangeDef(date_row, 3), "NUMBER", "#0")
    integer_format4 = create_format_request(rangeDef(date_row, 4), "CURRENCY", '"€"#,##0.00')
    integer_format5 = create_format_request(rangeDef(date_row, 5), "CURRENCY", '"€"#,##0.00')
    # percentage_format5 = create_format_request(rangeDef(date_row, 5), "PERCENT", "0.00%")

    requests.extend([currency_format1, integer_format2,integer_format3, integer_format4, integer_format5])

    request_body = {"requests": requests}
    service.spreadsheets().batchUpdate(spreadsheetId=WB, body=request_body).execute()
