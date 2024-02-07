
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from SMAFunctions import column_letter_to_index

SERVICE_ACCOUNT_FILE = 'sma-automatization-d95cdc6c39de.json'
WB='1XYa7prf5npKZw5OKmGzizXsUPhbL84o0vLxGKZab1c4'
Page_InterimFB=231244777
credentials = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, 
    scopes=['https://www.googleapis.com/auth/spreadsheets']
)

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

def total_summary_section_format(date_row, workbook_id=None, service=None, start_col=None):
# Ensure service and WB (Workbook ID) are passed correctly
    if service is None or workbook_id is None:
        raise ValueError("All service and sheet_id and WB must be provided")

     # Calculate the starting index from the column letter
    start_col_index = column_letter_to_index(start_col)-1 if start_col is not None else 0

    # Define column formats in a more maintainable structure
    column_formats = [
        (start_col_index+1, "CURRENCY", '"€"#,##0.00'),  # Ad Spend
        (start_col_index+2, "NUMBER", "#0"),              # Impressions
        (start_col_index+3, "NUMBER", "#0"),              # Total Leads
        (start_col_index+4, "NUMBER", "#0"),              # Total Comments
        (start_col_index+5, "CURRENCY", '"€"#,##0.00'),  # Total CPL
        (start_col_index+6, "CURRENCY", '"€"#,##0.00'),  # Total CPComments
    ]
    requests = [
        create_format_request(rangeDef(date_row, col_idx), format_type, pattern)
        for col_idx, format_type, pattern in column_formats
    ]
    request_body = {"requests": requests}
    service.spreadsheets().batchUpdate(spreadsheetId=workbook_id, body=request_body).execute()

def campaign_format_dates(date_row, workbook_id=None, service=None, start_col=None, nr_sets=None):
    if service is None or workbook_id is None:
        raise ValueError("Both service and my_spreadsheet must be provided")

    start_column = 14 if start_col is None else start_col
    step = 7
    number_of_sets = 8 if nr_sets is None else nr_sets
    column_formats = [
        ("CURRENCY", '"€"#,##0.00'),  # Ad Spend
        ("NUMBER", "#0"),             # Impressions
        ("NUMBER", "#0"),             # Total Leads
        ("NUMBER", "#0"),             # Total Comments
        ("CURRENCY", '"€"#,##0.00'),  # Total CPL
        ("CURRENCY", '"€"#,##0.00'),  # Total CPComments
        ("PERCENT", "0.00%"),         # Percentage of Spend
    ]

    requests = []
    for set_number in range(number_of_sets):
        column_offset = start_column + (set_number * step)
        for i, (format_type, pattern) in enumerate(column_formats):
            cell_range = rangeDef(date_row, column_offset + i)
            format_request = create_format_request(cell_range, format_type, pattern)
            requests.append(format_request)

    request_body = {"requests": requests}
    service.spreadsheets().batchUpdate(spreadsheetId=workbook_id, body=request_body).execute()
