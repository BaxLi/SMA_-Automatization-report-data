
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from SMAFunctions import (column_letter_to_index)
from SMA_Constants import CREDENTIALS

SERVICE_ACCOUNT_FILE = 'sma-automatization-d95cdc6c39de.json'
WB='1XYa7prf5npKZw5OKmGzizXsUPhbL84o0vLxGKZab1c4'
Page_InterimFB=231244777
credentials = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, 
    scopes=['https://www.googleapis.com/auth/spreadsheets']
)
# Build the service client
service = build('sheets', 'v4', credentials=CREDENTIALS)

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
    start_col_index = column_letter_to_index(start_col)-2 if start_col is not None else 0
    tp="CURRENCY" if start_col is not None else "PERCENT"
    tp_val='"€"#,##0.00' if start_col is not None else "0.00%"
    tp_cpl="NUMBER" if start_col is not None else "CURRENCY"
    tp_cpl_f='#0' if start_col is not None else '"€"#,##0.00'
    # print(f'{start_col}+1=Index {start_col_index}')

    # Define column formats in a more maintainable structure
    column_formats = [
        (start_col_index+1, "CURRENCY", '"€"#,##0.00'),  # Ad Spend
        (start_col_index+2, "NUMBER", "#0"),              # Impressions
        (start_col_index+3, "NUMBER", "#0"),              # Total Leads
        (start_col_index+4, tp_cpl, tp_cpl_f),              # Total Comments
        (start_col_index+5, tp, tp_val),  # Total CPL  - OR - % of Facebook in common total
        (start_col_index+6, tp, tp_val),  # Total CPComments - OR - % of GOOGEL in common total
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
    print(f'campaign_format_dates: start_column={start_column}')
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

def clear_sheet_formatting_and_content(a_sheet):
    # print(f'def clear_sheet_formatting_and_content')
    # Clear all content
    a_sheet.clear()
    # Reset the formatting for the entire sheet
    requests = []
    requests.append({
        "updateCells": {
            "range": {
                            "sheetId": a_sheet.id,  # Replace with your sheet ID
                            "startRowIndex": 0,  # Rows are zero indexed
                            "endRowIndex": a_sheet.row_count,
                            "startColumnIndex": 0,  
                            "endColumnIndex": a_sheet.col_count
                        },
            "fields": "userEnteredFormat"
        }
    })
    service.spreadsheets().batchUpdate(spreadsheetId=a_sheet.spreadsheet_id, body={"requests": requests}).execute()

    # a_sheet.batch_update({"requests": requests})

def add_borders_to_cells_only_allRows(worksheet, start_col_index, end_col_index):
    requests = {
        "requests": [
            {
                "updateBorders": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 0,  # Start from the first row
                        "endRowIndex": worksheet.row_count,  # Until the last row of the sheet
                        "startColumnIndex": start_col_index-1,
                        "endColumnIndex": end_col_index,
                    },
                    "top": {"style": "SOLID", "width": 1, "color": {"red": 0, "green": 0, "blue": 0}},
                    "bottom": {"style": "SOLID", "width": 1, "color": {"red": 0, "green": 0, "blue": 0}},
                    "left": {"style": "SOLID", "width": 1, "color": {"red": 0, "green": 0, "blue": 0}},
                    "right": {"style": "SOLID", "width": 1, "color": {"red": 0, "green": 0, "blue": 0}},
                    "innerHorizontal": {"style": "SOLID", "width": 1, "color": {"red": 0, "green": 0, "blue": 0}},
                    "innerVertical": {"style": "SOLID", "width": 1, "color": {"red": 0, "green": 0, "blue": 0}},
                }
            }
        ]
    }
    service.spreadsheets().batchUpdate(spreadsheetId=worksheet.spreadsheet_id, body= requests).execute()

    # response = worksheet.spreadsheet.batch_update(requests)

def add_left_right_borders_to_columns(worksheet, start_col_index, end_col_index, service=service):
    """
    Add left border to the starting column and right border to the ending column
    from the first row down to the bottom of the sheet.
    :param worksheet: The worksheet object.
    :param start_col_index: The starting column index (1-based, e.g., 8 for column 'H').
    :param end_col_index: The ending column index (1-based, e.g., 13 for column 'M').
    :param service: The Google Sheets API service object.
    """
    requests = {
        "requests": [
            # Left border for the start column
            {
                "updateBorders": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 0,
                        "endRowIndex": worksheet.row_count,
                        "startColumnIndex": start_col_index - 1,
                        "endColumnIndex": start_col_index,
                    },
                    "left": {
                        "style": "SOLID",
                        "width": 2,
                        "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1},
                    },
                }
            },
            # Right border for the end column
            {
                "updateBorders": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 0,
                        "endRowIndex": worksheet.row_count,
                        "startColumnIndex": end_col_index - 1,
                        "endColumnIndex": end_col_index,
                    },
                    "right": {
                        "style": "SOLID",
                        "width": 2,
                        "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1},
                    },
                }
            }
        ]
    }
    service.spreadsheets().batchUpdate(spreadsheetId=worksheet.spreadsheet_id, body=requests).execute()

def add_up_down_borders_to_rows(worksheet, start_row_index, end_row_index, border_width=1, service=service):
    """
    Add left border to the starting column and right border to the ending column
    from the first row down to the bottom of the sheet.
    :param worksheet: The worksheet object.
    :param start_col_index: The starting column index (1-based, e.g., 8 for column 'H').
    :param end_col_index: The ending column index (1-based, e.g., 13 for column 'M').
    :param service: The Google Sheets API service object.
    """
    print
    requests = {
        "requests": [
            # Left border for the start column
            {
                "updateBorders": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": start_row_index-1,
                        "endRowIndex": start_row_index,
                        "startColumnIndex": 0,
                        "endColumnIndex": worksheet.col_count-1,
                    },
                    "top": {
                        "style": "SOLID",
                        "width": border_width,
                        "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1},
                    },
                }
            },
            # Right border for the end column
            {
                "updateBorders": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": end_row_index-1,
                        "endRowIndex": end_row_index,
                        "startColumnIndex": 0,
                        "endColumnIndex": worksheet.col_count-1,
                    },
                    "bottom": {
                        "style": "SOLID",
                        "width": border_width,
                        "color": {"red": 0, "green": 0, "blue": 0, "alpha": 1},
                    },
                }
            }
        ]
    }
    service.spreadsheets().batchUpdate(spreadsheetId=worksheet.spreadsheet_id, body=requests).execute()

def add_summary_chart_OLD(worksheet, startsLineWith, chart_title="SPEND x Leads x CPL - Weekly"):
    print('EXECUTE - add_summary_chart')
    last_data_row_index = worksheet.row_count 
    print(f'last_data_row_index={last_data_row_index}')

    column_a_values = worksheet.col_values(1)
    print(f'column_a_values = {column_a_values}')
    last_data_row_index = None

    # Iterate in reverse to find the last occurrence
    for index, value in reversed(list(enumerate(column_a_values))):
        if value.startswith(startsLineWith):
            last_data_row_index = index + 1  # +1 because enumerate starts at 0
            break
        if startsLineWith=='month':
            last_data_row_index = index + 1  # +1 because enumerate starts at 0
            break
    print(f'2 - last_data_row_index={last_data_row_index}')

    # Define the chart request body
    requests = {
        "requests": [
            {
                "addChart": {
                    "chart": {
                        "spec": {
                            "title": chart_title,
                            "basicChart": {
                                "chartType": "LINE",
                                "legendPosition": "BOTTOM_LEGEND",
                                "axis": [
                                    {
                                        "position": "BOTTOM_AXIS",
                                        "title": "Weeks"
                                    },
                                    {
                                        "position": "LEFT_AXIS",
                                        "title": "Values"
                                    }
                                ],
                                "domains": [
                                    {
                                        "domain": {
                                            "sourceRange": {
                                                "sources": [
                                                    {
                                                        "sheetId": worksheet.id,
                                                        "startRowIndex": 0,  # Assuming header is in the first row
                                                        "endRowIndex": last_data_row_index,
                                                        "startColumnIndex": 0,  # Weeks column
                                                        "endColumnIndex": 1
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                ],
                                "series": [
                                    {
                                        "series": {
                                            "sourceRange": {
                                                "sources": [
                                                    {
                                                        "sheetId": worksheet.id,
                                                        "startRowIndex": 0,  # Assuming header is in the first row
                                                        "endRowIndex": last_data_row_index,
                                                        "startColumnIndex": i,
                                                        "endColumnIndex": i + 1
                                                    }
                                                ]
                                            }
                                        },
                                        "targetAxis": "LEFT_AXIS"
                                    } for i in range(1, 6)  # Assuming you have  columns of data
                                ],
                                "headerCount": 1
                            }
                        },
                        "position": {
                            "overlayPosition": {
                                "anchorCell": {
                                    "sheetId": worksheet.id,
                                    "rowIndex": last_data_row_index+2,  # Positioning the chart after the last row of data and empty rows
                                    "columnIndex": 10  # Starting from the first column
                                },
                                "offsetXPixels": 0,  # Adjust as needed
                                "offsetYPixels": 0   # Adjust as needed
                            }
                        }
                    }
                }
            }
        ]
    }

    # Send the request to the API
    response = service.spreadsheets().batchUpdate(spreadsheetId=worksheet.spreadsheet_id, body=requests).execute()

def add_summary_chart(worksheet, startsLineWith, chart_title, columns_to_chart, start_cell="G15", width=600, height=200):
    print('EXECUTE - add_summary_chart')
    last_data_row_index = worksheet.row_count 
    print(f'last_data_row_index={last_data_row_index}')

    column_a_values = worksheet.col_values(1)
    # print(f'column_a_values = {column_a_values}')
    last_data_row_index = None

    # Iterate in reverse to find the last occurrence
    for index, value in reversed(list(enumerate(column_a_values))):
        if value.startswith(startsLineWith):
            last_data_row_index = index + 1  # +1 because enumerate starts at 0
            break
        if startsLineWith=='month':
            last_data_row_index = len(column_a_values)  # +1 because enumerate starts at 0
            break

    print(f'2 - last_data_row_index={last_data_row_index}')

    # Get the index of the columns to chart from the header
    header = worksheet.row_values(1)
    column_indices_to_chart = [header.index(col) for col in columns_to_chart if col in header]

    # Define the chart request body
    requests = {
        "requests": [
            {
                "addChart": {
                    "chart": {
                        "spec": {
                            "title": chart_title,
                            "basicChart": {
                                "chartType": "LINE",
                                "legendPosition": "BOTTOM_LEGEND",
                                "axis": [
                                    {
                                        "position": "BOTTOM_AXIS",
                                        "title": "Weeks"
                                    },
                                    {
                                        "position": "LEFT_AXIS",
                                        "title": "Values"
                                    }
                                ],
                                "domains": [
                                    {
                                        "domain": {
                                            "sourceRange": {
                                                "sources": [
                                                    {
                                                        "sheetId": worksheet.id,
                                                        "startRowIndex": 0,
                                                        "endRowIndex": last_data_row_index,
                                                        "startColumnIndex": 0,  # Weeks column
                                                        "endColumnIndex": 1
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                ],
                                "series": [
                                    {
                                        "series": {
                                            "sourceRange": {
                                                "sources": [
                                                    {
                                                        "sheetId": worksheet.id,
                                                        "startRowIndex": 0,
                                                        "endRowIndex": last_data_row_index,
                                                        "startColumnIndex": col_index,
                                                        "endColumnIndex": col_index + 1
                                                    }
                                                ]
                                            }
                                        },
                                        "targetAxis": "LEFT_AXIS"
                                    } for col_index in column_indices_to_chart
                                ],
                                "headerCount": 1
                            }
                        },
                        "position": {
                            "overlayPosition": {
                                "anchorCell": {
                                    "sheetId": worksheet.id,
                                    "rowIndex": int(start_cell[1:])-1,  # Convert cell row to 0-indexed
                                    "columnIndex": column_letter_to_index(start_cell[0])  # Convert column letter to 0-indexed
                                },
                                "offsetXPixels": 0,
                                "offsetYPixels": 0,
                                "widthPixels": width,
                                "heightPixels": height
                            },
                        }
                    }
                }
            }
        ]
    }
   
   # print(f'column_letter_to_index(start_cell[0])={column_letter_to_index(start_cell[0])} rowIndex={int(start_cell[1:])-1}')

    # Send the request to the API
    response = service.spreadsheets().batchUpdate(spreadsheetId=worksheet.spreadsheet_id, body=requests).execute()

def add_chart_to_sheet(worksheet, chart_title, data_column_letter,
                    chart_place_to_row=15, chart_place_to_col=2):
    last_data_row_index = worksheet.row_count 
    print(f'last_data_row_index={last_data_row_index}')

    # Define the chart request body
    requests = {
        "requests": [
            {
                "addChart": {
                    "chart": {
                        "spec": {
                            "title": chart_title,
                            "basicChart": {
                                "chartType": "LINE",
                                "legendPosition": "BOTTOM_LEGEND",
                                "axis": [
                                    {
                                        "position": "BOTTOM_AXIS",
                                        "title": "Weeks"
                                    },
                                    {
                                        "position": "LEFT_AXIS",
                                        "title": "Values"
                                    }
                                ],
                                "domains": [
                                    {
                                        "domain": {
                                            "sourceRange": {
                                                "sources": [
                                                    {
                                                        "sheetId": worksheet.id,
                                                        "startRowIndex": 1,  # Assuming header is in the first row
                                                        "endRowIndex": last_data_row_index,
                                                        "startColumnIndex": 0,  # Weeks column
                                                        "endColumnIndex": 1
                                                    }
                                                ]
                                            }
                                        }
                                    }
                                ],
                                "series": [
                                    {
                                        "series": {
                                            "sourceRange": {
                                                "sources": [
                                                    {
                                                        "sheetId": worksheet.id,
                                                        "startRowIndex": 1,  # Assuming header is in the first row
                                                        "endRowIndex": last_data_row_index,
                                                        "startColumnIndex": column_letter_to_index(data_column_letter)-1,
                                                        "endColumnIndex": column_letter_to_index(data_column_letter)
                                                    }
                                                ]
                                            }
                                        },
                                        "targetAxis": "LEFT_AXIS"
                                    } 
                                ],
                                "headerCount": 1
                            }
                        },
                        "position": {
                            "overlayPosition": {
                                "anchorCell": {
                                    "sheetId": worksheet.id,
                                    "rowIndex": chart_place_to_row,  # Positioning the chart after the last row of data and empty rows
                                    "columnIndex": chart_place_to_col  # Starting from the first column
                                },
                                "offsetXPixels": 0,  # Adjust as needed
                                "offsetYPixels": 0   # Adjust as needed
                            }
                        }
                    }
                }
            }
        ]
    }

    # Send the request to the API
    response = service.spreadsheets().batchUpdate(spreadsheetId=worksheet.spreadsheet_id, body=requests).execute()
    return response  # Return the API response for further processing if necessary

def color_rows_in_export(sheet,row_format_updates):
    requests = []
    for row_index, color in row_format_updates:
    # Convert color to Google Sheets API format if necessary
        gs_color = {"red": color["red"], "green": color["green"], "blue": color["blue"]}
        requests.append({
        "updateCells": {
            "range": {
                "sheetId": sheet.id,  # You need to know the sheet ID
                "startRowIndex": row_index-1,
                "endRowIndex": row_index,
                "startColumnIndex": 0,
                "endColumnIndex": 1,  # Adjust if you want to format more than one column
            },
            "rows": [
                {
                    "values": [
                        {
                            "userEnteredFormat": {
                                "backgroundColor": gs_color,
                            }
                        }
                    ]
                }
            ],
            "fields": "userEnteredFormat.backgroundColor",
        }
    })

    body = {
    "requests": requests
    }

    response = service.spreadsheets().batchUpdate(
    spreadsheetId=sheet.spreadsheet_id,
    body=body
    ).execute()

def sortSheetByDateFromCol(sheet, col='A', order = 'ASCENDING'):
    print(f'{sheet.id} + {sheet.spreadsheet_id} + column_letter_to_index(col)={column_letter_to_index(col)}')
    requests = []
    requests.append({
                "sortRange": {
                        "range": {
                                "sheetId": sheet.id, # You need to provide the specific ID of the sheet you want to sort
                                "startRowIndex": 2, # Row index starts at 0, so 1 means starting from row 2
                                "startColumnIndex": column_letter_to_index(col)-1, # Column A
                                "endColumnIndex": column_letter_to_index(col) # Only sorting by Column A, so start and end ColumnIndex is 0 and 1 respectively
                                },
                        "sortSpecs": [
                            {
                                "dimensionIndex": 0, # Sorting by the first column (Column A)
                                "sortOrder": order # Sorting in ascending order
                            }
                        ]
                    }
    })
    body = {
    "requests": requests
    }

    response = service.spreadsheets().batchUpdate(
    spreadsheetId=sheet.spreadsheet_id,
    body=body
    ).execute()










