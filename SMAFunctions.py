from dateutil import parser
import pandas as pd
import time
from datetime import datetime, timedelta
from gspread.utils import column_letter_to_index
import gspread
from gspread.exceptions import WorksheetNotFound
import pandas as pd
from SMAGoogleAPICalls import (add_chart_to_sheet, add_summary_chart, clear_sheet_formatting_and_content, add_left_right_borders_to_columns,
                                add_borders_to_cells_only_allRows, add_up_down_borders_to_rows, color_rows_in_export, sortSheetByDateFromCol,
                                group_rows, column_width)
from SMA_Constants import (FB_CAMPAIGNS, GOOGLE_CAMPAIGNS, commonExportedCampaignsSheet, TOTAL_TOTAL_COL,FB_TOTAL_COL,GOOGLE_TOTAL_COL,
                           INTERIM_SHEET_DATA, column_index_to_string)

# Helper function to standardize date format, e.g., '2024-01-31'
def standardize_date(date_str):
    try:
        # This assumes the date format in the sheet is like '2024-01-31'
        # Adjust the date format if it's different
        return datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
    except ValueError as e:
        print(f"Error parsing date: {e}")
        return None

def find_insert_position(sorted_dates, new_date, delta=INTERIM_SHEET_DATA):
    """Find the correct position to insert a new date in a sorted list of dates."""
    for i, current_date in enumerate(sorted_dates):
        if new_date < current_date:
            return i+1+delta  # +2 to adjust for 1-based indexing and header row in Google Sheets
    return len(sorted_dates) + 1+delta # If new_date is latest, insert at the end

def fillInterimCampaignsDataColumn(interim, toexport):
    print('EXECUTE - fillInterimCampaignsDataColumn')
    toexport_dates =  toexport.col_values(1)[1:]
    interim_dates = interim.col_values(1)[INTERIM_SHEET_DATA:] 

    # Convert the interim dates to string format for comparison
    toexport_dates  = sorted([standardize_date(date) for date in toexport_dates])
    sorted_interim_dates  = sorted([standardize_date(date) for date in interim_dates])
    # print(f'interim_dates_str={sorted_interim_dates }')

    latest_date = max(sorted_interim_dates + toexport_dates)
    earliest_date = min(sorted_interim_dates + toexport_dates)
    print(f'earliest_date={earliest_date}            latest_date={latest_date}\n')

    # Generate all dates from earliest to latest
    dt=[earliest_date + timedelta(days=i) for i in range((latest_date - earliest_date).days + 1)]
    all_dates = reversed(dt)
    print(f'alldates={dt}')

    # Determine the number of rows to add if there aren't enough
    rows_to_add = len(dt) - interim.row_count + INTERIM_SHEET_DATA - 1  # Subtract the number of existing rows and adjust for the header
    need_to_delete_Latest=False
    if rows_to_add>0:
        interim.append_row(['']*1)
        need_to_delete_Latest=True

    # Find and insert missing dates
    for date in all_dates:
        if date not in sorted_interim_dates :
            formatted_date = date.strftime("%Y-%m-%d")
            # Find the correct position (row index) to insert the new date
            # Determine where to insert the new date
            position_to_insert = find_insert_position(interim_dates, formatted_date)
            print(f"Inserting missed date: {formatted_date}, position_to_insert={position_to_insert}")
            interim.insert_row([formatted_date], position_to_insert)
            time.sleep(2)
            # Note: This example inserts at the end. You might want to adjust the insertion logic.
            # Update the sorted_interim_dates list to include the newly added date
            sorted_interim_dates.append(date)
            sorted_interim_dates.sort()  # Ensure the list is sorted after insertion
    if need_to_delete_Latest:
        try:
            interim.delete_rows(interim.row_count)
        except ValueError as e:
            print(f'Last row deletion Error {e}')

    sortSheetByDateFromCol(interim)

def update_sheet_headers(worksheet, replacements):
    first_row_values = worksheet.row_values(1)
    new_first_row_values = [replacements.get(value, value) for value in first_row_values]

    if first_row_values != new_first_row_values:
        worksheet.update('A1', [new_first_row_values])

def column_letter_to_index(col_letter):
    index = 0
    for char in col_letter:
        index = index * 26 + (ord(char.upper()) - ord('A') + 1)
    return index

def pauseMe(x=0):
    print(f"\n {x} - Press Enter to continue... \n")
    input()

def step1_commonCampaignSheetCreate(spreadsheet):
    FB_data_exp_sheet = spreadsheet.worksheet('Facebook Export Data')
    GOOGLE_data_exp_sheet = spreadsheet.worksheet('Google Export Data')
    print(f'LINE 36 !')

    # Prepare a dictionary of terms to replace
    replacements_FB = {'Amount spent': 'Ad Spend', 'Contacts (website)': 'Total Leads'}
    replacements_GOOGLE = {'Campaign name': 'AdSet name', 'Amount spent': 'Ad Spend'}

    # Update headers if necessary
    update_sheet_headers(FB_data_exp_sheet, replacements_FB)
    update_sheet_headers(GOOGLE_data_exp_sheet, replacements_GOOGLE)

    print(f'LINE 65 !')
    # Fetch data into DataFrame
    data_fb = pd.DataFrame(FB_data_exp_sheet.get_all_records())
    data_google = pd.DataFrame(GOOGLE_data_exp_sheet.get_all_records())
    data=pd.concat([data_fb, data_google], ignore_index=True)

    # Convert numerical columns to float and fill missing values
    numeric_columns = ['Ad Spend', 'Total Leads',
                    'Post comments', 'Impressions']
    for column in numeric_columns:
        data[column] = pd.to_numeric(data[column], errors='coerce').fillna(0)

    # Process data
    consolidated_data = []
    # Function to determine the campaign based on AdSet name
    def determine_campaign(adset_name):
        for campaign in FB_CAMPAIGNS:
            if campaign in adset_name:
                return campaign
        for campaign in GOOGLE_CAMPAIGNS:
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
        campaign_exp_sheet = spreadsheet.worksheet(commonExportedCampaignsSheet)
    except gspread.exceptions.WorksheetNotFound:
        # If not found, create a new worksheet

        campaign_exp_sheet = spreadsheet.add_worksheet(
            title=commonExportedCampaignsSheet, rows="100", cols="20")
    # Write the data to the new sheet
    campaign_exp_sheet = spreadsheet.worksheet(commonExportedCampaignsSheet)
    campaign_exp_sheet.clear()  # Clear existing data
    campaign_exp_sheet.update( values=final_data_list, range_name='A1')  # Update with new data

    print(f'LINE 116 Finish FUNCTION STEP - 1  \n')
    pauseMe("Step-1 finish")

def step1_v2_commonCampaignSheetCreate(spreadsheet):
    print(f'\n Line 144 -  Start step1_v2 function execution \n\n')
    # Pre-fetch worksheets to reduce the number of API calls
    try:
        FB_data_exp_sheet = spreadsheet.worksheet('Facebook Export Data')
        data_fb = pd.DataFrame(FB_data_exp_sheet.get_all_records())
    except WorksheetNotFound:
        print("Facebook Export Data sheet not found.")
        pauseMe("Facebook Export Data sheet not found.")
        data_fb = pd.DataFrame()

    try:
        GOOGLE_data_exp_sheet = spreadsheet.worksheet('Google Export Data')
        data_google = pd.DataFrame(GOOGLE_data_exp_sheet.get_all_records())
    except WorksheetNotFound:
        print("Google Export Data sheet not found.")
        pauseMe('Google export to read')
        data_google = pd.DataFrame()

    # No data fetched scenario
    if data_fb.empty and data_google.empty:
        print("No data to process.")
        return
    print(f'data_fb.size ={data_fb.shape}   data_google={data_google.shape}     ')

    # Define replacements in a more streamlined way
    replacements = {
        'Facebook Export Data': {'Amount spent': 'Ad Spend', 'Contacts (website)': 'Total Leads'},
        'Google Export Data': { 'Campaign name': 'AdSet name','Amount spent': 'Ad Spend'}
    }

    # Apply replacements and concat data
    for sheet, df in [('Facebook Export Data', data_fb), ('Google Export Data', data_google)]:
        for old, new in replacements[sheet].items():
            if old in df.columns:
                df.rename(columns={old: new}, inplace=True)

    data = pd.concat([data_fb, data_google], ignore_index=True)
    print(f'concatenated data={data.shape}')
    # Convert numerical columns to float and fill missing values
    data[['Ad Spend', 'Total Leads', 'Post comments', 'Impressions']] = data[['Ad Spend', 'Total Leads', 'Post comments', 'Impressions']].apply(pd.to_numeric, errors='coerce').fillna(0)

    # Simplify the determine_campaign function and apply it
    campaigns = {'fb_campaigns': FB_CAMPAIGNS, 'google_campaigns': GOOGLE_CAMPAIGNS}  # Assuming fb_campaigns and google_campaigns are defined elsewhere

    def determine_campaign(adset_name):
        for source, campaign_list in campaigns.items():
            for campaign in campaign_list:
                if campaign in adset_name:
                    return campaign
        return 'Other'

    data['Determined Campaign'] = data['AdSet name'].apply(determine_campaign)
    # Now, replace all instances of 'Brand Protect | MCPC' with 'BAU | Brand' in the 'Determined Campaign' column
    data['Determined Campaign'] = data['Determined Campaign'].replace('Brand Protect | MCPC', 'BAU | Brand')

    # Aggregate data
    grouped = data.groupby(['Date', 'Determined Campaign']).sum().reset_index()
    grouped = grouped.drop(columns=['AdSet name'])

    # Write to 'campaign_exp_sheet'
    sheet_title = commonExportedCampaignsSheet
    try:
        campaign_exp_sheet = spreadsheet.worksheet(sheet_title)
        spreadsheet.del_worksheet(campaign_exp_sheet)
    except WorksheetNotFound:
        pass
    campaign_exp_sheet = spreadsheet.add_worksheet(title=sheet_title, rows="2000", cols="20")

    campaign_exp_sheet.update(values=[grouped.columns.values.tolist()] + grouped.values.tolist(), range_name='A1' , value_input_option='USER_ENTERED')
    print(f'LINE 116 Finish FUNCTION STEP - 1  \n')
    # pauseMe("Step-1 finish")

# Iterate over each row in 'campaign_exp_data'
def step2_iterateExport_OLD(campaign_exp_sheet, interim_campaigns_sheet):
    # Now, retrieve the data from the sheet and store it in the 'campaign_exp_data' DataFrame
    campaign_exp_data = pd.DataFrame(campaign_exp_sheet.get_all_records())
    # Define colors for success and failure
    success_color = {"red": 0.85, "green": 0.93, "blue": 0.83}  # Light green
    failure_color = {"red": 0.96, "green": 0.80, "blue": 0.80}  # Light red

    # Retrieve headers to find the correct column for each campaign metric
    header_row = interim_campaigns_sheet.row_values(1)  # Campaign names

    # Assuming the date column is the first one in 'InterimFB'
    dates_column = interim_campaigns_sheet.col_values(1)[INTERIM_SHEET_DATA:]  # Start from row 3 to skip header rows

    for index, row in campaign_exp_data.iterrows():
        # Find the row number for the current date in 'InterimFB'
        date = row['Date']
        if date in dates_column:
            date_row_number = dates_column.index(date) + 3  # Offset for header rows
        else:
            # Add new row at the end with the date
            date_row_number = len(dates_column) + 3  # New row number
            interim_campaigns_sheet.append_row([date])  # Append the new date
            dates_column.append(date)  # Update local date list

        # Determine the campaign from the 'AdSet name'
        determined_campaign = row['Determined Campaign'] 
        print(f'\nDATE={date}   determined_campaign={determined_campaign}  date_row_number={date_row_number}\n ============')
        if determined_campaign == "Other":
            print(f'determine_campaign == "Other" ? {determined_campaign == "Other"}')
            campaign_exp_sheet.format(f"{index + 2}:{index + 2}", {"backgroundColor": failure_color})
            continue
        if not determined_campaign:
            # If no matching campaign is found, skip this iteration
            campaign_exp_sheet.format(f"{index + 2}:{index + 2}", {"backgroundColor": failure_color})
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
        percent_of_spend_formula = f"=IFERROR({adspend_cell}/B{date_row_number},0)"

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
        interim_campaigns_sheet.update(values=[update_values], range_name=f'{start_cell}:{end_cell}', value_input_option='USER_ENTERED')
        # Mark as successfull export
        campaign_exp_sheet.format(f"{index + 2}:{index + 2}", {"backgroundColor": success_color})
        time.sleep(1)

def step2_iterateExport(campaign_exp_sheet, interim_campaigns_sheet):
    print('EXECUTE - step2_iterateExport')
    campaign_exp_data = pd.DataFrame(campaign_exp_sheet.get_all_records())
    success_color = {"red": 0.85, "green": 0.93, "blue": 0.83}  # Light green
    failure_color = {"red": 0.96, "green": 0.80, "blue": 0.80}  # Light red

    header_row = interim_campaigns_sheet.row_values(1)
    dates_column = interim_campaigns_sheet.col_values(1)[2:]

    # Prepare a list to hold all updates for batch processing
    batch_updates = []
    row_format_updates = []

    for index, row in campaign_exp_data.iterrows():
        date = row['Date']
        print(f'{date}')
        if date in dates_column:
            date_row_number = dates_column.index(date) + 3
        else:
            date_row_number = len(dates_column) + 3
            interim_campaigns_sheet.append_row([date])
            time.sleep(1)
            dates_column.append(date)

        determined_campaign = row['Determined Campaign']
        if determined_campaign == "Other" or not determined_campaign:
            row_format_updates.append((index + 2, failure_color))
            continue

        start_column_index = header_row.index(determined_campaign) + 1
        update_range_start = gspread.utils.rowcol_to_a1(date_row_number, start_column_index)
        update_range_end = gspread.utils.rowcol_to_a1(date_row_number, start_column_index + 6)

        update_values = [
            row['Ad Spend'],
            row['Impressions'] if row['Impressions'] else 0,
            row['Total Leads'] if row['Total Leads'] else 0,
            row['Post comments'] if row['Post comments'] else 0,
            f"=IFERROR({update_range_start}/{gspread.utils.rowcol_to_a1(date_row_number, start_column_index + 2)},0)",  # total_cpl_formula
            f"=IFERROR({update_range_start}/{gspread.utils.rowcol_to_a1(date_row_number, start_column_index + 3)},0)",  # cp_comments_formula
            f"=IFERROR({update_range_start}/B{date_row_number},0)"  # percent_of_spend_formula
        ]

        # Collect update data for batch processing
        batch_updates.append({
            "range": f"{update_range_start}:{update_range_end}",
            "values": [update_values]
        })
        row_format_updates.append((index + 2, success_color))
        # time.sleep(1)  # Consider the necessity of this sleep in the context of batch updates
    print('Batch updates')
    print(f'{batch_updates}')
    # Perform batch update for all collected data
    interim_campaigns_sheet.batch_update(batch_updates, value_input_option='USER_ENTERED')
    time.sleep(2)
    # Apply formatting updates separately if necessary
    # for row_index, color in row_format_updates:
    #     campaign_exp_sheet.format(f"{row_index}:{row_index}", {"backgroundColor": color})
    #     time.sleep(1)
    
    # color_rows_in_export(interim_campaigns_sheet,row_format_updates)


def step_Campaign_totals(interim_campaigns_sheet, start_totals_column, last_column=None):
    print('Calculating campaign totals...')
    # Retrieve all data from the sheet
    all_data = interim_campaigns_sheet.get_all_values()

    # Calculate the starting index
    start_index = column_letter_to_index(start_totals_column)+6
    
    # Prepare the list to hold all row updates
    rows_to_update = []

    # If last_column is provided, use it; otherwise, find dynamically
    if last_column:
        end_index = column_letter_to_index(last_column)   # Adjust to include the last_column in the range
    else:
        # Dynamically determining the last column can be tricky if not explicitly provided,
        # as interim_campaigns_sheet.col_count might give a larger number than expected.
        # A safer approach might involve inspecting the last row's length in all_data, but this requires assumptions about the data's consistency.
        end_index = len(all_data[0])  # Assuming the first row (headers) spans all relevant columns

    # Loop through each row of data (skipping headers)
    for i, row in enumerate(all_data[2:], start=3):  # Skip header rows
        # Initialize formula parts for aggregation
        ad_spend_parts = []
        impressions_parts = []
        leads_parts = []
        comments_parts = []

        # Aggregate formulas for each metric within the specified columns range
        for col_offset in range(start_index, end_index, 7):
            ad_spend_parts.append(f'{column_index_to_string(col_offset)}{i}')
            impressions_parts.append(f'{column_index_to_string(col_offset + 1)}{i}')
            leads_parts.append(f'{column_index_to_string(col_offset + 2)}{i}')
            comments_parts.append(f'{column_index_to_string(col_offset + 3)}{i}')

        # Construct the final formulas
        ad_spend_formula = "+".join(ad_spend_parts)
        impressions_formula = "+".join(impressions_parts)
        leads_formula = "+".join(leads_parts)
        comments_formula = "+".join(comments_parts)

        cpl_formula = f"=IF({leads_formula}<>0,({ad_spend_formula})/({leads_formula}),0)"
        cp_comments_formula = f"=IF({comments_formula}<>0,({ad_spend_formula})/({comments_formula}),0)"

        # Add the row update to the batch list
        rows_to_update.append([
            f"=SUM({ad_spend_formula})", 
            f"=SUM({impressions_formula})", 
            f"=SUM({leads_formula})", 
            f"=SUM({comments_formula})", 
            cpl_formula, 
            cp_comments_formula
        ])

    # Calculate the update range
    update_range = f"{start_totals_column}3:{column_index_to_string(start_index + 5)}{len(all_data) + 1}"
    # Batch update the sheet with all calculated formulas at once
    interim_campaigns_sheet.update(values=rows_to_update, range_name=update_range,  value_input_option='USER_ENTERED')

def step2_Totals_Calc(interim_campaigns_sheet, total_col=TOTAL_TOTAL_COL, fb_col=FB_TOTAL_COL, google_col=GOOGLE_TOTAL_COL):
    print('step2 Totals_Calc filling')
    # Retrieve all data from the sheet
    all_data = interim_campaigns_sheet.get_all_values()
    
    # Calculate the column indices based on the provided column letters
    fb_col_index = column_letter_to_index(fb_col)  # Assuming this function converts column letter to zero-based index
    google_col_index = column_letter_to_index(google_col)
    total_col_index = column_letter_to_index(total_col)

    # Prepare the range and values for batch update
    range_name = f'{total_col}3:{column_index_to_string(total_col_index+5)}{len(all_data)+1}'
    values = []

    for i, row in enumerate(all_data[2:], start=3):  # Skip header rows
        # Prepare formulas for each column
        total_ad_formula = f"={fb_col}{i}+{google_col}{i}"
        total_impressions_formula = f"={column_index_to_string(fb_col_index+1)}{i}+{column_index_to_string(google_col_index+1)}{i}"
        total_leads_formula = f"={column_index_to_string(fb_col_index+2)}{i}+{column_index_to_string(google_col_index+2)}{i}"
        total_comments_formula = f"=IF(D{i}<>0,{TOTAL_TOTAL_COL}{i}/D{i},0)"
        totalFBPercentOfSpend =  f"=IF(B{i}<>0,{FB_TOTAL_COL}{i}/B{i},0)"
        totalGooglePercentOfSpend = f"=IF(B{i}<>0,{GOOGLE_TOTAL_COL}{i}/B{i},0)"
        
        # Add the prepared row to the values list
        values.append([
            total_ad_formula,
            total_impressions_formula,
            total_leads_formula,
            total_comments_formula,
            totalFBPercentOfSpend,
            totalGooglePercentOfSpend
        ])

    # Perform a batch update for the prepared range and values
    interim_campaigns_sheet.update(values=values, range_name=range_name,  value_input_option='USER_ENTERED')

def copy_Headers(interim_campaigns_sheet, total_sheet):
    # Fetch the first two rows (headers) from 'interim_campaigns_sheet'
    headers = interim_campaigns_sheet.get('A1:DF2')

    # Update the 'total_sheet' with the headers
    total_sheet.update(values=headers, range_name='A1',  value_input_option='USER_ENTERED')

def format_dates_in_column_a(total_sheet):
    # Define the number format pattern for dates
    # This pattern corresponds to "Sunday, January 21, 2024"
    date_format_pattern = 'dddd, mmmm dd, yyyy'

    # Calculate the range to apply the date format
    # Assuming you want to format all cells starting from row 3 down to the last row with data
    number_of_rows = len(total_sheet.get_all_values())
    date_format_range = f'A3:A{number_of_rows}'

    # Apply the date format to the range
    total_sheet.format(date_format_range, {
        "numberFormat": {
            "type": "DATE",
            "pattern": date_format_pattern
        }
    })

def insert_week_and_month_totals(total_sheet):
    # Fetch all the dates from column 'A', starting from row 3 to skip headers
    dates = total_sheet.col_values(1)[INTERIM_SHEET_DATA:]

    # Keep track of how many rows have been inserted to adjust row indices accordingly
    inserted_rows_count = 0

    column_width(total_sheet, 'A','B', 220)

    # Initialize the previous week number to None for comparison
    previous_week_number = None
    current_year = None
    dates_pairs = zip(dates, dates[1:] + [None]) # Ensure the last date handles end of data
    start_group_row_weeks=3
    start_group_row_month=3

    for i, (date_str, next_date_str) in enumerate(dates_pairs, start=start_group_row_weeks):
        if date_str:  # Ensure the date string is not empty
            # Convert the string to a datetime object
            date_obj = datetime.strptime(date_str, '%A, %B %d, %Y')
            current_week_number = date_obj.isocalendar()[1]
            month = date_obj.month
            year = date_obj.year  # Track the year of the current date

            # Check for year change
            if current_year is not None and year != current_year:
                # Reset the previous week number when the year changes
                previous_week_number = None

            is_last_day_of_month = False
            # Determine if it's the last day of the month
            if next_date_str:
                next_day = datetime.strptime(next_date_str, '%A, %B %d, %Y')
                is_last_day_of_month = next_day.month != month
            else:  # This means it's the last date in the list
                is_last_day_of_month = True

            print(f'-->  month={month} nextDay={next_day} is_last_day_of_month={is_last_day_of_month}  previous_week_number={previous_week_number} \n')

            # Determine if the week number has changed (indicating a new week) or it's the last day of the month
            if current_week_number != previous_week_number or is_last_day_of_month:
                # Update the row index to account for previously inserted rows
                adjusted_row_index = i + inserted_rows_count
                print(f'-->  adjusted_row_index={adjusted_row_index} \n')

                # Insert a row for the current week number if it has changed
                if (current_week_number != previous_week_number and previous_week_number is not None) and is_last_day_of_month==False:
                    print(f'(!) Condition 1 \n')
                    total_sheet.insert_row(["Week - " + str(previous_week_number)+", "+str(year)], adjusted_row_index)
                    
                    group_rows(total_sheet, start_group_row_weeks, adjusted_row_index-1)

                    print(f'group_rows .... start_group_row={start_group_row_weeks} adjusted_row_index={adjusted_row_index} ')
                    
                    start_group_row_weeks=adjusted_row_index+1
                    
                    time.sleep(2)
                    
                    # Insert the SUM formula for column B and replicate it across the row
                    
                    colB_Week_Sum(adjusted_row_index,  total_sheet)
                    time.sleep(1)
                    inserted_rows_count += 1  # Update the count of inserted rows
                    adjusted_row_index += 1  # Adjust the row index for possible next insertion

                # If it's the last day of the month, insert a row for month totals
                if is_last_day_of_month:
                    print(f' Last day of Month !   (!) Condition 2 \n')
                    total_sheet.insert_row(["Week - " + str(current_week_number)+", "+str(year)], adjusted_row_index+1)
                    group_rows(total_sheet, start_group_row_weeks, adjusted_row_index)
                    start_group_row_weeks=adjusted_row_index+3
                    time.sleep(1)
                    # Insert the SUM formula for column B and replicate it across the row
                    colB_Week_Sum(adjusted_row_index+1,  total_sheet)
                    inserted_rows_count += 1  # Update the count of inserted rows
                    adjusted_row_index += 1  # Adjust the row index for possible next insertion
                    month_name = date_obj.strftime('%B')
                    time.sleep(1)
                    total_sheet.insert_row(["TOTAL " + month_name+", "+str(year)], adjusted_row_index+1)

                    group_rows(total_sheet, start_group_row_month, adjusted_row_index)
                    start_group_row_month=adjusted_row_index+2
                    time.sleep(1)
                    colB_Month_Sum(adjusted_row_index+1, total_sheet)
                    inserted_rows_count += 1  # Update the count of inserted rows

            # Update the previous week number for the next iteration
            previous_week_number = current_week_number
            current_year = year


def colB_Month_Sum(row_index, mysheet, FB_Summary_COL='J', GOOGLE_Summary_COL='BT'):
    print('colB_Month_Sum START')
    end_row = row_index - 1
    # Fetch the entire first column at once
    first_col_values = mysheet.col_values(1)

    # Find the start index
    idx = row_index - 1  # Adjusted for zero-based indexing in Python
    while idx > 2 and not (first_col_values[idx-1].startswith('TOTAL') or first_col_values[idx-1].startswith('Date')):
        idx -= 1
    idx=idx+1
    start_row=idx
    sum_formulas = []
    special_formulas = {
    'H': f"=IF(B{row_index}<>0,{FB_Summary_COL}{row_index}/B{row_index},0)",
    'I': f"=IF(B{row_index}<>0,{GOOGLE_Summary_COL}{row_index}/B{row_index},0)",
    'G': f"=IF(F{row_index}<>0,B{row_index}/F{row_index},0)",
    'E': f"=IF(D{row_index}<>0,B{row_index}/D{row_index},0)"
        } 
    # Generate the formulas for the all columns starts from B
    for col_index in range(2, mysheet.col_count ):  # Assuming total_sheet.col_count gives the number of columns
        col_letter = column_index_to_string(col_index) # Convert column index to letter
        if col_letter in special_formulas:
            sum_formula = special_formulas[col_letter]
        else:
            sum_formula = f"=SUM({col_letter}{start_row}:{col_letter}{end_row})/2"
        sum_formulas.append(sum_formula)
        # if col_letter=='H' or col_letter=="I":
        #     myCol= FB_Summary_COL if col_letter=='H' else GOOGLE_Summary_COL
        #     sum_formula=f"=IF(B{row_index}<>0,{myCol}{row_index}/B{row_index},0)"
        # else:
        #     sum_formula = f"=SUM({col_letter}{idx}:{col_letter}{row_index-1})/2"
        # sum_formulas.append(sum_formula)

    range_to_update = f"B{row_index}:{column_index_to_string(mysheet.col_count - 1)}{row_index}"
    mysheet.update(values=[sum_formulas],range_name=range_to_update, value_input_option='USER_ENTERED' )
    time.sleep(1)
    format_row_as_lightgrey_and_bold(mysheet, row_index, 0.7,1.0,0.7)
    time.sleep(2)
    add_up_down_borders_to_rows(mysheet, row_index, row_index, 2)

def colB_Week_Sum(adjusted_row_index, total_sheet, FB_Summary_COL='J', GOOGLE_Summary_COL='BT'):
    end_row = adjusted_row_index - 1
    start_row = identify_non_numerical_cell_in_column_B(end_row, total_sheet)
    sum_formulas = []
    print(f' LINE 636 total_sheet.col_count ={total_sheet.col_count}')
    special_formulas = {
    'H': f"=IF(B{end_row+1}<>0,{FB_Summary_COL}{end_row+1}/B{end_row+1},0)",
    'I': f"=IF(B{end_row+1}<>0,{GOOGLE_Summary_COL}{end_row+1}/B{end_row+1},0)",
    'G': f"=IF(F{end_row+1}<>0,B{end_row+1}/F{end_row+1},0)",
    'E': f"=IF(D{end_row+1}<>0,B{end_row+1}/D{end_row+1},0)"
        } 
    # Fetch the entire second row once
    second_row_values = total_sheet.row_values(2)  # This assumes you have a method like `row_values` to fetch an entire row

    # Generate the formulas for the remaining columns
    for col_index in range(2, total_sheet.col_count ):  # Assuming total_sheet.col_count gives the number of columns
        col_letter = column_index_to_string(col_index) #string.ascii_uppercase[col_index - 1]  # Convert column index to letter
        cell_value = second_row_values[col_index - 1] 
        if col_letter in special_formulas:
            sum_formula = special_formulas[col_letter]
        elif '% of Spend' in cell_value:
            letter_to_use=FB_Summary_COL if col_index<column_letter_to_index(GOOGLE_Summary_COL) else GOOGLE_Summary_COL
            sum_formula =f"=IF(B{end_row+1}<>0,{column_index_to_string(col_index-6)}{end_row+1}/{letter_to_use}{end_row+1},0)"
        else:
            sum_formula = f"=SUM({col_letter}{start_row+1}:{col_letter}{end_row})"
        sum_formulas.append(sum_formula)

    # Update the entire row with sum formulas in a single API call
    # print(f' total_sheet.col_count - {total_sheet.col_count}')
    range_to_update = f"B{adjusted_row_index}:{column_index_to_string(total_sheet.col_count - 1)}{adjusted_row_index}"
    total_sheet.update(values=[sum_formulas],range_name=range_to_update, value_input_option='USER_ENTERED' )
    time.sleep(2)
    format_row_as_lightgrey_and_bold(total_sheet, adjusted_row_index)
    time.sleep(2)
    add_up_down_borders_to_rows(total_sheet, adjusted_row_index, adjusted_row_index, 1)

def format_row_as_lightgrey_and_bold(total_sheet, adjusted_row_index, r=.9, g=.9, b=.9 , alpha=1):
    print(f' COLOR ! - {adjusted_row_index} -{ column_index_to_string(total_sheet.col_count-1)}')
    # Define the range for the entire row
    row_range = f"A{adjusted_row_index}:{column_index_to_string(total_sheet.col_count-1)}{adjusted_row_index}"
    cell_format = {
        "backgroundColor": {
            "red": r,
            "green": g,
            "blue": b,
            "alpha": alpha
        },
        "textFormat": {
            "bold": True
        }
    }
    time.sleep(1)
    total_sheet.format(row_range, cell_format)

def identify_non_numerical_cell_in_column_B(end_row,  mysheet=None):
    start_row=2
    for row in range(end_row, 3, -1):
        cell_value = mysheet.cell(row, 1).value  # Assuming column B is column 2
        if cell_value.startswith('Week') or  cell_value.startswith('TOTAL') or cell_value=='' or cell_value=='Date':
            return row        
    return start_row

def restructure_to_weekly(interim_campaigns_sheet,spreadsheet, sheet_name):
    print(f'restructure_to_weekly')
    try:
        # Try to open the worksheet by title
        to_del=spreadsheet.worksheet(sheet_name)
        spreadsheet.del_worksheet(to_del)
    except gspread.WorksheetNotFound:
        pass
    
    new_worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="20")
    print(f"New worksheet created: {type(new_worksheet)} with ID {new_worksheet.id}")  # Debug print

    # Debugging: Print existing worksheet IDs
    existing_ids = [ws.id for ws in spreadsheet.worksheets()]
    print(f"Existing worksheet IDs before reordering: {existing_ids}")

    # Move the newly created sheet to the first position
    # try:
    #     spreadsheet.reorder_worksheets([new_worksheet.id] + [ws.id for ws in spreadsheet.worksheets() if ws.id != new_worksheet.id])
    # except Exception as e:
    #     print(f"Error during reordering: {e}")

    # Fetch the data from 'interim_campaigns_sheet'
    interim_data = interim_campaigns_sheet.get_all_values()
    # print(f'SHEET ID={new_worksheet.id}')

    # Clear the total sheet before updating it with new data
    clear_sheet_formatting_and_content(new_worksheet)
    time.sleep(1)
    copy_Headers(interim_campaigns_sheet, new_worksheet)
    time.sleep(1)
    # # Update the total sheet with the data from the interim sheet
    new_worksheet.update(range_name='A1', values=interim_data, value_input_option='USER_ENTERED')
    time.sleep(1)
    new_worksheet = spreadsheet.worksheet(sheet_name)
    internal_leads_sheet=spreadsheet.worksheet('Internal Leads')
    add_internal_leads(new_worksheet,internal_leads_sheet)
    new_worksheet = spreadsheet.worksheet(sheet_name)
    format_dates_in_column_a(new_worksheet)
    time.sleep(2)
    add_borders_to_cells_only_allRows(new_worksheet, 1,new_worksheet.col_count)
    time.sleep(2)
    insert_week_and_month_totals(new_worksheet)
    time.sleep(2)
    merge_non_empty_columns_in_first_row(new_worksheet)   
    format_row_as_lightgrey_and_bold(new_worksheet, 1, 0.0,0.9,1.0)
    new_worksheet.freeze(rows=2)

def add_internal_leads(total_campaigns_sheet, internal_leads_sheet):
    HEADER_ROW_COLUMN_NAMES=2
    print('Execute add_internal_leads')
    headers = total_campaigns_sheet.row_values(HEADER_ROW_COLUMN_NAMES)

    # Find the index of the 'Total CPL' column
    try:
        total_cpl_col_idx = headers.index('Total CPL') + 1
    except ValueError as e:
        raise ValueError("The 'Total CPL' column was not found in the second row. Please check the sheet to ensure this column exists.") from e
    
    # Fetch the values of 'Total Leads' and 'Total CPL' columns
    total_leads_values = total_campaigns_sheet.col_values(total_cpl_col_idx-1, value_render_option='FORMULA')
    total_cpl_values = total_campaigns_sheet.col_values(total_cpl_col_idx, value_render_option='FORMULA')
    
   # Insert two new columns after 'Total CPL'
    new_columns_values = [[''] * len(total_leads_values), [''] * len(total_cpl_values)]
    total_campaigns_sheet.insert_cols(new_columns_values, total_cpl_col_idx + 1)

    internal_leads_col_idx=total_cpl_col_idx+1
    internal_leads_cpl_col_idx=internal_leads_col_idx+1

    # Update the values in the newly inserted 'Internal Leads' column with the values from 'Total Leads'
    internal_leads_range = f'{gspread.utils.rowcol_to_a1(1, internal_leads_col_idx)}:{gspread.utils.rowcol_to_a1(len(total_leads_values) + 2, internal_leads_col_idx)}'
    total_campaigns_sheet.update(range_name=internal_leads_range, values=[[value] for value in total_leads_values], value_input_option='USER_ENTERED')

    # Update the values in the newly inserted 'Internal Leads CPL' column with the values from 'Total CPL'
    internal_leads_cpl_range = f'{gspread.utils.rowcol_to_a1(1, internal_leads_cpl_col_idx)}:{gspread.utils.rowcol_to_a1(len(total_cpl_values) + 2, internal_leads_cpl_col_idx)}'
    total_campaigns_sheet.update(range_name=internal_leads_cpl_range, values=[[value] for value in total_cpl_values], value_input_option='USER_ENTERED')
    
    # Rename the new columns
    total_campaigns_sheet.update_cell(HEADER_ROW_COLUMN_NAMES, internal_leads_col_idx, 'Internal Leads')
    total_campaigns_sheet.update_cell(HEADER_ROW_COLUMN_NAMES, internal_leads_cpl_col_idx, 'Internal leads CPL')
    
    # Create a mapping of dates to internal leads from the 'Internal Leads' sheet
    internal_leads_data = internal_leads_sheet.get_all_records()
    # Change the date format in the dictionary comprehension
    internal_leads_dict = {datetime.strptime(row['Date'], '%Y-%m-%d'): row['Number of internal leads'] for row in internal_leads_data}

    # Get all dates from the 'TOTAL_CAMPAIGNS_SHEET_NAME' sheet
    dates = total_campaigns_sheet.col_values(1)[HEADER_ROW_COLUMN_NAMES:]

    # Prepare the data for batch updating the 'Internal Leads' and 'Internal Leads CPL' columns
    internal_leads_updates = []
    internal_leads_cpl_updates = []
    for i, date_str in enumerate(dates, start=HEADER_ROW_COLUMN_NAMES+1):
        if date_str:
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            internal_leads_value = internal_leads_dict.get(date_obj, 0)
            internal_leads_updates.append([internal_leads_value])
            
            formula = f'=IF({gspread.utils.rowcol_to_a1(i, internal_leads_col_idx)}<>0,B{i}/{gspread.utils.rowcol_to_a1(i, internal_leads_col_idx)},0)'
            internal_leads_cpl_updates.append([formula])
    
    # Batch update the 'Internal Leads' column
    if internal_leads_updates:
        internal_leads_range = f"{gspread.utils.rowcol_to_a1(HEADER_ROW_COLUMN_NAMES+1, internal_leads_col_idx)}:{gspread.utils.rowcol_to_a1(len(internal_leads_updates) + HEADER_ROW_COLUMN_NAMES, internal_leads_col_idx)}"
        total_campaigns_sheet.update(range_name=internal_leads_range, values=internal_leads_updates, value_input_option='USER_ENTERED')
    
    # Batch update the 'Internal Leads CPL' column
    if internal_leads_cpl_updates:
        internal_leads_cpl_range = f"{gspread.utils.rowcol_to_a1(HEADER_ROW_COLUMN_NAMES+1, internal_leads_cpl_col_idx)}:{gspread.utils.rowcol_to_a1(len(internal_leads_cpl_updates) + HEADER_ROW_COLUMN_NAMES, internal_leads_cpl_col_idx)}"
        total_campaigns_sheet.update(range_name=internal_leads_cpl_range, values=internal_leads_cpl_updates, value_input_option='USER_ENTERED')

def merge_non_empty_columns_in_first_row(mysheet):
    # Get all values in the first row
    first_row_values = mysheet.row_values(1)
    # print(first_row_values)
 # List to keep track of the column numbers of non-empty cells
    columns_idx = [i  for i, value in enumerate(first_row_values) if value.strip()]  # 0-indexed for easier enumeration
        # Adjust for 1-indexed column numbers and API usage
    columns_idx = [idx + 1 for idx in columns_idx]
    print(columns_idx)

# Iterate over non-empty columns
    for i, start_index in enumerate(columns_idx):
        # If it's not the last non-empty cell, merge until the next non-empty cell
        if i < len(columns_idx) - 1:
            next_index = columns_idx[i + 1] - 1
        else:  # For the last non-empty cell, merge it with the next 7 cells
            next_index = start_index + 7
            # Ensure not to exceed the total number of columns
            next_index = min(next_index, len(first_row_values))
        if next_index-start_index==0:
            continue
        print(f'start_index={start_index}   next_index={next_index} ')
        # Convert column indexes to letters
        start_col_letter = column_index_to_string(start_index)
        end_col_letter = column_index_to_string(next_index)

        # Define the range to merge
        merge_range = f"{start_col_letter}1:{end_col_letter}1"
        print(f'Merging range: {merge_range}')
        mysheet.merge_cells(merge_range, merge_type='MERGE_ALL')
        add_left_right_borders_to_columns(mysheet,start_index, next_index)
        # pauseMe(22)   

def create_weeks_summary_sheet(spreadsheet, source_sheet):
    print('EXECUTE  - create_weeks_summary_sheet')
    # Get all the data from the source sheet
    source_data = source_sheet.get_all_values()
    headers = source_data[0]  # Assuming row 1 contains headers
    headers1 = source_data[1]  # Assuming row 1 contains headers
    # Identify columns to copy
    b_column_index = 1  # Column 'B' is at index 1
    bau_brand_column_index = headers.index('BAU | Brand')
    fb_total_col_index = headers.index('TOTAL Summary FB') 
    google_total_col_index = headers.index('TOTAL Summary Google')
    internal_leads_col_index = headers1.index('Internal Leads')
    internal_leads_cpl_col_index = headers1.index('Internal leads CPL')

    # print(f'bau_brand_column_index={bau_brand_column_index}')
    # Ensure BAU | Brand column was found
    if bau_brand_column_index is None:
        print("BAU | Brand column not found.")
        return

    week_data_aggregated = {}
    # Filter out the rows where column A starts with 'Week' and aggregate the data
    for row in source_data:
        if row[0].startswith('Week'):
            week_number = row[0].split('-')[1].strip()  # Extract the week number
            # Extract and convert values for each required column
            values = [
                float(row[b_column_index].replace('€', '').replace(',', '').strip() if row[b_column_index] else 0),
                float(row[fb_total_col_index].replace('€', '').replace(',', '').strip() if row[fb_total_col_index] else 0),
                float(row[google_total_col_index].replace('€', '').replace(',', '').strip() if row[google_total_col_index] else 0),
                float(row[bau_brand_column_index].replace('€', '').replace(',', '').strip() if row[bau_brand_column_index] else 0),
                float(row[internal_leads_col_index].replace('€', '').replace(',', '').strip() if row[internal_leads_col_index] else 0),
                float(row[internal_leads_cpl_col_index].replace('€', '').replace(',', '').strip() if row[internal_leads_cpl_col_index] else 0)
            ]
            if week_number not in week_data_aggregated:
                week_data_aggregated[week_number] = values
            else:
                # Sum up the values
                week_data_aggregated[week_number] = [sum(x) for x in zip(week_data_aggregated[week_number], values)]
    
    try:
        weeks_summary_sheet = spreadsheet.worksheet('WeeksSummary')
        spreadsheet.del_worksheet(weeks_summary_sheet)
    except gspread.WorksheetNotFound:
        pass

    weeks_summary_sheet = spreadsheet.add_worksheet(title='WeeksSummary', rows=str(len(week_data_aggregated) + 5), cols="20")
    # print(f'week_data_aggregated={week_data_aggregated}')
     # Prepare and insert the data into 'WeeksSummary'

    headers = ["Weeks", "Totals", "FB_Total", "Google_Total"]
    if bau_brand_column_index is not None:
        headers.append(source_data[0][bau_brand_column_index]+'_Impressions')
    headers.append('Internal Leads')
    headers.append('Internal Leads CPL')
    data_to_insert = [headers]

    for week_number, aggregated_data in sorted(week_data_aggregated.items(), 
                                            key=lambda x: (int(x[0].split(',')[1].strip()), int(x[0].split(',')[0].strip()))
                                               ):
        data_to_insert.append([f"Week-{week_number}"] + aggregated_data)

    # print('data_to_insert')
    # print(data_to_insert)

    for _ in range(5):  # Add five empty rows after the last row of data
        data_to_insert.append(['' for _ in range(len(data_to_insert[0]))])

    end_column_letter = chr(64 + len(data_to_insert[0]))  # Calculate the letter for the last column
    end_row_number = len(data_to_insert)  # Calculate the last row number
    rng = f'A1:{end_column_letter}{end_row_number}'  # Update the range string accordingly
    # print(f'Calculated range: {rng}')
    weeks_summary_sheet.update(values=data_to_insert, range_name=rng, value_input_option='USER_ENTERED')
    normalize_data(spreadsheet, weeks_summary_sheet,'week')

def create_months_summary_sheet(spreadsheet, source_sheet):
    print('EXECUTE - create_month_summary_sheet')

    # Get all the data from the source sheet
    source_data = source_sheet.get_all_values()
    headers = source_data[0]  # Assuming row 1 contains headers
    headers1 = source_data[1]  # Assuming row 1 contains headers

    # Identify columns to copy
    b_column_index = 1  # Column 'B' is at index 1
    bau_brand_column_index = headers.index('BAU | Brand')
    fb_total_col_index = headers.index('TOTAL Summary FB') 
    google_total_col_index = headers.index('TOTAL Summary Google')
    internal_leads_col_index = headers1.index('Internal Leads')
    internal_leads_cpl_col_index = headers1.index('Internal leads CPL')
    # print(f'baubrand ={column_index_to_string(bau_brand_column_index)}')

    # Ensure BAU | Brand column was found
    if bau_brand_column_index is None:
        print("BAU | Brand column not found.")
        return

    month_data_aggregated = {}
    # Filter out the rows where column A starts with 'TOTAL' and aggregate the data
    for row in source_data:
        if row[0].startswith('TOTAL'):
            splt= row[0].split(' ')
            month_name =splt[1].strip()+splt[2].strip()  # Extract the month name
            # print(f'HEY {row[0]} --- {month_name} \n')
            # Extract and convert values for each required column
            values = [
                            float(row[b_column_index].replace('€', '').replace(',', '').strip() if row[b_column_index] else 0),
                            float(row[fb_total_col_index].replace('€', '').replace(',', '').strip() if row[fb_total_col_index] else 0),
                            float(row[google_total_col_index].replace('€', '').replace(',', '').strip() if row[google_total_col_index] else 0),
                            float(row[bau_brand_column_index].replace('€', '').replace(',', '').strip() if row[bau_brand_column_index] else 0),
                            float(row[internal_leads_col_index].replace('€', '').replace(',', '').strip() if row[internal_leads_col_index] else 0),
                            float(row[internal_leads_cpl_col_index].replace('€', '').replace(',', '').strip() if row[internal_leads_cpl_col_index] else 0)
                        ]
            if month_name not in month_data_aggregated:
                month_data_aggregated[month_name] = values
            else:
                # Sum up the values for existing months
                month_data_aggregated[month_name] = [sum(x) for x in zip(month_data_aggregated[month_name], values)]

    try:
        month_summary_sheet = spreadsheet.worksheet('MonthSummary')
        spreadsheet.del_worksheet(month_summary_sheet)
    except WorksheetNotFound:
        pass

    month_summary_sheet = spreadsheet.add_worksheet(title='MonthSummary', rows=str(len(month_data_aggregated) + 5), cols="20")
    # print(f'month_data_aggregated={month_data_aggregated}')
    # Prepare and insert the data into 'MonthSummary'
    headers = ["Month", "Totals", "FB_Total", "Google_Total", source_data[0][bau_brand_column_index]+'_Impressions']
    headers.append('Internal Leads')
    headers.append('Internal Leads CPL')
    data_to_insert = [headers]

    def parse_month_year(month_year_str):
        try:
            # Split the string into month and year
            month_str, year_str = month_year_str.split(',')
            # Convert month string to its corresponding integer value
            month = datetime.strptime(month_str.strip(), '%B').month
            # Convert year string to integer
            year = int(year_str.strip())
            return year, month
        except ValueError:
            # Handle unexpected format
            return 0, 0

    for month_name, aggregated_data in sorted(  month_data_aggregated.items(),
                                                key=lambda x: parse_month_year(x[0])
                                                ):
        data_to_insert.append([month_name] + aggregated_data)

    for _ in range(5):  # Add five empty rows after the last row of data for formatting
        data_to_insert.append(['' for _ in range(len(data_to_insert[0]))])

    end_column_letter = chr(64 + len(data_to_insert[0]))  # Calculate the letter for the last column
    end_row_number = len(data_to_insert)  # Calculate the last row number
    range_name = f'A1:{end_column_letter}{end_row_number}'  # Update the range string accordingly

    # Update the sheet with the aggregated data, respecting the updated method signature
    month_summary_sheet.update(values=data_to_insert, range_name=range_name, value_input_option='USER_ENTERED')

    # Normalize data if required
    normalize_data(spreadsheet, month_summary_sheet, 'month')

# Function to normalize values
def normalize_data(spreadsheet, sheet, period='week'):
    print('EXECUTE - normalize_data')
    # Get all the data from the sheet
    data = sheet.get_all_values()
    # Convert to a DataFrame
    df = pd.DataFrame(data)
    # # Assuming the first row is the header
    df.columns = df.iloc[0] # This is the header row 
    df = df[1:] # Drop the first row since it's now the header

    # Convert numeric columns to float, ensure 'Totals' is treated as numeric
    # Ensure 'Totals' column is correctly targeted for numeric conversion
    numeric_columns = [col for col in df.columns if col != 'Date' and col != 'Weeks' and col != 'Month' ]  # Exclude 'Date' if it's not to be normalized
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

    # Normalize the numeric columns
    df_normalized = df.copy()
    for column in numeric_columns:
        # Skip normalization for 'Date' if it's included in numeric_columns by mistake
        if column != 'Date' and column != 'Weeks':
            df_normalized[column] = (df[column] - df[column].min()) / (df[column].max() - df[column].min())

    
    increments = {
        'FB_Total': 1,
        'Google_Total': 2,
        'BAU | Brand_Impressions': 3,
        'Internal Leads':4,
        'Internal Leads CPL':5,
        # Add more if needed, like 'Total cpComments': X, where X is the increment for that column
    }

    # Add the increments to each column's values
    for column, increment in increments.items():
        if column in df_normalized.columns:
            df_normalized[column] = df_normalized[column] + increment

    # Check if a sheet named 'Normalized Data' exists
    try:
        normalized_sheet = spreadsheet.worksheet(f'Normalized Data '+period)
        spreadsheet.del_worksheet(normalized_sheet)
    except gspread.WorksheetNotFound:
        # If it does not exist, create a new sheet
        pass
    normalized_sheet = spreadsheet.add_worksheet(title=f'Normalized Data '+period, rows=df_normalized.shape[0]+10, cols=len(df_normalized.columns)+10)
    # Prepare the data for update, including the header
    normalized_data = [df.columns.tolist()] +  df_normalized.values.tolist()
    # print('Normalized Data')
    # print(f'{normalized_data}')
    
    for _ in range(37):  # Add five empty rows after the last row of data
        normalized_data.append(['' for _ in range(len(normalized_data[0]))])

 # Update the sheet with normalized data
    normalized_sheet.update(values=normalized_data, range_name='A1', value_input_option='USER_ENTERED') #started range = A1 !
    add_summary_chart(normalized_sheet, period,"Total Ad Spend vs Brand Impressions", ['Totals', 'BAU | Brand_Impressions'], "I1", width=800, height=300)
    add_summary_chart(normalized_sheet, period,"Ad Spend by platform vs Brand Impressions", ['FB_Total', 'Google_Total','BAU | Brand_Impressions'], "H17", width=800, height=300)
    add_summary_chart(normalized_sheet, period,"Ad Spend by platform vs CPL vs Leads Volume", ['FB_Total', 'Google_Total','Internal Leads','Internal Leads CPL'], "A10", width=600, height=300)
    add_summary_chart(normalized_sheet, period,"Total Ad Spend vs CPL vs Leads Volume ", ['Totals','Internal Leads','Internal Leads CPL'], "A20", width=600, height=300)

        # add_summary_chart(normalized_sheet, "Totals","Per BRAND ", ['FB_Total', 'Google_total','BAU | Brand_Impressions'], "F11")
    return df_normalized






