from dateutil import parser
import gspread
import time
from gspread.utils import column_letter_to_index
from gspread.exceptions import WorksheetNotFound
import pandas as pd
from SMAGoogleAPICalls import total_summary_section_format, campaign_format_dates
from SMA_Constants import fb_campaigns, google_campaigns, commonExportedCampaignsSheet, TOTAL_TOTAL_COL,FB_TOTAL_COL,GOOGLE_TOTAL_COL

  

def restructure_to_weekly(interim_campaigns_sheet, total_sheet):
    # Fetch the data from 'interim_campaigns_sheet'
    interim_data = interim_campaigns_sheet.get_all_values()
    total_data = total_sheet.get_all_values()

    # Headers are assumed to be in the first two rows
    headers = [interim_data[0], interim_data[1]]

    # Data is assumed to start from the third row
    df_interim = pd.DataFrame(interim_data[2:], columns=headers[1])
    df_total = pd.DataFrame(total_data[2:], columns=headers[1])
    # Convert the 'Date' column to datetime if necessary
    df_interim['Date'] = pd.to_datetime(df_interim['Date'])
    df_total['Date'] = pd.to_datetime(df_total['Date'])

    # Set 'Date' as the index for both DataFrames
    df_interim.set_index('Date', inplace=True)
    df_total.set_index('Date', inplace=True)

    # Update df_total with df_interim entries; this will overwrite existing data in df_total with that in df_interim
    # for matching dates. Non-matching dates in df_total will remain as is.
    df_total.update(df_interim)
    # Reset the index if you want 'Date' back as a column
    df_total.reset_index(inplace=True)

    df=df_total
    print(df.iloc[3:5].to_string())
    # Sort the dataframe by date
    df.sort_values(by='Date', inplace=True)

    # Initialize structured data with headers
    structured_data = [headers[0]] + [headers[1]]

    # Initialize tracking for the week of the year and month
    previous_week = None
    week_days_count = 0

    for index, row in df.iterrows():
        current_date = row['Date']
        week_of_year = current_date.isocalendar().week
        month_name = current_date.strftime('%B')

        # Track the day count within the same week
        if week_of_year == previous_week or previous_week is None:
            week_days_count += 1
        else:
            # Append week label when moving to a new week
            structured_data.append([f"{previous_month} Week {previous_week}"])
            week_days_count = 1  # Reset counter for the new week

        # Append daily data
        structured_data.append([current_date.strftime('%A, %B %d, %Y')] + row[1:].tolist())

        # Track previous day's week and month for comparison in the next iteration
        previous_week = week_of_year
        previous_month = month_name

        # Check if it's the last row to append the week and total labels correctly
        if index == len(df) - 1:
            structured_data.append([f"{month_name} Week {week_of_year}"])  # Week label for the last week
            structured_data.append([f"Total {month_name}"])  # Total label for the last month

    # Clear the 'TOTAL' sheet before updating it with the new structured data
    total_sheet.clear()
    total_sheet.update('A1', structured_data, value_input_option='USER_ENTERED')

def update_sheet_headers(worksheet, replacements):
    first_row_values = worksheet.row_values(1)
    new_first_row_values = [replacements.get(value, value) for value in first_row_values]

    if first_row_values != new_first_row_values:
        worksheet.update('A1', [new_first_row_values])

def column_index_to_string(col_index):
    """Convert a column index into a column letter: 1 -> A, 2 -> B, etc."""
    if col_index < 1:
        raise ValueError("Index is too small")
    result = ""
    while col_index > 0:
        col_index, remainder = divmod(col_index - 1, 26)
        result = chr(65 + remainder) + result
    return result

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
        for campaign in fb_campaigns:
            if campaign in adset_name:
                return campaign
        for campaign in google_campaigns:
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
        pauseMe(1)
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

    # Convert numerical columns to float and fill missing values
    data[['Ad Spend', 'Total Leads', 'Post comments', 'Impressions']] = data[['Ad Spend', 'Total Leads', 'Post comments', 'Impressions']].apply(pd.to_numeric, errors='coerce').fillna(0)

    # Simplify the determine_campaign function and apply it
    campaigns = {'fb_campaigns': fb_campaigns, 'google_campaigns': google_campaigns}  # Assuming fb_campaigns and google_campaigns are defined elsewhere

    def determine_campaign(adset_name):
        for source, campaign_list in campaigns.items():
            for campaign in campaign_list:
                if campaign in adset_name:
                    return campaign
        return 'Other'

    data['Determined Campaign'] = data['AdSet name'].apply(determine_campaign)

    # Aggregate data
    grouped = data.groupby(['Date', 'Determined Campaign']).sum().reset_index()
    grouped = grouped.drop(columns=['AdSet name'])

    # Write to 'campaign_exp_sheet'
    sheet_title = commonExportedCampaignsSheet
    try:
        campaign_exp_sheet = spreadsheet.worksheet(sheet_title)
    except WorksheetNotFound:
        campaign_exp_sheet = spreadsheet.add_worksheet(title=sheet_title, rows="100", cols="20")

    campaign_exp_sheet.clear()  # Clear before updating to avoid appending to old data
    campaign_exp_sheet.update(values=[grouped.columns.values.tolist()] + grouped.values.tolist(), range_name='A1' )
    print(f'LINE 116 Finish FUNCTION STEP - 1  \n')
    # pauseMe("Step-1 finish")

# Iterate over each row in 'campaign_exp_data'
def step2_iterateExport(campaign_exp_sheet, interim_campaigns_sheet):
    # Now, retrieve the data from the sheet and store it in the 'campaign_exp_data' DataFrame
    campaign_exp_data = pd.DataFrame(campaign_exp_sheet.get_all_records())
    # Define colors for success and failure
    success_color = {"red": 0.85, "green": 0.93, "blue": 0.83}  # Light green
    failure_color = {"red": 0.96, "green": 0.80, "blue": 0.80}  # Light red

    # Retrieve headers to find the correct column for each campaign metric
    header_row = interim_campaigns_sheet.row_values(1)  # Campaign names

    # Assuming the date column is the first one in 'InterimFB'
    dates_column = interim_campaigns_sheet.col_values(1)[2:]  # Start from row 3 to skip header rows

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
        time.sleep(2)

def step_Campaign_totals(interim_campaigns_sheet, start_totals_column, last_column=None):
    # Retrieve all data from the sheet
    all_data = interim_campaigns_sheet.get_all_values()

    # Calculate the starting index
    start_index = column_letter_to_index(start_totals_column)
    
    # If last_column is provided, use it; otherwise, find dynamically
    if last_column:
        end_index = column_letter_to_index(last_column)
    else:
        # Determine the end index based on the condition
        end_index=interim_campaigns_sheet.col_count
    
    # Loop through each row of data (skipping headers)
    for i, row in enumerate(all_data[2:], start=3):  # Skip header rows
        # Build the sum formulas for the specified range
        sum_formulas = {
            'ad_spend': [],
            'impressions': [],
            'leads': [],
            'comments': []
        }
        
        # Aggregate formulas for each metric
        for col_offset in range(start_index+6, end_index, 7):
            sum_formulas['ad_spend'].append(f'{column_index_to_string(col_offset)}{i}')
            sum_formulas['impressions'].append(f'{column_index_to_string(col_offset + 1)}{i}')
            sum_formulas['leads'].append(f'{column_index_to_string(col_offset + 2)}{i}')
            sum_formulas['comments'].append(f'{column_index_to_string(col_offset + 3)}{i}')
        
        # Construct the final formulas
        ad_spend_formula = '+'.join(sum_formulas['ad_spend'])
        impressions_formula = '+'.join(sum_formulas['impressions'])
        leads_formula = '+'.join(sum_formulas['leads'])
        comments_formula = '+'.join(sum_formulas['comments'])
        cpl_formula = f"=IF(SUM({leads_formula})<>0,SUM({ad_spend_formula})/SUM({leads_formula}),0)"
        cp_comments_formula = f"=IF(SUM({comments_formula})<>0,SUM({ad_spend_formula})/SUM({comments_formula}),0)"
        
        # Update the sheet with the final formulas for each metric
        update_range = f'{start_totals_column}{i}:{column_index_to_string(start_index + 6)}{i}'
        interim_campaigns_sheet.update(update_range, [[
            f"=SUM({ad_spend_formula})", 
            f"=SUM({impressions_formula})", 
            f"=SUM({leads_formula})", 
            f"=SUM({comments_formula})", 
            cpl_formula,
            cp_comments_formula
        ]], value_input_option='USER_ENTERED')
        time.sleep(1)

def step2_Totals_Calc(interim_campaigns_sheet, total_col=TOTAL_TOTAL_COL, fb_col=FB_TOTAL_COL, google_col=GOOGLE_TOTAL_COL):
    # Retrieve all data from the sheet
    all_data = interim_campaigns_sheet.get_all_values()
    
    # Calculate the column indices based on the provided column letters
    fb_col_index = column_letter_to_index(fb_col)
    google_col_index = column_letter_to_index(google_col)
    total_col_index = column_letter_to_index(total_col)
    
    # Loop through the rows and calculate totals
    for i, row in enumerate(all_data[2:], start=3):  # Skip header rows and adjust for 1-based indexing
        # Formulas for "Total Ad Spend", "Impressions", "Leads", "Comments", "Total CPL", and "cpComments"
        total_ad_formula = f"={fb_col}{i}+{google_col}{i}"
        total_impressions_formula = f"={column_index_to_string(fb_col_index+1)}{i}+{column_index_to_string(google_col_index+1)}{i}"
        total_leads_formula = f"={column_index_to_string(fb_col_index+2)}{i}+{column_index_to_string(google_col_index+2)}{i}"
        total_comments_formula = f"={column_index_to_string(fb_col_index+3)}{i}+{column_index_to_string(google_col_index+3)}{i}"
        total_cpl_formula = f"=IF({column_index_to_string(total_col_index+2)}{i}<>0,{column_index_to_string(total_col_index)}{i}/{column_index_to_string(total_col_index+2)}{i},0)"
        cp_comments_formula = f"=IF({column_index_to_string(total_col_index+3)}{i}<>0,{column_index_to_string(total_col_index)}{i}/{column_index_to_string(total_col_index+3)}{i},0)"
        
        # Update the cells with formulas
        interim_campaigns_sheet.update(f'{total_col}{i}:{column_index_to_string(total_col_index+5)}{i}', [[
            total_ad_formula,
            total_impressions_formula,
            total_leads_formula,
            total_comments_formula,
            total_cpl_formula,
            cp_comments_formula
        ]], value_input_option='USER_ENTERED')
