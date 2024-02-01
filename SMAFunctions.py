from dateutil import parser
import gspread
import time
from gspread.utils import column_letter_to_index
from gspread.exceptions import WorksheetNotFound
import pandas as pd
from SMAGoogleAPICalls import total_row_format, campaign_format_dates
from SMA_Constants import fb_campaigns, google_campaigns, commonExportedCampaignsSheet

def update_sheet_headers(worksheet, replacements):
    first_row_values = worksheet.row_values(1)
    new_first_row_values = [replacements.get(value, value) for value in first_row_values]

    if first_row_values != new_first_row_values:
        worksheet.update('A1', [new_first_row_values])

def update_sum_formulas_in_row_TOTAL_company(company, interim_campaigns_sheet, date_row):
    print(f' CALL update_sum_formulas_in_row - date_row={date_row}')
    def create_sum_formula(start_col, date_row, step, total_cols):
        # Generate the range of columns for the sum formula
        formula_parts = [f'INDIRECT("{column_index_to_string( idx)}"&{date_row})' for idx in
                         range(column_letter_to_index(start_col), total_cols + 1, step)]
        return f"=SUM({','.join(formula_parts)})"

    # Starting column and step size
    start_col_letter = 'N' if company=='FB' else 'CE'  # Starting from column G
    start_col_index = column_letter_to_index(start_col_letter)
    
    step = 7  # Step size
    
    end_col_letter='BR' if company=='FB' else 'CZ'
    end_col_index = column_letter_to_index(end_col_letter)
 

    # Calculate the total number of columns from start_col_letter to end_col_letter
    total_cols = end_col_index - start_col_index + 1
    # total_cols = interim_campaigns_sheet.col_count  # Total number of columns in the sheet
    print(f'start_col_letter={start_col_letter} start_col_index={start_col_index} end_col_letter={end_col_letter}' \
          f' end_col_index={end_col_index} total_cols={total_cols} \n')
    # Iterate through the cells B, C, D
    # for i in range(2, 6):  # B(2), C(3), D(4)
    for i in range(start_col_index-6, start_col_index-2):  # B(2), C(3), D(4), E(5)

        # Calculate the start column letter for this iteration
        calculated_col_letter = column_index_to_string(
            # column_letter_to_index(start_col_letter) + (i - 2))
            start_col_index + (i - 8))
        print(f'calculated_col_letter={calculated_col_letter} \n')
        sum_formula = create_sum_formula(
            calculated_col_letter, date_row, step, total_cols)
        # Convert 2 to B, 3 to C, and 4 to D
        cell_to_update = f'{chr(64 + i)}{date_row}'
        interim_campaigns_sheet.update( values=sum_formula,range_name=cell_to_update, value_input_option='USER_ENTERED')
        # print(f'Updated cell {cell_to_update} with formula: {sum_formula}')

    ad_spend_total = gspread.utils.rowcol_to_a1(date_row, start_col_index-6) #2
    leads_total = gspread.utils.rowcol_to_a1(date_row, start_col_index-4) #4
    comments_total = gspread.utils.rowcol_to_a1(date_row, start_col_index-3) #5
    # for i in range(6, 8):
    for i in range(start_col_index-2, start_col_index):
        calculated_col_letter = column_index_to_string(i)
        # Convert 5 to E, etc
        cell_to_update = f'{calculated_col_letter}{date_row}'
        sum_formula = ''
        if i == start_col_index-2:
            sum_formula = f"=IF({leads_total}<>0,{ad_spend_total}/{leads_total},0)"
        if i == start_col_index-1:
            sum_formula = f"=IF({comments_total}<>0,{ad_spend_total}/{comments_total},0)"
        interim_campaigns_sheet.update(cell_to_update, sum_formula,
                        value_input_option='USER_ENTERED')
        print(
            f'percentage i={i} Updated cell {cell_to_update} with formula: {sum_formula}')


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
    pauseMe("Step-1 finish")

# Iterate over each row in 'campaign_exp_data'
def step2_iterateExport(campaign_exp_sheet, interim_campaigns_sheet):

    # Now, retrieve the data from the sheet and store it in the 'campaign_exp_data' DataFrame
    campaign_exp_data = pd.DataFrame(campaign_exp_sheet.get_all_records())

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
            if (date_row_number>interim_campaigns_sheet.row_count):
                interim_campaigns_sheet.append_row([date])  # Append the new date
                dates_column.append(date)  # Update local date list
            else:
                cell_address = gspread.utils.rowcol_to_a1(date_row_number, 1)
                interim_campaigns_sheet.update_acell(cell_address, row['Date'])

        # Determine the campaign from the 'AdSet name'
        determined_campaign = row['Determined Campaign'] 
        print(f'\n determined_campaign={determined_campaign} date_row_number={date_row_number}\n ============')
        if determined_campaign == "Other":
            print(f'determine_campaign == "Other" ? {determined_campaign == "Other"}')
            continue
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
        interim_campaigns_sheet.update(values=[update_values], range_name=f'{start_cell}:{end_cell}', value_input_option='USER_ENTERED')
        time.sleep(2)
        # raise ValueError(f"LINE 190  INTERIM sheet updated date_row_number={date_row_number}")

    pauseMe("\n Step-2 finish \n")