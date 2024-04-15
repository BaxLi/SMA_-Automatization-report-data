from google.oauth2.service_account import Credentials

SERVICE_ACCOUNT_FILE_PROD = 'sma-marketing.json'
GOOGLE_WORKBOOK = '1qQFyHYEzebnI3oLNEmLgG5izUXFdzTV9ijhphLCxmWY'
workbook_url= 'https://docs.google.com/spreadsheets/d/1qQFyHYEzebnI3oLNEmLgG5izUXFdzTV9ijhphLCxmWY/edit#gid=741057592'

# Define the scope
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

CREDENTIALS = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE_PROD,
    scopes=SCOPES
)

FB_CAMPAIGNS = [
    "BAU | Control_AdSet",
    "BAU | DC Type",
    "BAU | LLAs",
    "BAU | RTG",
    "BAU | Lead Generation",
    "BAU | PPE",
    "BAU | Page Likes",
    "nBAU | FB"
]
GOOGLE_CAMPAIGNS = [ "BAU | Brand", "BAU | Search", "BAU | PMAX", "nBAU Google"]

#Campaign Export step - column names
CAMPAIGN_AMOUNT_SPEND_FB="Amount Spent"
CAMPAIGN_LEADS_FB="Leads (all)"

interim_campaigns_sheet_name='InterimCampaigns'
TOTAL_CAMPAIGNS_SHEET_NAME='Total Summary'
INTERIM_SHEET_DATA=2

commonExportedCampaignsSheet='campaign_exp_sheet'
TOTAL_TOTAL_COL='B'
FB_TOTAL_COL='H'
GOOGLE_TOTAL_COL='BR'



def column_index_to_string(col_index):
    """Convert a column index into a column letter: 1 -> A, 2 -> B, etc."""
    if col_index < 1:
        raise ValueError("Index is too small")
    result = ""
    while col_index > 0:
        col_index, remainder = divmod(col_index - 1, 26)
        result = chr(65 + remainder) + result
    return result
