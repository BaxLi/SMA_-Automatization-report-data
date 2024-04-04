from google.oauth2.service_account import Credentials

SERVICE_ACCOUNT_FILE_TEST = 'sma-automatization-d95cdc6c39de.json'
SERVICE_ACCOUNT_FILE_PROD = 'sma-marketing.json'
# -------- TEST WORKBOOK -------------
GOOGLE_WORKBOOK = '1XYa7prf5npKZw5OKmGzizXsUPhbL84o0vLxGKZab1c4'
workbook_url= 'https://docs.google.com/spreadsheets/d/1XYa7prf5npKZw5OKmGzizXsUPhbL84o0vLxGKZab1c4/edit#gid=231244777'

#  ___________________________ Production  ____________________________
# workbook_url= 'https://docs.google.com/spreadsheets/d/1NEAGIiwY6rBirO0YWzP8Eq0f41BdxLiazuRYBVzxHyU/edit#gid=443982442'
# GOOGLE_WORKBOOK = '1NEAGIiwY6rBirO0YWzP8Eq0f41BdxLiazuRYBVzxHyU'

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
# Google campaigns have to be mapped to 
# 1. BAU | Brand 
# 2. ⁠BAU | Search 
# 3. ⁠BAU | PMAX 
# 4. ⁠nBAU Google 
GOOGLE_CAMPAIGNS = [
    "Brand Protect | MCPC", "Denti Fissi | MCPC -19.12", "Main KW | MCPC - 19.12", "PMAX | MCONV"
]

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
