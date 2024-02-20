from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials

SERVICE_ACCOUNT_FILE = 'sma-automatization-d95cdc6c39de.json'
Google_workbook = '1XYa7prf5npKZw5OKmGzizXsUPhbL84o0vLxGKZab1c4'
Google_doc_sheet_id = 231244777
workbook_url= 'https://docs.google.com/spreadsheets/d/1XYa7prf5npKZw5OKmGzizXsUPhbL84o0vLxGKZab1c4/edit#gid=231244777'

# Use the JSON key file you downloaded to set up the credentials
scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(
    'sma-automatization-d95cdc6c39de.json', scope)

CREDENTIALS = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=['https://www.googleapis.com/auth/spreadsheets']
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
GOOGLE_CAMPAIGNS = [
    "Brand Protect | MCPC","Denti Fissi | MCPC -19.12", "Main KW | MCPC - 19.12", "nBAU | Google"
]

interim_campaigns_sheet_name='InterimCampaigns'
INTERIM_SHEET_DATA=2
commonExportedCampaignsSheet='campaign_exp_sheet'
TOTAL_TOTAL_COL='B'
FB_TOTAL_COL='H'
GOOGLE_TOTAL_COL='BY'