
 import requests
import pandas as pd
from google.oauth2.service_account import Credentials
import gspread
from gspread_dataframe import set_with_dataframe
import csv
from io import StringIO
import argparse
import os

def authenticate_and_run(sec):
    res = requests.post('https://metabase-lierhfgoeiwhr.newtonschool.co/api/session',
                        headers={"Content-Type": "application/json"},
                        json={"username": 'your-email@example.com', "password": sec})
    if not res.ok:
        raise Exception("Failed to authenticate")
    return res.json()['id']

def update_sheet(gc, token, metabase_card_id, spreadsheet_key, worksheet_name):
    try:
        res = requests.post(f'https://metabase-url/api/card/{metabase_card_id}/query/csv',
                            headers={'X-Metabase-Session': token})
        res.raise_for_status()

        if res.headers.get("Content-Type") == "text/csv":
            reader = csv.reader(StringIO(res.text))
            header = next(reader, [])
            df = pd.DataFrame(reader, columns=header)

            sheet = gc.open_by_key(spreadsheet_key)
            worksheet = sheet.worksheet(worksheet_name)

            num_cols = len(df.columns)
            if num_cols > 0:
                max_row = 1000000
                clear_range = f'A1:{chr(64 + num_cols)}{max_row}'
                worksheet.batch_clear([clear_range])

            set_with_dataframe(worksheet, df, include_index=False, include_column_header=True, resize=False, row=1, col=1)
            print(f"Updated '{worksheet_name}' with data from card {metabase_card_id}")

    except Exception as e:
        print(f"Error for '{worksheet_name}': {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Google Sheets Automation')
    parser.add_argument('--service-account-file', type=str, required=True)
    args = parser.parse_args()

    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_file(args.service_account_file, scopes=SCOPES)
    gc = gspread.authorize(creds)

    sec = os.getenv('ASHRITHA_SECRET_KEY')
    token = authenticate_and_run(sec)

    updates = [
        {"metabase_card_id": 7584, "worksheet_name": "Coding", "spreadsheet_key": "your-sheet-key"},
        # Add other update mappings here
    ]
    for update in updates:
        update_sheet(gc, token, update['metabase_card_id'], update["spreadsheet_key"], update['worksheet_name'])
