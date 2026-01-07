
import gspread
import yaml
import json
from google.oauth2.service_account import Credentials

def get_headers():
    with open('sources.yml', 'r') as f:
        config = yaml.safe_load(f)
    
    with open('secrets/google-service-account.json', 'r') as f:
        creds_info = json.load(f)
    
    scopes = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    gc = gspread.authorize(creds)
    
    results = {}
    
    for ssid, sdata in config.get('spreadsheets', {}).items():
        sh = gc.open_by_key(ssid)
        for sheet_cfg in sdata.get('sheets', []):
            target_table = sheet_cfg['target_table']
            range_name = sheet_cfg['range']
            # Get only the first row
            # Range might be "A1:R", we want the headers
            # If range starts at row 4, we need that row
            worksheet = sh.get_worksheet_by_id(sheet_cfg['gid'])
            
            # Simple way: get all values in the first row of the range
            # Range format "A1:R" -> "A1:R1"
            # Range format "B4:W" -> "B4:W4"
            parts = range_name.split(':')
            start_cell = parts[0]
            import re
            match = re.search(r'\d+', start_cell)
            row_num = match.group() if match else "1"
            col_letter = re.sub(r'\d+', '', start_cell)
            
            end_col = re.sub(r'\d+', '', parts[1]) if len(parts) > 1 else col_letter
            header_range = f"{start_cell}:{end_col}{row_num}"
            
            headers = worksheet.get(header_range)
            if headers:
                results[target_table] = headers[0]
                print(f"Table {target_table}: {headers[0]}")
            else:
                print(f"Failed to get headers for {target_table}")
                
    with open('headers.json', 'w') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    get_headers()
