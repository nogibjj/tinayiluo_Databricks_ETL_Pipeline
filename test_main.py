"""
Test databricks fucntionaility
"""
import requests
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/tinayiluo_Databricks_ETL_Pipeline"
url = f"https://{server_h}/api/2.0"

# Function to check if a file path exists and auth settings still work
def check_filestore_path(path, headers): 
    try:
        response = requests.get(url + f"/dbfs/get-status?path={path}", headers=headers)
        response.raise_for_status()
        return response.json()['path'] is not None
    except Exception as e:
        print(f"Error checking file path: {e}")
        return False

# Test if the specified FILESTORE_PATH exists
def test_databricks():
    headers = {'Authorization': f'Bearer {access_token}'}
    assert check_filestore_path(FILESTORE_PATH, headers) is True

if __name__ == "__main__":
    test_databricks()