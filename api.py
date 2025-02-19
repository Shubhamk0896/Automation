import requests
import pandas as pd
from google.cloud import bigquery

# Set up BigQuery client
client = bigquery.Client()

def fetch_api_data(api_url):
    """Fetch data from API."""
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error fetching data: {response.status_code}")

def load_data_to_bq(data, dataset_id, table_id):
    """Load JSON data into BigQuery."""
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)
    
    # Convert JSON data to DataFrame
    df = pd.DataFrame(data)
    
    # Load DataFrame into BigQuery
    job = client.load_table_from_dataframe(df, table)
    job.result()  # Wait for the job to complete
    print("Data loaded successfully")

if __name__ == "__main__":
    API_URL = "https://jsonplaceholder.typicode.com/posts"  # Open-source API
    DATASET_ID = "calm-analog-383811.exercise_arena"  # Replace with your dataset
    TABLE_ID = "api_test"  # Replace with your table
    
    api_data = fetch_api_data(API_URL)
    print(api_data)
    # load_data_to_bq(api_data, DATASET_ID, TABLE_ID)
