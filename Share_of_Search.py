import os
import json
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from google_auth_oauthlib.flow import InstalledAppFlow
from google.ads.googleads.client import GoogleAdsClient
from google.oauth2 import service_account
import pandas_gbq
import time

############################## AUTHENTICATION #################################

# Authentication of Google Ads API

# Deleting expired Google tokens
if os.path.isfile('C:/path_to_token/token_Gads.json'):
  os.remove('C:/path_to_token/token_Gads.json')

# Importing credentials json file
flow = InstalledAppFlow.from_client_secrets_file(
    'C:/path_to_client_secret/SOS_client_secret.json',
    scopes=['https://www.googleapis.com/auth/adwords']
)

# Prompting new authentication window
creds = flow.run_local_server(port=0)

# Saving new refresh token on the computer
with open('C:/path_to_token/token_Gads.json', 'w') as token:
    token.write(creds.to_json())

# Extracting refresh_token from token json file
with open('C:/path_to_token/token_Gads.json') as f:
    data = json.load(f)
refresh_token = data['refresh_token']

# Creating credentials object
credentials = {
    "developer_token": open('C:/path_to_developer_token/developer_token.txt', 'r').read().strip(),
    "refresh_token": refresh_token,  
    "client_id": open('C:/path_to_client_id/client_id.txt', 'r').read().strip(),
    "client_secret": open('C:/path_to_client_secret/client_secret.txt', 'r').read().strip(),
    "use_proto_plus": False
}

# Creating service object
client_GAds = GoogleAdsClient.load_from_dict(credentials, version="v14")

# Authentication of BigQuery API

scopes_bq = ['https://www.googleapis.com/auth/bigquery',
             'https://www.googleapis.com/auth/cloud-platform',
             'https://www.googleapis.com/auth/drive']

# Creating service object
creds_bq = service_account.Credentials.from_service_account_file('C:/path_to_service_account_file/service_account.json',
                                                                 scopes = scopes_bq)

############################# DATE DEFINITION #################################

# Current date
now = datetime.now()

# Subtracting one month
last_month = now - relativedelta(months=1)

# Defining the year of last month
year_of_last_month = last_month.year

# Defining the name of last month and converting it to uppercase
name_of_last_month = last_month.strftime('%B').upper()

################################ FUNCTIONS ####################################

# Function to pull search volume
def pull_search_volume(chosen_keyword):
    googleads_service = client_GAds.get_service("GoogleAdsService")
    # Selecting service
    keyword_plan_idea_service = client_GAds.get_service("KeywordPlanIdeaService")
    request = client_GAds.get_type("GenerateKeywordHistoricalMetricsRequest")
    # Google Ads customer ID
    request.customer_id = "1234567890"
    # Defining desired keyword
    request.keywords.extend([chosen_keyword])
    # Defining the 5 regions of Denmark
    geo_codes = ["20243", "20245", "20252", "20254", "20256"]
    request.geo_target_constants.extend([googleads_service.geo_target_constant_path(geo_code) for geo_code in geo_codes])
    # Defining search network. Here, only Google Search is chosen
    request.keyword_plan_network = (client_GAds.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH)
    # Defining start date
    request.historical_metrics_options.year_month_range.start.year = year_of_last_month
    request.historical_metrics_options.year_month_range.start.month = getattr(client_GAds.enums.MonthOfYearEnum.MonthOfYear, name_of_last_month)
    # Defining end date
    request.historical_metrics_options.year_month_range.end.year = year_of_last_month
    request.historical_metrics_options.year_month_range.end.month = getattr(client_GAds.enums.MonthOfYearEnum.MonthOfYear, name_of_last_month)

    # Sending request
    attempts = 0 
    max_attempts = 5
    while attempts < max_attempts:
        try:
            # API call
            response = keyword_plan_idea_service.generate_keyword_historical_metrics(request=request)
            break
        except Exception as e:  # Catching all errors
            if "Unauthenticated" in str(e) or "401" in str(e):  # Retry if error concerns authentication
                attempts += 1
                time.sleep(10)   
                print(f"Authentication Error occurred: {e}. Retrying...")
            else:
                print(f"Unexpected error occurred: {e}. Exiting loop.")
                break
    else:
        print("Max authentication attempts reached. Terminating program.")
    
    return response

# Function to convert json response from API to an understandable dataframe
def create_dataframe(json_response):
    # Creating empty list to store data
    data = []
    # Loop to save data in the list
    for result in json_response.results:
        keyword = result.text
        for monthly_volume in result.keyword_metrics.monthly_search_volumes:
            data.append({
                'keyword': keyword,
                'year': monthly_volume.year,
                'month': monthly_volume.month,  
                'searches': monthly_volume.monthly_searches
            })

    # Data from the list is saved in a dataframe
    df = pd.DataFrame(data)
    # Months are subtracted by 1 to get months in 1-12 format instead of 2-13
    df['month'] = df['month'] -1
    # New column with combined year and month
    df['year_month'] = df['year'].astype(str) + "-" + df['month'].astype(str).str.zfill(2)
    df['year_month'] = pd.to_datetime(df['year_month'], format='%Y-%m')
    # Date is converted to date format
    df['year_month'] = pd.to_datetime(df['year_month'], format='%Y-%m').dt.strftime('%Y-%m-%d')
    
    return df

############################### DATA EXTRACTION ###############################

# List of all sites to be included
sites_list = ["alm brand bank",
              "arbejdernes landsbank",
              "banknordik",
              "danske bank",
              "den jyske sparekasse",
              "djursland bank",
              "handelsbanken",
              "dronninglund sparekasse",
              "handelsbanken",
              "jutlander bank",
              "jyske bank",
              "lollands bank",
              "lån og spar bank",
              "middelfart sparekasse",
              "nordea",
              "nordjyske bank",
              "nykredit",
              "saxo bank",
              "spar nord",
              "sydbank"]

# Empty list to store dataframes
dataframes = []

# Loop to extract data for each site using the functions
for i in sites_list:
    # Data extraction
    search_volume_response = pull_search_volume(i)
    # Response is converted to dataframe 
    search_volume_df = create_dataframe(search_volume_response)
    # Dataframe is appended
    dataframes.append(search_volume_df)
    time.sleep(1)
    
# All dataframes are combined
done_df = pd.concat(dataframes, ignore_index=True)

############################### Data Export ###################################

# Data export to BigQuery
pandas_gbq.to_gbq(
    dataframe = done_df,
    destination_table = 'share_on_search.share_on_search_table', 
    project_id = 'gcp_project_name', 
    if_exists = 'append', 
    credentials = creds_bq,
    api_method="load_csv")


