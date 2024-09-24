#%% Imports
import requests
import pandas as pd
from bs4 import BeautifulSoup
from collections import defaultdict

#%% 
def extract_FRED_data(url,variable):

    # Make a GET request to extract data from the url
    response = requests.get(url)
    assert response.status_code == 200, "GET Request Failed"

    # Extract table from the url
    soup = BeautifulSoup(response.text,'lxml')
    table = soup.find('table')

    # Extract headers from the table
    header_rows = table.find('thead').find_all('tr')
    main_header_row = header_rows[1] # Second header row is the main header row which actually contains headers
    headers = [header.get_text(strip=True) for header in main_header_row.find_all('th')] #Extract table header (th) elements from the main header row
    # print(headers) #['', 'Name', '2023', 'PrecedingPeriod', 'Year Agofrom Period']

    # Extract unit
    unit = table.find('thead').find('th', id='table-unit-heading').get_text(strip=True)
    # unit_header_row = header_rows[0]
    # unit = unit_header_row.find('th', id='table-unit-heading').get_text(strip=True)

    # Create a df to store data from the table with column names based on headers extracted
    df = pd.DataFrame(columns=headers[1:-1]) # Skip first (check box) and last (duplicated info with second last) headers
    # Extract data row by row
    body_rows = table.find('tbody').find_all('tr')
    for body_row in body_rows:

        row_data = [] # to store data element-wise
        state_name = body_row.find('th').find('span',class_='fred-rls-elm-nm').get_text(strip=True)
        values = [value.get_text(strip=True) for value in body_row.find_all('td', class_='fred-rls-elm-vl-td')[:-1]] # Skip the last element; extract 2023 and 2022 data
        row_data.append(state_name)
        row_data += values
        
        # Append new data row to df
        length = len(df)
        df.loc[length] = row_data
    df.rename(columns= {
        'Name':'State Name',
        '2023': f'{variable} [{unit}] (2023)',
        'PrecedingPeriod': f'{variable} [{unit}] (2022)'      
    }, inplace=True)

    value_cols = [f'{variable} [{unit}] (2023)',f'{variable} [{unit}] (2022)']
    for value_col in value_cols:
        df[value_col] = df[value_col].str.replace(',','').astype(float)

    return df
#%%
def extract_ACS(api_key, year, variables, state_code=None):
    '''
    Extract state-level ACS data using a session (if provided).
    '''
    # Define API Base URL for ACS 1-Year Estimates
    base_url = f'https://api.census.gov/data/{year}/acs/acs1'
    variables_str = ','.join(variables)
    
    # Define query params based on input
    state_filter = f'state:{state_code}' if state_code is not None else 'state:*'
    params = {
        'get': variables_str,
        'for': state_filter,
        'key': api_key
    }

    response = requests.get(base_url, params=params)
    assert response.status_code == 200, 'GET request failed'
    
    data = response.json()
    df = pd.DataFrame(data[1:], columns=data[0])

    # Map variable code to variable name (Column names)
    # Make another request to retrieve variable metadata (i.e., dictionary of variable information)
    metadata_url = f'https://api.census.gov/data/{year}/acs/acs1/variables.json'
    metadata_response = requests.get(metadata_url)
    metadata = metadata_response.json()

    var_url = f"https://api.census.gov/data/{year}/acs/acs1/variables.json"
    response = requests.get(var_url)
    assert response.status_code == 200, 'GET request failed'
    
    # print(response.text) # Nested dictionary
    variables = dict(response.json())

    # Loop through the DataFrame columns and rename them based on the metadata
    for variable_code in df.columns:
        variable_info = metadata.get('variables', {}).get(variable_code, {})
        variable_concept = variable_info.get('concept', '')
        variable_label = variable_info.get('label', '')
        variable_name = f'{variable_label} ({variable_concept})' if variable_concept else variable_label
        df.rename(columns={variable_code: variable_name}, inplace=True)
    df['Year'] = year
    return df

def preprocess_ACS(acs_df):
    new_acs_cols = [
    'Median Household Income',
    'Per Capita Income',
    'Unemployed Population',
    'Employed Population',
    'Gini Index of Income Inequality',
    'Total Population',
    'Median Age',
    'Foreigner Population',
    'Median Home Value', 
    'State Code (FIPS)',
    'Year'
    ]
    acs_df.columns = new_acs_cols
    acs_df[new_acs_cols[:-2]] = acs_df[new_acs_cols[:-2]].astype(float)
    acs_df['Unemployment Rate'] = acs_df['Unemployed Population']/acs_df['Total Population']
    acs_df['Percent Foreigners'] = acs_df['Foreigner Population']/acs_df['Total Population']
    acs_df.drop(columns=['Unemployed Population','Employed Population','Foreigner Population'], inplace=True)
    return acs_df

def extract_and_preprocess_ACS(year, api_key, variables, state_code=None):
    # Extract ACS Data
    acs_df = extract_ACS(api_key=api_key, year=year, variables=variables, state_code=None)
    # Preprocess the ACS Data
    acs_df = preprocess_ACS(acs_df) 
    return acs_df

#%%
def extract_state_mapper():
    url = 'https://www.census.gov/library/reference/code-lists/ansi/ansi-codes-for-states.html'
    response = requests.get(url)
    assert response.status_code ==200
    soup = BeautifulSoup(response.text, 'lxml')
    # print(soup.prettify)

    table = soup.find('table', border="1")
    # print(table.get_text()) #looks correct
    headers = [header.get_text(strip=True) for header in table.find('tr').find_all('th')] # First row contains headers
    rows = table.find_all('tr')
    df = pd.DataFrame(columns=headers)
    for row in rows[1:]: # Skip header row
        row_data = [td.get_text(strip=True) for td in row.find_all('td')]
        # print(row_data)
        length = len(df)
        df.loc[length] = row_data
    df.columns = ['State Name', 'State Code (FIPS)', 'State Code (USPS)']   
    return df 
