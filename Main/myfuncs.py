#%% imports
import requests
from bs4 import BeautifulSoup
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib as mpl

#%% 
def extract_FRED_data(url,variable):
    '''
    Extract Economic Data from FRED API
    '''
    response = requests.get(url)
    assert response.status_code == 200, "GET Request Failed"

    soup = BeautifulSoup(response.text, 'lxml')
    table = soup.find('table')

    # Extract data based on inspection of meta data
    meta_headers = table.find('thead').find_all('tr')[1].find_all('th') # th elements of second header row
    headers = [meta_header.get_text(strip=True) for meta_header in meta_headers]
    # headers >> ['', 'Name', '2023', 'PrecedingPeriod', 'Year Agofrom Period']

    unit = table.find('thead').find_all('tr')[0].find('th', id='table-unit-heading').get_text(strip=True) #th element with specified id of first header row

    body_rows = table.find('tbody').find_all('tr') # all rows in the table body

    # Tabulate the content into a df
    df = pd.DataFrame(columns=headers[1:-1]) # Skip first header, an empty string for checkbox
    for body_row in body_rows:
        # Extract row data
        row_data = [] # a list to store each row element
        state_name = body_row.find('th').find('span',class_='fred-rls-elm-nm').get_text(strip=True)
        meta_values = body_row.find_all('td', class_='fred-rls-elm-vl-td')[:-1]
        values = [meta_value.get_text(strip=True) for meta_value in meta_values]
        row_data.append(state_name)
        row_data += values
        # Add row data to the df
        length = len(df)
        df.loc[length] = row_data

    df.rename(columns= {
        'Name':'State Name',
        # '2023': '2023' #f'{variable} [{unit}] (2023)',
        'PrecedingPeriod': '2022' #f'{variable} [{unit}] (2022)'      
    }, inplace=True)   

    df_2023 = df[['State Name','2023']].reset_index(drop=True).copy()
    df_2023['Year'] = 2023
    df_2023.rename(columns={'2023':f'{variable} [{unit}]'},inplace=True)

    df_2022 = df[['State Name','2022']].reset_index(drop=True).copy()
    df_2022['Year'] = 2022
    df_2022.rename(columns={'2022':f'{variable} [{unit}]'},inplace=True)
    
    df_final = pd.concat([df_2023, df_2022], ignore_index=True)

    # Remove commas from the numbers and convert to float
    df_final[f'{variable} [{unit}]'] = df_final[f'{variable} [{unit}]'].str.replace(',','').astype(float)
    
    return df_final

#%%
def extract_ACS_data(api_key, year, state_code=None):
    '''
    Extract state-level ACS data using a session (if provided).
    '''
    # Selected economic and socio-demographic variables
    variables = ['B19013_001E','B19301_001E','B23025_005E','B23025_003E','B19083_001E','B01003_001E','B01002_001E','B05002_013E','B25077_001E']
    
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

def extract_and_preprocess_ACS_data(year, api_key, state_code=None):
    acs_df = extract_ACS_data(api_key=api_key, year=year, state_code=None)
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

#%% 
def extract_airport_info():
    url = 'https://www.transtats.bts.gov/ONTIME/AirportInfo.html'
    response = requests.get(url)
    assert response.status_code == 200
    soup = BeautifulSoup(response.text, 'lxml')

    table = soup.find('table')
    rows = table.find_all('tr')

    # Extract table data into a df
    header_row = rows[0]
    meta_headers = header_row.find_all('th')
    headers = [meta_header.get_text(strip=True) for meta_header in meta_headers]
    df_airports = pd.DataFrame(columns=headers)
    for row in rows[1:]: #skip header row
        row_data = [td.get_text(strip=True) for td in row.find_all('td')]
        newrow_idx = len(df_airports) # newrow_idx = [lastrow_idx] +1 = [len(df)-1] +1 = len(df)
        df_airports.loc[newrow_idx] = row_data
    # df_airports
    col_name = 'Airport/City/State Name'
    # df_airports[col_name] = df_airports['Airport/City/State Name'].str.strip().str.rstrip(',') #doesn't work, still have additional column of "None" values -> use n=2 instead
    new_cols = col_name.split('/') # Expand Airport/City/State Name into three separate columns
    df_airports[new_cols] = df_airports[df_airports.columns[1]].str.split(',', n=2, expand=True) #splits the string only at the first two commas
    df_airports.drop(columns=[col_name],inplace=True)
    df_airports.rename(columns={
        'State Name':'State Code (USPS)',
        'Airport': 'Airport Name'
        }, 
        inplace=True
    )
    df_airports['State Code (USPS)'] = df_airports['State Code (USPS)'].str.strip()
    return df_airports
#%%
def plot_timeseries(df,y='Flight Count', year=None, hue='Day of Week'):
    # Use the tab10 colormap
    tab10 = mpl.colormaps['tab10']

    if hue=='Day of Week':
        # Assign tab10 colors to each day of the week manually
        day_of_week_palette = {
            'Mon': tab10(0),
            'Tue': tab10(1),
            'Wed': tab10(2),
            'Thu': tab10(3),
            'Fri': tab10(4),
            'Sat': tab10(5),
            'Sun': tab10(6)
        }
        palette = day_of_week_palette
    else:
        palette = 'tab10'

    if year:
        tmp = df[df['Year']==year]
    else: 
        tmp = df.copy()
    sns.lineplot(data=tmp, x=tmp.index, y=y,hue=hue,palette=palette)
    plt.legend(loc='lower left')
    plt.show()
# %%
