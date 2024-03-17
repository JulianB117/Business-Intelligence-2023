import pandas as pd

# Data Sources:
# Housing Prices: https://www.zillow.com/research/data/
# Unemployment: https://www.kaggle.com/datasets/justin2028/unemployment-in-america-per-us-state
# Federal Interest Rate: https://fred.stlouisfed.org/series/FEDFUNDS

# Import Data
HPrice_Data = pd.read_csv(r'State_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv')
Unemployment_Data = pd.read_csv(r'Unemployment in America Per US State.csv')
InterestRate_Data = pd.read_csv(r'FEDFUNDS.csv')

# Manipulate Data Frames
# Drop unused Columns.
HPrice_Data.drop('StateName', axis=1, inplace=True)
HPrice_Data.drop('RegionType', axis=1, inplace=True)
HPrice_Data.drop('RegionID', axis=1, inplace=True)
HPrice_Data.drop('SizeRank', axis=1, inplace=True)
Unemployment_Data.drop('FIPS Code', axis=1, inplace=True)

# Melt Date Columns into Rows.
HPrice_Data = HPrice_Data.melt(id_vars=['RegionName'], value_vars=HPrice_Data.columns[1:len(HPrice_Data.columns)])

# Converting Date format into separate Year and Month Columns.
HPrice_Data['Year'] = pd.to_datetime(HPrice_Data['variable'])
HPrice_Data['Year'] = HPrice_Data['Year'].dt.strftime('%Y').astype(int)
HPrice_Data['Month'] = pd.to_datetime(HPrice_Data['variable'])
HPrice_Data['Month'] = HPrice_Data['Month'].dt.strftime('%#m').astype(int)
InterestRate_Data['Year'] = pd.to_datetime(InterestRate_Data['DATE'])
InterestRate_Data['Year'] = InterestRate_Data['Year'].dt.strftime('%Y').astype(int)
InterestRate_Data['Month'] = pd.to_datetime(InterestRate_Data['DATE'])
InterestRate_Data['Month'] = InterestRate_Data['Month'].dt.strftime('%#m').astype(int)

# Dropping old columns.
HPrice_Data.drop('variable', axis=1, inplace=True)
InterestRate_Data.drop('DATE', axis=1, inplace=True)

# Dropping 2019+ data as Covid was an anomaly that is not taken into account for this project.
HPrice_Data = HPrice_Data[HPrice_Data['Year'] <= 2018]
InterestRate_Data = InterestRate_Data[InterestRate_Data['Year'] <= 2018]

# Dropping data prior to the year 2000 to condense data and make it more relevant to recent times.
Unemployment_Data = Unemployment_Data[Unemployment_Data['Year'] >= 2000]
InterestRate_Data = InterestRate_Data[InterestRate_Data['Year'] >= 2000]

# Renaming Columns for clarity and for upcoming merge.
HPrice_Data.rename(columns={"RegionName": "StateName"}, inplace=True)
HPrice_Data.rename(columns={"value": "AVGHouseValue"}, inplace=True)
Unemployment_Data.rename(columns={"State/Area": "StateName"}, inplace=True)

# Sorting column order to better facilitate merge.
HPrice_Data = HPrice_Data[['StateName', 'Year', 'Month', 'AVGHouseValue']]

# Convert AVGHouseValue to a float.
HPrice_Data['AVGHouseValue'] = HPrice_Data['AVGHouseValue'].str.replace('$', '')
HPrice_Data['AVGHouseValue'] = HPrice_Data['AVGHouseValue'].str.replace(',', '')
HPrice_Data['AVGHouseValue'] = HPrice_Data['AVGHouseValue'].astype(float)

# Interpolate missing data.
HPrice_Data['AVGHouseValue'] = HPrice_Data['AVGHouseValue'].interpolate(method='linear', limit_direction='forward')

# Data Merging
Merged_Data = Unemployment_Data.merge(HPrice_Data, how='right', on=['StateName','Year','Month']).merge(InterestRate_Data, how='right', on=['Year','Month'])

# Remove '$' and ',' from the entire Merged_Data dataframe.
Merged_Data.replace(',','', regex=True, inplace=True)
Merged_Data.replace('$','', regex=True, inplace=True)

# Convert all numeric values to floats and ints.
Merged_Data['Year'] = Merged_Data['Year'].astype(int)
Merged_Data['Month'] = Merged_Data['Month'].astype(int)
Merged_Data['Total Civilian Non-Institutional Population in State/Area'] = Merged_Data['Total Civilian Non-Institutional Population in State/Area'].astype(float)
Merged_Data['Total Civilian Labor Force in State/Area'] = Merged_Data['Total Civilian Labor Force in State/Area'].astype(float)
Merged_Data["Percent (%) of State/Area's Population"] = Merged_Data["Percent (%) of State/Area's Population"].astype(float)
Merged_Data['Total Employment in State/Area'] = Merged_Data['Total Employment in State/Area'].astype(float)
Merged_Data['Percent (%) of Labor Force Employed in State/Area'] = Merged_Data['Percent (%) of Labor Force Employed in State/Area'].astype(float)
Merged_Data['Total Unemployment in State/Area'] = Merged_Data['Total Unemployment in State/Area'].astype(float)
Merged_Data['Percent (%) of Labor Force Unemployed in State/Area'] = Merged_Data['Percent (%) of Labor Force Unemployed in State/Area'].astype(float)
Merged_Data['AVGHouseValue'] = Merged_Data['AVGHouseValue'].astype(float)
Merged_Data['FEDFUNDS'] = Merged_Data['FEDFUNDS'].astype(float)

# Print and output final data.
print(Merged_Data)
Merged_Data.to_csv('Output.csv', index=False)