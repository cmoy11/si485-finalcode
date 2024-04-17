# Imports
import sqlite3
import os
import pandas as pd
from arcgis.geocoding import geocode
from arcgis.gis import GIS

def get_dataframes():
    df = pd.read_csv('../data/3-2-geocode.csv')         # OG df
    cleaned_df = pd.read_csv('../data/cleaned.csv')     # Brandon's cleaned df from drive
    big_df = pd.read_csv('../data/biggggg-profile.csv') # 9M Profile
    interest_df = pd.read_csv('../data/DART Interest Data 2024 - Known interests for ISR Constituents.csv') # DART Interest Data
    return df, cleaned_df, big_df, interest_df

def UM_donor(row):
    if pd.isnull(row.donor_status) and not pd.isnull(row['UM-Wide\nLifetime Recognition']):
        return 'UM Donor'
    return row.donor_status

def replace(num):
    if type(num) == float:
        return 0
    return float(num.replace(',', '').replace('$', ''))

def new_columns(df):
    df['formatted_address'] = df.apply(lambda x: f"{x['Home Address']}, {x['Home City']}, {x['Home State']} {x['Home Zip']}, {x['Home Country']}", axis=1)
    df['donor_status'] = df['Institute for Social Research\nLifetime Recognition'].map(lambda x: 'ISR Donor', na_action='ignore')
    df['donor_status'] = df.apply(UM_donor, axis=1)
    df['donor_status'].fillna('Non Donor', inplace=True)
    df['Institute for Social Research Lifetime Recognition Numric'] = df['Institute for Social Research\nLifetime Recognition'].map(replace)
    df["UM-Wide Lifetime Recognition Numeric"] = df['UM-Wide\nLifetime Recognition'].map(replace)

    for column in df.columns:
        if df[column].dtype == 'int64' or df[column].dtype == 'float64':
            df[column].fillna(0, inplace=True)
        if df[column].dtype == 'object':
            df[column].fillna('Not Available', inplace=True)   
            
    return df

# Checks if address is already geocoded, geocodes if not
def check_addresses(df):
    """
    Checks and updates geocode information for addresses in the provided DataFrame.
    Merges existing geocoded data with new data, geocodes unprocessed addresses.

    Args:
    df (DataFrame): DataFrame containing the addresses to be checked.

    Returns:
    DataFrame: Updated DataFrame with latitude and longitude information.
    """
    old_addresses = pd.read_csv('[Do not edit] Geocode Data/address_data.csv')
    merged_df = df.merge(old_addresses, how = 'left', left_on = 'Constituent LookupID', right_on = 'Constituent LookupID')
    merged_df['latlong2'] = merged_df.apply(update_addresses, axis=1)
    merged_df['latitude'] = merged_df.latlong2.apply(lambda x:x[0])
    merged_df['longitude'] = merged_df.latlong2.apply(lambda x:x[1])
    # merged_df[['Constituent LookupID', 'formatted_address_y', 'latitude', 'longitude']].to_csv('[Do not edit] Geocode Data/address_data.csv')
    return merged_df

# Checks if address has been changed or has not yet been geocoded
def update_addresses(row):
    """
    Helper function to decide if a row's address needs geocoding.
    
    Args:
    row (Series): A row from DataFrame.

    Returns:
    tuple: Latitude and longitude of the address.
    """
    if row.formatted_address == row.formatted_address_y:
        print('no change to address')
        return row.latitude, row.longitude
    return geocode_us2(row)


def geocode_us2(row):
    """
    #TODO api key will likely be lost mid May
    Uses ArcGIS API to geocode a given address.

    Args:
    row(Series): A row from Datafram containing the address.
    
    Returns:
    typle: Latitude and longitude coordinates.
    """
    gis = GIS(api_key="AAPK393b8da67c074504bdc73ed3037e193bYC_cZGRLP9Tf-592LpmQzMqO27or9AbbzYGHX1e5Xjowm3CnSytMzKUZ5uxjzysf")

    try:
        print('geocoding')
        gc = geocode(row['formatted_address'])
        return gc[0]['location']['y'], gc[0]['location']['x']
    except:
        print('exception')
        return float('nan'), float('nan')

# interest df functions

def make_int_dic(row, dic):
    id = row['Constituent LookupID']
    interest_cat = row['Interest Category']
    interest_subc = row['Interest Subcategory']
    int_level = row['Interest Level']

    if id not in dic.keys():
        dic[id] = {}
        dic[id][interest_cat] = interest_subc
    else:
        dic[id][interest_cat] = (interest_subc, int_level)
    return dic

def add_int_data(row, interest_dic):
    if row['Constituent LookupID'] in interest_dic.keys():
        return interest_dic[row['Constituent LookupID']]
    else:
        return 'No Known Interests'

def call_make_int_dic(interest_df):
    interest_dic = {}
    interest_df.apply(make_int_dic, dic = interest_dic, axis=1)
    return interest_dic


def several_merges(big_df, df, interest_df, cleaned_df):
    """
    Merges multiple datasets into a single DataFrame.

    Returns:
    DataFrame: The combined DataFrame.
    """
    pre_merge = big_df[['Constituent LookupID' ,'Date of Last Recognition Transaction', 'Date of Last Recognition Transaction.1']]
    dic = call_make_int_dic(interest_df)
    pre_merge['Interests'] = pre_merge.apply(add_int_data, axis=1, interest_dic = dic)
    pre_merge = pre_merge.merge(df[['Constituent LookupID']], on='Constituent LookupID', how='left')
    pre_merge = pre_merge.merge(interest_df[['Constituent LookupID', 'Interest Category', 'Interest Subcategory', 'Interest Level']], on='Constituent LookupID', how='left')    
    merged_df = cleaned_df.merge(pre_merge, on='Constituent LookupID', how='left')
    return merged_df

def merged_df_edits(merged_df):
    merged_df = merged_df.rename(columns= {'Date of Last Recognition Transaction': 'Date of Last UM Recognition Transaction', 'Date of Last Recognition Transaction.1':'Date of Last ISR Recognition Transaction'})
    merged_df = merged_df[~merged_df['Constituent LookupID'].duplicated(keep='first')]
    merged_df = merged_df.drop(columns= ['SCU Selected Communicaiton Preference Codes'])
    return merged_df

#==============================
# Affiliation Work
#==============================
def split_affils(s):
    """
    Splits affiliation strings into lists.

    Args:
    s (str): String containing multiple affiliations separated by line breaks.

    Returns:
    list: List of affiliations.
    """

    s = s.split('\n')
    return s

def num_ICPSR_programs(x):
    """
    Counts the number of times a constituent has participated in the ICPSR Summer Program.

    Args:
    x (list): List of affiliations.

    Returns:
    int: Count of ICPSR Summer Program affiliations.
    """
    count = 0
    for a in x:
        if a == 'ISR ICPSR Summer Program':
            count += 1
    return count

def remove_extra_icpsr_affils(x):
    ret = []
    for a in x:
        if a not in ret:
            ret.append(a)
    return ret

def splitting_affil_lists(merged_df):
    merged_df['Constituent Affiliation'] = merged_df['Constituent Affiliation'].apply(split_affils)
    return merged_df

def add_num_ICPSR(merged_df):    
    merged_df['# Times in ICPSR'] = merged_df['Constituent Affiliation'].apply(num_ICPSR_programs)
    merged_df['Constituent Affiliation'] = merged_df['Constituent Affiliation'].apply(remove_extra_icpsr_affils)
    return merged_df

def add_num_affils(merged_df):
    merged_df['Num_Affiliations'] = merged_df.apply(lambda x: len(x['Constituent Affiliation']), axis=1)
    return merged_df

def get_affils(col, L):
    for affil in col:
        if affil not in L:
            L.append(affil)
    return L

def create_affil_lst(merged_df):
    affil_list = []
    merged_df['Constituent Affiliation'].apply(get_affils, L = affil_list)
    return affil_list

def add_affil_cols(row, affil):
    if affil in row['Constituent Affiliation']:
        return 'Affiliated'
    else:
        return 'Not Affiliated'
    

def anon_type(row):
    # if row['A.2'] == 'A':
    #     return "A.2"
    if row['A.1'] == 'A':
        return "A.1"
    elif row['A'] == 'A':
        return "A"
    else:
        return 'None'

def add_anon_type(merged_df):
    merged_df['Anonymous_Type'] = merged_df.apply(anon_type, axis = 1)
    return merged_df

#Should only need to call below function for all affiliation work...
def affiliation_work(merged_df):
    splitting_affil_lists(merged_df)
    add_num_ICPSR(merged_df)
    add_num_affils(merged_df)
    affil_lst = create_affil_lst(merged_df)
    for x in affil_lst:
        merged_df['Affiliation: ' + x] = merged_df.apply(add_affil_cols, affil = x, axis = 1)
    add_anon_type(merged_df)
    merged_df = merged_df.drop(columns=['Constituent Affiliation'])
    return merged_df
    
# Creates individual CSV files for each affiliation
def create_affiliation_files(df):
    affiliations = ['Affiliation: ISR ICPSR Consort Pol/ Soc Res', 'Affiliation: ISR ICPSR Summer Program', 'Affiliation: ISR Survey Research Center', 'Affiliation: ISR Historic Affiliate', 'Affiliation: ISR Event Attendee', 'Affiliation: ISR Ret Support Staff List', 'Affiliation: ISR Social Psychology', 'Affiliation: ISR Res Ctr for Group Dynamics', 'Affiliation: ISR Ctr for Political Studies', 'Affiliation: Friend', 'Affiliation: ISR Population Studies Center Trainee', 'Affiliation: ISR Dissertation Affiliates', 'Affiliation: ISR Next Gen Awardee', 'Affiliation: ISR Organizational Psych', 'Affiliation: ISR Top Lifetime Donor']
    
    for affiliation in affiliations:
        sub_df = df[df[affiliation] == 'Affiliated']
        formatted_affiliation = affiliation.replace('Affiliation: ', '').replace(' ', '-').replace('/', '-')
        sub_df.to_csv(f'affiliation_layers/{formatted_affiliation}-layer.csv')


def main():
    # get data
    # df, cleaned_df, big_df, interest_df = get_dataframes()
    df = pd.read_excel('mprofile.xlsx')

    # geocoding...
    print('geocoding')
    df = new_columns(df)
    df = check_addresses(df)   
    
    # merging / post-merge cleaning
    # merged_df = several_merges(big_df, df, interest_df, cleaned_df)
    print('merging')
    merged_df = merged_df_edits(df)
    
    # affiliations
    print('coding affiliations')
    merged_df = affiliation_work(merged_df)
    
    # write final csv
    merged_df.to_csv('new_master_dataset.csv')

    # write layer data
    print('creating affilitaion files')
    df = create_affiliation_files(merged_df)

    print('done')

if __name__ == '__main__':
    main()