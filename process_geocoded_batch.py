#This script processes geocoded addresses and attempts to pull the most correct match out for addresses that are geocoded multiple time (ie. same address_id but different addresses)
#Additional work is needed to preprocess to split out PO Boxes and Unit #s, etc, as these often confuse the Google Geocoding API.
#N.B. There is a Google Places type override for pharmacies that may not be needed for your purposes.

import numpy as np
import os.path
import pandas as pd
import requests
import logging
import time

# Create logger
logger = logging.getLogger("root")
logger.setLevel(logging.DEBUG)

# Create console handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# Add console handler
logger.addHandler(ch)

#------------------ CONFIGURATION -------------------------------

# Set your input file here
FILENAME_INPUT = 'AddressesForGeocoding_output.csv'

# Set your output file name here.
FILENAME_OUTPUT = 'AddressesForGeocoding_output_final.csv'

# Specify the segments/splitcounts for printing
SPLITCOUNT_PRINT = 50

# Fields from import file/dataframe to be used
ADDRESS_IDENTIFIER_FIELD = 'address_id'
ACCURACY_FIELD = 'accuracy'
ADDRESS_FIELD = 'formatted_address'
LATITUDE_FIELD = 'latitude'
LONGITUDE_FIELD = 'longitude'
STATUS_FIELD = 'status'
TYPE_FIELD = 'type'

#------------------ DATA LOADING --------------------------------

# Read the data to a Pandas Dataframe
df = pd.read_csv(FILENAME_INPUT, encoding='utf8')

#------------------ FUNCTION DEFINITIONS ------------------------

def determine_replace_master(df_row_master, df_row):
    """
    Get first geocode result from Google Maps Geocoding API.
    
    @param address: String address as accurate as possible. 
                        If including the components parameter, exclude those details from the address.
                        Address should not include landmarks, company names, etc.
    @param api_key: String API key from Google. 
    @param return_full_response: Boolean to indicate if you'd like to return the full response from Google. 
                                    This useful if you'd like additional location details for storage or parsing later.

    Future Work: Return a dataframe instead of a dict.
    """

    # Compare and return the better entry

    row_master_type = df_row_master['type'].iloc[0]
    row_master_accuracy = df_row_master['accuracy'].iloc[0]
    row_master_address = df_row_master['formatted_address'].iloc[0]

    row_type = df_row['type'].iloc[0]
    row_accuracy = df_row['accuracy'].iloc[0]
    row_address = df_row['formatted_address'].iloc[0]

    skip_compare = False
    if str(row_master_type) == 'nan' or str(row_master_accuracy) == 'nan' or str(row_master_address) == 'nan':
        replace_master = True
        skip_compare = True
    if str(row_type) == 'nan' or str(row_accuracy) == 'nan' or str(row_address) == 'nan' :
        replace_master = False
        skip_compare = True

    if not skip_compare:

        # Adjust type to allow for string match on 'pharmacy'
        # We have a type override because we are interested in pharmacy locations in this template.
        type_override = 'pharmacy'
        if type_override in row_master_type:
            row_master_type = type_override
        if type_override in row_type:
            row_type = type_override

        type_priority_list = ['pharmacy', 'subpremise', 'premise', 'street']
        accuracy_priority_list = ['ROOFTOP', 'RANGE_INTERPOLATED', 'GEOMETRIC_CENTER', 'APPROXIMATE']

        try:
            row_master_type_index = type_priority_list.index(row_master_type)
        except ValueError:
            row_master_type_index = len(type_priority_list)
        try:
            row_master_accuracy_index = accuracy_priority_list.index(row_master_accuracy)
        except ValueError:
            row_master_accuracy_index = len(accuracy_priority_list)  

        try:
            row_type_index = type_priority_list.index(row_type)
        except ValueError:
            row_type_index = len(type_priority_list)        
        try:
            row_accuracy_index = accuracy_priority_list.index(row_accuracy)
        except ValueError:
            row_accuracy_index = len(accuracy_priority_list)   

        if row_type_index == row_master_type_index:   
            # Matched Type
            if row_accuracy_index == row_master_accuracy_index:
                # Matched Accuracy
                if row_address != row_master_address:
                    print('Different addresses but same type and accuracy')
                replace_master = True
            elif row_accuracy_index < row_master_accuracy_index:
                replace_master = True
                pass
            elif row_accuracy_index > row_master_accuracy_index:
                replace_master = False
        elif row_type_index < row_master_type_index:
            replace_master = True
            pass
        elif row_type_index > row_master_type_index:
            replace_master = False

    return replace_master

#------------------ PROCESSING LOOP -----------------------------

df_duplicated = df[df.duplicated(ADDRESS_IDENTIFIER_FIELD, keep=False)]
list_duplicated = df_duplicated[ADDRESS_IDENTIFIER_FIELD].tolist()
list_duplicated = list(set(list_duplicated))

# Not duplicated, are allowed to have non 'OK' statuses (will be checked manually)
df_notduplicated = pd.concat([df, df_duplicated]).drop_duplicates(keep=False)

# Create a dataframe to hold results
df_master = None

for idx_address, address_identifier in enumerate(list_duplicated):

    print(address_identifier)

    # df_temp stores the duplicated geocode rows for address_identifier
    df_temp = df_duplicated.loc[df_duplicated[ADDRESS_IDENTIFIER_FIELD] == address_identifier]
    df_temp.reset_index(drop=True,inplace=True)

    df_temp_recordcount = df_temp.shape[0]

    for idx_row in range(0, df_temp_recordcount):

        df_row = df_temp.iloc[[idx_row]]

        if idx_row == 0:
            df_row_master = df_row
        else:
            replace_master = determine_replace_master(df_row_master, df_row)
            if replace_master == True:
                df_row_master = df_row

    #save df_row_master to a final X
    if df_master is None:
        df_master = df_row_master
    else:
        df_master = df_master.append(df_row_master, ignore_index=True)

# Save results to file 
df_master = pd.concat([df_master,df_notduplicated], ignore_index=True)
df_master.to_csv(FILENAME_OUTPUT, encoding='utf8', index=False)

print('Finished!')