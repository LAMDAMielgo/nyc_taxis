# -------------- files
import os
import zipfile
import requests
from io import BytesIO
import time

# -------------- for data
import re
import numpy as np
import pandas as pd
import geopandas as gpd

# -------------- globals
from nyc_taxi_etl import m_globals as GL

# data
NYC_TAXI_ZIP, NYC_ACS,  NYC_BLOCK_GEOM = GL.NYC_TAXI_ZIP, GL.NYC_ACS, GL.NYC_BLOCK_GEOM

# dictionaries
VENDORID_DICT, RATECODEID_DICT, PAYMENTTYPE_DICT = GL.VENDORID_DICT, GL.RATECODEID_DICT, GL.PAYMENTTYPE_DICT

# -------------- globals
from nyc_taxi_etl.m_data_transformation import datetime_transformations
from nyc_taxi_etl.m_data_transformation import coord_to_geomObject
from nyc_taxi_etl.m_data_transformation import abs_var_col
from nyc_taxi_etl.m_data_transformation import clean_outliers
from nyc_taxi_etl.m_data_transformation import total_amount_inconsistency

def data_etl(df_to_transform, dict_ratecodeID, dict_paymenttype, drop_cols=True):
    """
    input
    output
    """
    datetime_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    geom_cols = ['pickup_latitude', 'pickup_longitude', 'dropoff_longitude', 'dropoff_latitude']
    abs_cols = ['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'improvement_surcharge', 'tolls_amount',
                'total_amount']
    cat_cols = ['RatecodeID', 'payment_type']
    num_cols_to_sum = ['fare_amount', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'mta_tax', 'extra']
    total_payment_col = 'total_amount'
    cols_to_drop = ['VendorID', 'store_and_fwd_flag', 'total_amount']

    existing_cols = set(datetime_cols + geom_cols + abs_cols + cat_cols + num_cols_to_sum + [total_payment_col] + cols_to_drop)

    assert existing_cols not in df_to_transform.columns.tolist()


    # 1.1
    # OBJECT TO DATETIME
    datetime_transformations(datetime_cols=datetime_cols,
                             df=df_to_transform, drop_cols=True)
    # 1.2
    # GET RID OF OUTLIERS
    df_to_transform = clean_outliers(df = df_to_transform,
                                     columns = geom_cols,
                                     iqr_range = [0.05, 0.95])
    # OBJECT TO GEOMETRY
    coord_to_geomObject(df_to_transform, drop_bool=True)

    # 2
    # NUMERIC COLS IN ABSOLUTES
    abs_var_col(df_to_transform, cols=abs_cols)

    # 3
    # CATEGORICAL COLUMNS
    df_to_transform[cat_cols[0]] = df_to_transform[cat_cols[0]].apply(lambda col: dict_ratecodeID[col])
    df_to_transform[cat_cols[1]] = df_to_transform[cat_cols[1]].apply(lambda col: dict_paymenttype[col])

    #  4
    # Drops columns with inconsistency in amount
    #total_amount_inconsistency(df = df_to_transform,
    #                           cols_to_sum = num_cols_to_sum,
    #                           total_col = total_payment_col)

    if drop_cols: df_to_transform.drop(columns=cols_to_drop, axis=1, inplace=True)
    else: pass

    return df_to_transform

# -------------- globals
from nyc_taxi_etl.m_opening import request_info_from_ZIP
from nyc_taxi_etl.m_opening import getting_df_fromZip

def main(zipfile_dir, drop_columns_in_cleaning, ratecode_dict, payment_dict):
    """

    :param zipfile_to_workwith:
    :return:
    """
    info_from_zip = request_info_from_ZIP(zipfile_dir = zipfile_dir)

    frames = getting_df_fromZip(zipfile_info = info_from_zip,
                                minLen_toDisgard = 7,
                                first_row = 5,
                                num_rows = 35)

    for i, frame in enumerate(frames[::2]):
        print(f"\n\n{i} Frame ---------------------------------------------")
        tic = time.perf_counter()
        data_etl(df_to_transform=frame,
               dict_ratecodeID = ratecode_dict,
               dict_paymenttype = payment_dict,
               drop_cols= drop_columns_in_cleaning)

        toc = time.perf_counter()
        mins = (toc - tic) // 60; secs = np.around((toc - tic) % 60, 3)
        print(f" Frame cleaned in  transformed in {mins}'{secs}''")

    return frames[::2]
# -------------- globals

frames = main( zipfile_dir = NYC_TAXI_ZIP,
         drop_columns_in_cleaning = True,
         ratecode_dict = RATECODEID_DICT,
         payment_dict = PAYMENTTYPE_DICT)

df_all = pd.concat(frames)
print(df_all.head(2))