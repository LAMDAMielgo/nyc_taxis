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

# -------------- imports
from nyc_taxi_etl.m_data_transformation import datetime_transformations
from nyc_taxi_etl.m_data_transformation import coord_to_geomObject
from nyc_taxi_etl.m_data_transformation import abs_var_col
from nyc_taxi_etl.m_data_transformation import total_amount_inconsistency
from nyc_taxi_etl.m_data_transformation import clean_outliers

# ---------------------------------------------------------------------------------------------------  DEFS

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
    # Points have outliers
    # OUTLIERS
    df_without_outliers = clean_outliers(df=df_to_transform,
                                         columns=geom_cols,
                                         iqr_range=[0.15, 0.85])
    # OBJECT TO GEOMETRY
    coord_to_geomObject(df_without_outliers, drop_bool=True)

    # 2
    # NUMERIC COLS IN ABSOLUTES
    abs_var_col(df_without_outliers, cols=abs_cols)

    # 3
    # CATEGORICAL COLUMNS
    df_without_outliers[cat_cols[0]] = df_without_outliers[cat_cols[0]].apply(lambda col: dict_ratecodeID[col])
    df_without_outliers[cat_cols[1]] = df_without_outliers[cat_cols[1]].apply(lambda col: dict_paymenttype[col])

    #  4
    # Drops columns with inconsistency in amount
    #total_amount_inconsistency(df = df_to_transform,
    #                           cols_to_sum = num_cols_to_sum,
    #                           total_col = total_payment_col)

    if drop_cols: df_without_outliers.drop(columns=cols_to_drop, axis=1, inplace=True)
    else: pass

    return df_without_outliers

