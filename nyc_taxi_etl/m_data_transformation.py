# for files
import os
import zipfile
import requests
from io import BytesIO
import time

# for data
import re
import numpy as np
import geopandas as gpd
import pandas as pd


# --------------------------------------------------------------------------------------------------- OBJECT TO DATETIME


def str_to_datetime(date_string):
    """
    input: '2015-07-02 14:04:06'  <str>
    output: '2015-07-02T14:04:06' <datetime>
    """
    try:
        date_stt = date_string.split(" ")[0]
        time_str = date_string.split(" ")[1]
        result = np.datetime64(date_stt) + np.timedelta64(time_str.split(":")[0], 'h') + np.timedelta64(
            time_str.split(":")[1], 'm') + np.timedelta64(time_str.split(":")[2], 's')
        return result


    except:
        print(f"!! \t String value does not match:  {date_string}")
        return np.nan


def datetime_transformations(datetime_cols, df, drop_cols):
    """
    input:
    output:
    """
    str_to_datetime_vect = np.vectorize(str_to_datetime)
    print(f"---------------------- Transform OBJECT to DATETIME")

    for Dtime_col in datetime_cols:

        tic = time.perf_counter()
        new_col_name = Dtime_col.split("_")[1] + '_' + Dtime_col.split("_")[2]

        df[new_col_name] = str_to_datetime_vect(df[Dtime_col])
        if drop_cols: df.drop(columns=[Dtime_col], axis=1, inplace=True)

        toc = time.perf_counter();
        mins = (toc - tic) // 60;
        secs = np.around((toc - tic) % 60, 3)

        print(f"{Dtime_col} transformed in {mins}'{secs}''")


# --------------------------------------------------------------------------------------------------- OBJECT TO GEOMETRY


def coord_to_geomObject(df, drop_bool=False):
    """
    input: df[lat], df[long] --> 40  3
    output: df[point]        --> Point(40,3)
    """
    # find pairs
    latitude = sorted([col for col in df.columns if re.findall(r'latitud', col)])
    longitud = sorted([col for col in df.columns if re.findall(r'longitude', col)])
    print(f"Latitude cols found:\t {latitude}");
    print(f"Longitude cols found:\t {longitud}");
    print()

    # error if
    assert len(latitude) == len(longitud)
    # there has to be the same lats as longs for constr points

    for lng, lat in zip(longitud, latitude):

        if lng.split('longitude') == lat.split('latitude'):
            new_col_name = [n.strip() + 'geometry' for n in lng.split('longitude') if len(n) != 0][0]

        df[new_col_name] = gpd.points_from_xy(df[lng], df[lat])
        print(f"Adding new col:\t {new_col_name}\tDone")

    print(f"\nDropping cols {latitude + longitud}\t\t{str(drop_bool).upper()}")
    if drop_bool: df.drop(columns=latitude + longitud, axis=1, inplace=True)
    # no return

# --------------------------------------------------------------------------------------------------- OUTLIERS

def clean_outliers(df, columns, iqr_range):
    """
    Returns df without outliers in defined columns defined range
    """
    print(f"\n---------------------- Droping OUTLIers in {columns}")
    iqr_range_sorted = sorted(iqr_range)

    Q_down = df[columns].quantile(iqr_range_sorted[0])
    Q_up = df[columns].quantile(iqr_range_sorted[1])
    IQR = Q_up - Q_down

    filtr = ((df[columns] < (Q_down - 1.5 * IQR)) | (df[columns] > (Q_up + 1.5 * IQR)))
    df = df[~filtr.any(axis=1)]
    print(f"Done")

    return df

# --------------------------------------------------------------------------------------------------- PRICES TO POSITIVE NUMBS

def abs_var_col(df, cols):
    """
    some numerics cols for prices are negative (all row neg). This is a corrective def
    """
    print(f"\n---------------------- Transform NEG PRICES to POSITIVE PRICES")
    tic = time.perf_counter()

    for col in cols:
        try:
            df[col] = np.abs(df[col])
        except:
            pass

    toc = time.perf_counter()
    mins = (toc - tic) // 60; secs = np.around((toc - tic) % 60, 3)

    print(f" Objects transformed in {mins}'{secs}''")

# --------------------------------------------------------------------------------------------------- GETTING RID OF CASH ERRORS

def total_amount_inconsistency(df, cols_to_sum, total_col):
    """
    drop all cols that are inconsistent in the total amount payed for the trip
    """
    print(f"\n---------------------- Dropping CASH ERRORS")
    tic = time.perf_counter()

    # create col for and drop if aux_col == False
    aux_col = 'amount_equality'

    df[aux_col] = df.apply(lambda x: np.around(np.sum(x[cols_to_sum]), 3) == x[total_col], axis=1)
    df.drop(df[df[aux_col] == False].index, inplace=True)

    # del aux_col
    df.drop(columns=[aux_col], axis=1, inplace=True)
    toc = time.perf_counter()
    mins = (toc - tic) // 60;
    secs = np.around((toc - tic) % 60, 3)

    print(f" Objects transformed in {mins}'{secs}''")
    # no return

