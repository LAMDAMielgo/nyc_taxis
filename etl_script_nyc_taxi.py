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


# -------------- globals
from nyc_taxi_etl import m_globals as GL

# data
NYC_TAXI_ZIP, NYC_ACS,  NYC_BLOCK_GEOM = GL.NYC_TAXI_ZIP, GL.NYC_ACS, GL.NYC_BLOCK_GEOM

# dictionaries
VENDORID_DICT, RATECODEID_DICT, PAYMENTTYPE_DICT = GL.VENDORID_DICT, GL.RATECODEID_DICT, GL.PAYMENTTYPE_DICT

# -------------- globals

