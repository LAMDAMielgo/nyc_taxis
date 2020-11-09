

#### NYC TAXI Data (Jan, Apr, July 2015)
NYC_TAXI_ZIP = 'https://storage.googleapis.com/hiring-test/data.zip'
#### Data dictionary of NYC taxi data
NYC_TAXI_DICT = 'https://storage.googleapis.com/hiring-test/data_dictionary_trip_records_yellow.pdf'
#### ACS demopgraphic and socio-economic data by census block group (self-explanatory variables)
NYC_ACS = 'https://storage.googleapis.com/hiring-test/nyc_acs_demographics.csv'
#### NYC census block group geometries
NYC_BLOCK_GEOM = 'https://storage.googleapis.com/hiring-test/nyc_cbg_geoms.geojson'

#### DICT from NYX_TAXI_DICT for data cleaning
VENDORID_DICT = {
    1: 'Creative_Mobile_Technologies',
    2: 'VeriFone_INC',
}

RATECODEID_DICT = {
    1: 'Standard_Rate',
    2: 'JFK',
    3: 'Newark_Westchester',
    4: 'Nassau',
    5: 'Negotiated',
    6: 'Group_Ride',
    99:'UNKNOWN',
}

PAYMENTTYPE_DICT = {
    1: 'Credit_Card',
    2: 'Cash',
    3: 'No_Charge',
    4: 'Dispute',
    5: 'Unknown',
    6: 'Voided_Trip'
}