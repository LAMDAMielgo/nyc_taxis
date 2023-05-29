META={
    "head"      : "File for configuration for ETL",
    "source_name"   : "yellow_tripdata",
    "key" : {
        "payment_type" : {
            "1" : "credit_card",
            "2" : "cash",
            "3" : "no_charge",
            "4" : "dispute",
            "5" : "unknown",
            "6" : "voided trip"
        },
        "RateCodeID" : {
            "1" : "standard_rate",
            "2" : "jfk",
            "3" : "newark",
            "4" : "nassau_or_westchester",
            "5" : "negotiated_fare",
            "6" : "group_fare"
        },
        "VendorID" : {
            "1" : "Creative Mobile Technologies, LLC.",
            "2" : "VeriFone Inc."
        },
        "store_and_fwd_flag": {
            "Y" : True, "N": False
        }
    },
    "raw" : {
        "encoding"  : "utf-8",
        "delimiter" : "\t",
        "schema"    : {
            "VendorID"              : { "dtype": "int",       "comment" : "TPEP Provider"},  
            "tpep_pickup_datetime"  : { "dtype": "datetime",  "comment" : ""},
            "tpep_dropoff_datetime" : { "dtype": "datetime",  "comment" : ""},
            "passenger_count"       : { "dtype": "int",       "comment" : "Num passenger per ride"},
            "trip_distance"         : { "dtype": "float",     "comment" : "miles, by taximeter"},
            "pickup_longitude"      : { "dtype": "float",     "comment" : "x geom"},
            "pickup_latitude"       : { "dtype": "float",     "comment" : "y geom"},
            "RateCodeID"            : { "dtype": "int",       "comment" : "usd/meter applied"},
            "store_and_fwd_flag"    : { "dtype": "bool",      "comment" : "type of data processing from device"},
            "dropoff_longitude"     : { "dtype": "float",     "comment" : ""},
            "dropoff_latitude"      : { "dtype": "float",     "comment" : ""},
            "payment_type"          : { "dtype": "int",       "comment" : ""},
            "fare_amount"           : { "dtype": "float",     "comment" : "usd/fare"},
            "extra"                 : { "dtype": "float",     "comment" : "usd"},
            "mta_tax"               : { "dtype": "float",     "comment" : "usd, cte"},
            "tip_amount"            : { "dtype": "float",     "comment" : "usd"},
            "tolls_amount"          : { "dtype": "float",     "comment" : "usd"},
            "improvement_surcharge" : { "dtype": "float",     "comment" : "usd"},
            "total_amount"          : { "dtype": "float",     "comment" : "usd"}
        },
       "dateformat" : "%Y-%m-%d %H:%M:%S"
    },
    "staging" : {
        "delimiter" : "|",
        "type"      : "str",
        "dateisoformat" : "%Y-%m-%d",
        "bq_schema" : {
            
        }
    }
}
