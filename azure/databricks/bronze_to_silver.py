from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType
)

# Define the paths for the bronze and silver layers in Azure Data Lake Storage (ADLS)
BRONZE_PATH = "abfss://transactions@strtransactions.dfs.core.windows.net/bronze/"
SILVER_PATH = "abfss://transactions@strtransactions.dfs.core.windows.net/silver/"

# Read new JSON transactions from the Bronze layer as a streaming DataFrame wih a schema that matches the nested structure of the JSON data : It's better to define the schema of our json data.
def read_bronze_stream():

    schema = StructType([
        StructField("customer", StructType([
            StructField("customer_id", StringType()),
            StructField("age", IntegerType()),
            StructField("gender", StringType()),
            StructField("annual_income", StringType()),
            StructField("credit_score", IntegerType()),
            StructField("number_of_cards", IntegerType())
        ])),

        StructField("card", StructType([
            StructField("card_id", StringType()),
            StructField("card_brand", StringType()),
            StructField("card_type", StringType()),
            StructField("has_chip", BooleanType()),
            StructField("credit_limit", StringType())
        ])),

        StructField("transaction", StructType([
            StructField("transaction_id", StringType()),
            StructField("timestamp", StringType()),
            StructField("amount", StringType()),
            StructField("card_usage_method", StringType()),
            StructField("merchant_id", StringType()),
            StructField("merchant_city", StringType()),
            StructField("merchant_state", StringType()),
            StructField("postal_code", StringType()),
            StructField("merchant_category_code", StringType()),
            StructField("transaction_error", StringType())
        ]))
    ])

    df_bronze = (
        spark.readStream
        .format("json")
        .schema(schema)
        .load(BRONZE_PATH)
    )

    return df_bronze
#--------------------------------------------------------------
# Flatten the nested transaction, customer, and card structures
#--------------------------------------------------------------

def flatten_transactions(df_bronze):
    df_flatted = df_bronze.select(
        "transaction.*",
        "customer.*",
        "card.*"
    ).withColumnRenamed( # Change column names to avoid conflicts
        "credit_limit",
        "card_limit"
    )

    return df_flatted

# ==> Apply Silver transformations to the flattened DataFrame
def transform_to_silver(df):

    # Put transaction_id as the first column
    columns = ["transaction_id"] + [col for col in df.columns if col != "transaction_id"]
    df = df.select(columns)

    # Remove duplicates based on transaction_id
    df = df.dropDuplicates(["transaction_id"])

    #-----------------------------------------------------------------
    # Standardize data types across PostgreSQL and Stripe transactions
    #-----------------------------------------------------------------
    
    # 1. Standarize string columns
    string_columns=["transaction_id", "customer_id", "card_id","merchant_id","postal_code","merchant_category_code","gender", "card_brand","card_usage_method","card_type","merchant_city","merchant_state","transaction_error"]

    for columns in string_columns :
        df = df.withColumn(
            columns,
            F.col(columns).cast("string")
        )

    # 2. Standarize integer columns
    integer_columns=["age","credit_score","number_of_cards"]

    for column in integer_columns :
        df = df.withColumn(
            column,
            F.col(column).cast("integer")
        )

    # 3. Standarize decimal columns
    decimal_columns=["amount","annual_income","card_limit"]

    for column in decimal_columns :
        df = df.withColumn(
            column,
            F.col(column).cast("decimal(20,2)")
        )

    # 4. Standarize timestamp columns
    df = df.withColumn(
        "timestamp",
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss")
    )

    # 5. Standardize boolean columns
    df = df.withColumn(
        "has_chip",
        F.col("has_chip").cast("boolean")
        )

    #-----------------------------
    # Normalize categorical values
    #-----------------------------

    # 1. Normalize US state abbreviations to full state names

    state_mapping = { 
        "AA": "Armed Forces Americas",
        "AK": "Alaska",
        "AL": "Alabama",
        "CA": "California",
        "CO": "Colorado",
        "CT": "Connecticut",
        "DC": "District of Columbia",
        "DE": "Delaware",
        "FL": "Florida",
        "GA": "Georgia",
        "HI": "Hawaii",
        "IA": "Iowa",
        "ID": "Idaho",
        "IL": "Illinois",
        "IN": "Indiana",
        "KS": "Kansas",
        "KY": "Kentucky",
        "LA": "Louisiana",
        "MA": "Massachusetts",
        "MD": "Maryland",
        "ME": "Maine",
        "MI": "Michigan",
        "MN": "Minnesota",
        "MO": "Missouri",
        "MS": "Mississippi",
        "MT": "Montana",
        "NC": "North Carolina",
        "ND": "North Dakota",
        "NE": "Nebraska",
        "NH": "New Hampshire",
        "NJ": "New Jersey",
        "NM": "New Mexico",
        "NV": "Nevada",
        "NY": "New York",
        "OH": "Ohio",
        "OK": "Oklahoma",
        "OR": "Oregon",
        "PA": "Pennsylvania",
        "RI": "Rhode Island",
        "SC": "South Carolina",
        "SD": "South Dakota",
        "TN": "Tennessee",
        "TX": "Texas",
        "UT": "Utah",
        "VA": "Virginia",
        "VT": "Vermont",
        "WA": "Washington",
        "WI": "Wisconsin",
        "WV": "West Virginia",
        "WY": "Wyoming",
        "AZ": "Arizona"
    }

    mapping_expr = F.create_map(
        [F.lit(x) for x in sum(state_mapping.items(), ())]
    )

    df = df.withColumn(
        "merchant_state",
        F.coalesce(
            mapping_expr[F.col("merchant_state")],
            F.col("merchant_state")
        )
    )

    # 2. Normalize other categorical columns
    categorical_columns = ["gender", "card_brand", "card_usage_method", "card_type", "merchant_city"]

    for column in categorical_columns:
        df = df.withColumn(
            column,
            F.initcap(F.col(column))
        )

    #----------------------
    # Handle missing values
    #----------------------

    # 1. Normalize NULL representations
    for column in df.columns:
        df = df.withColumn(
            column,
            F.when(
                F.col(column).isNull() |
                (F.trim(F.col(column).cast("string")) == "") |
                (F.lower(F.trim(F.col(column).cast("string"))) == "null"),
                None
            ).otherwise(F.col(column))
        )

    # 2. Drop transactions with  critical missing values
    critical_columns = ["transaction_id", "customer_id", "card_id", "merchant_id"]
    
    df = df.dropna(subset=critical_columns) # (subset = ....) to specify the columns to check for null values

    #-------------------------------------------
    # Remove duplicates based on transaction_id
    #-------------------------------------------
    
    df = df.dropDuplicates(["transaction_id"])

    #----------------------------
    # Validate transaction values
    #----------------------------

    # 1. Remove transactions with negative amounts
    df = df.filter(F.col("amount") > 0)

    # 2. Validate customer age (between 16 and 100)
    df = df.filter((F.col("age").isNull()) | ((F.col("age") >=16) & (F.col("age")<=100)))

    # 3. Validate credit score (between 300 and 850)
    df = df.filter((F.col("credit_score").isNull()) | ((F.col("credit_score") >= 300) & (F.col("credit_score") <= 850)))

    # 5. Validate number of cards
    df = df.filter((F.col("number_of_cards").isNull()) | (F.col("number_of_cards") >= 0))

    # 6. Validate annual income
    df = df.filter((F.col("annual_income").isNull()) | (F.col("annual_income") >= 0))

    # 7. Validate card limit
    df = df.filter((F.col("card_limit").isNull()) | (F.col("card_limit") >= 0))

    # 8.Validate transaction timestamp
    df = df.filter(
        (F.col("timestamp").isNotNull()) &
        (F.year(F.col("timestamp")) >= 2026) &
        (F.month(F.col("timestamp")).between(1, 12)) &
        (F.dayofmonth(F.col("timestamp")).between(1, 31)) &
        (F.hour(F.col("timestamp")).between(0, 23)) &
        (F.minute(F.col("timestamp")).between(0, 59)) &
        (F.second(F.col("timestamp")).between(0, 59))
    )


    return df       

# Write the transformed DataFrame to the Silver layer in Delta format
def write_to_silver(df):
    query = (
        df.writeStream      # Create a streaming write  
        .format("delta")     # Write in Delta format
        .outputMode("append")     # Add new transactions without replacing existing data
        .option("checkpointLocation", SILVER_PATH + "_checkpoint/")   # Store streaming progress
        .trigger(availableNow=True) 
        .start(SILVER_PATH)    # Start writing to the Silver path
    )

    return query

# Main function to orchestrate and execute the Bronze to Silver pipeline transformations
if __name__ == "__main__":

    # Read the Bronze stream
    df_bronze = read_bronze_stream()

    # Flatten the nested structures
    df_flattened = flatten_transactions(df_bronze)

    # Transform to Silver layer
    df_silver = transform_to_silver(df_flattened)

    # Write to Silver layer
    query = write_to_silver(df_silver)

    # Keep the streaming query running until termination~
    query.awaitTermination()