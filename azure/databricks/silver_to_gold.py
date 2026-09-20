from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable


# Define the paths for the Silver and Gold layers in Azure Data Lake Storage (ADLS)
SILVER_PATH = "abfss://transactions@strtransactions.dfs.core.windows.net/silver/"
GOLD_PATH = "abfss://transactions@strtransactions.dfs.core.windows.net/gold/"


# =============================================================================
# Read Silver Layer
# =============================================================================

def read_silver_stream():

    df_silver = (
        spark.readStream
        .format("delta")
        .load(SILVER_PATH)
    )

    return df_silver


# =============================================================================
# 1. Fact Transformations and Writes
# =============================================================================

# -----------------------------------------------------------------------------
# Transform Fact Transactions
# -----------------------------------------------------------------------------

def transform_fact_transactions(df):

    # Extract analytical time attributes from the transaction timestamp

    df = (
        df
        .withColumn("transaction_date", F.to_date("timestamp"))
        .withColumn("transaction_year", F.year("timestamp"))
        .withColumn("transaction_month", F.month("timestamp"))
        .withColumn("transaction_day", F.dayofmonth("timestamp"))
        .withColumn("transaction_hour", F.hour("timestamp"))
        .withColumn("transaction_minute", F.minute("timestamp"))
        .withColumn(
            "day_of_week",
            F.date_format(F.col("timestamp"), "EEEE")
        )
    )

    # Create a transaction success indicator based on transaction errors

    df = (
        df
        .withColumn(
            "is_successful",
            F.when(
                F.col("transaction_error").isNull(),
                1
            ).otherwise(0)
        )
    )

    # Select the columns that define the transaction fact dataset

    fact_transactions = df.select(

        # Transaction identifier
        "transaction_id",

        # Transaction timestamp
        "timestamp",

        # Analytical time attributes
        "transaction_date",
        "transaction_year",
        "transaction_month",
        "transaction_day",
        "transaction_hour",
        "transaction_minute",
        "day_of_week",

        # Transaction measure
        "amount",

        # Transaction status indicator
        "is_successful",

        # Customer attributes
        "customer_id",
        "age",
        "gender",
        "annual_income",
        "credit_score",
        "number_of_cards",

        # Card attributes
        "card_id",
        "card_brand",
        "card_type",
        "has_chip",
        "card_limit",

        # Merchant attributes
        "merchant_id",
        "merchant_city",
        "merchant_state",
        "postal_code",
        "merchant_category_code",

        # Additional transaction attributes
        "card_usage_method",
        "transaction_error"
    )

    return fact_transactions


# -----------------------------------------------------------------------------
# Merge Fact Transactions into Gold 
# -----------------------------------------------------------------------------

def merge_fact_transactions(batch_df, batch_id):

    window_spec = Window.partitionBy("transaction_id").orderBy(
        F.col("timestamp").desc()
    )

    batch_df = (
        batch_df
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    target_path = GOLD_PATH + "fact_transactions/"

    # Create the Delta table on the first execution
    if not DeltaTable.isDeltaTable(spark, target_path):

        (
            batch_df.write
            .format("delta")
            .mode("overwrite")
            .save(target_path)
        )

    else:

        delta_table = DeltaTable.forPath(
            spark,
            target_path
        )

        (
            delta_table.alias("target")
            .merge(
                batch_df.alias("source"),
                "target.transaction_id = source.transaction_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


# =============================================================================
# 2. Summary Transformations and Writes
# =============================================================================

# -----------------------------------------------------------------------------
# Transform Amount Summary
# -----------------------------------------------------------------------------

def transform_amount_summary(df):

    # Daily summary
    daily_amount_summary = (
        df
        .groupBy("transaction_date")
        .agg(
            F.sum("amount").alias("total_transaction_amount"),
            F.avg("amount").alias("average_transaction_amount"),
            F.min("amount").alias("minimum_transaction_amount"),
            F.max("amount").alias("maximum_transaction_amount")
        )
    )

    # Monthly summary
    monthly_amount_summary = (
        df
        .groupBy("transaction_month", "transaction_year")
        .agg(
            F.sum("amount").alias("total_transaction_amount"),
            F.avg("amount").alias("average_transaction_amount"),
            F.min("amount").alias("minimum_transaction_amount"),
            F.max("amount").alias("maximum_transaction_amount")
        )
    )

    return daily_amount_summary, monthly_amount_summary


# =============================================================================
# Write Amount Summaries to Gold
# =============================================================================

def write_amount_summaries_to_gold(
    daily_amount_summary,
    monthly_amount_summary
):

    # Write Daily Amount Summary
    (
        daily_amount_summary
        .write
        .format("delta")
        .mode("overwrite")
        .save(
            GOLD_PATH + "daily_amount_summary/"
        )
    )

    # Write Monthly Amount Summary
    (
        monthly_amount_summary
        .write
        .format("delta")
        .mode("overwrite")
        .save(
            GOLD_PATH + "monthly_amount_summary/"
        )
    )


# -----------------------------------------------------------------------------
# Transform Transaction Status Summary
# -----------------------------------------------------------------------------

def transform_status_summary(df):

    # Daily summary
    daily_status_summary = (
        df
        .groupBy("transaction_date")

        # Aggregating transaction status metrics
        .agg(

            # Total transactions
            F.count("*").alias("total_transactions"),

            # Successful transactions
            F.sum(
                F.when(
                    F.col("is_successful") == 1,
                    1
                ).otherwise(0)
            ).alias("successful_transactions"),

            # Failed transactions
            F.sum(
                F.when(
                    F.col("is_successful") == 0,
                    1
                ).otherwise(0)
            ).alias("failed_transactions")
        )

        # Calculating success and failure rates
        .withColumn(
            "success_rate",
            F.col("successful_transactions")
            / F.col("total_transactions")
        )
        .withColumn(
            "failure_rate",
            F.col("failed_transactions")
            / F.col("total_transactions")
        )
    )

    # Monthly summary
    monthly_status_summary = (
        df
        .groupBy(
            "transaction_year",
            "transaction_month"
        )

        .agg(

            # Total transactions
            F.count("*").alias("total_transactions"),

            # Successful transactions
            F.sum(
                F.when(
                    F.col("is_successful") == 1,
                    1
                ).otherwise(0)
            ).alias("successful_transactions"),

            # Failed transactions
            F.sum(
                F.when(
                    F.col("is_successful") == 0,
                    1
                ).otherwise(0)
            ).alias("failed_transactions")
        )

        # Calculating success and failure rates
        .withColumn(
            "success_rate",
            F.col("successful_transactions")
            / F.col("total_transactions")
        )
        .withColumn(
            "failure_rate",
            F.col("failed_transactions")
            / F.col("total_transactions")
        )
    )

    return daily_status_summary, monthly_status_summary


# =============================================================================
# Write Status Summaries to Gold
# =============================================================================

def write_status_summaries_to_gold(
    daily_status_summary,
    monthly_status_summary
):

    # Write Daily Status Summary
    (
        daily_status_summary
        .write
        .format("delta")
        .mode("overwrite")
        .save(
            GOLD_PATH + "daily_status_summary/"
        )
    )

    # Write Monthly Status Summary
    (
        monthly_status_summary
        .write
        .format("delta")
        .mode("overwrite")
        .save(
            GOLD_PATH + "monthly_status_summary/"
        )
    )


# -----------------------------------------------------------------------------
# Transform Merchant Category Summary
# -----------------------------------------------------------------------------

def transform_merchant_category_summary(df):

    merchant_category_summary = (
        df
        .groupBy("merchant_category_code")

        # Aggregating transaction metrics by merchant category
        .agg(

            # Total transactions
            F.count("*").alias("total_transactions"),

            # Transaction amount metrics
            F.sum("amount").alias("total_transaction_amount"),
            F.avg("amount").alias("average_transaction_amount"),
            F.min("amount").alias("minimum_transaction_amount"),
            F.max("amount").alias("maximum_transaction_amount"),

            # Successful transactions
            F.sum(
                F.when(
                    F.col("is_successful") == 1,
                    1
                ).otherwise(0)
            ).alias("successful_transactions"),

            # Failed transactions
            F.sum(
                F.when(
                    F.col("is_successful") == 0,
                    1
                ).otherwise(0)
            ).alias("failed_transactions")
        )

        # Calculating success and failure rates
        .withColumn(
            "success_rate",
            F.col("successful_transactions")
            / F.col("total_transactions")
        )
        .withColumn(
            "failure_rate",
            F.col("failed_transactions")
            / F.col("total_transactions")
        )
    )

    return merchant_category_summary


# =============================================================================
# Write Merchant Category Summary to Gold
# =============================================================================

def write_merchant_category_summary_to_gold(
    merchant_category_summary
):

    # Write Merchant Category Summary
    (
        merchant_category_summary
        .write
        .format("delta")
        .mode("overwrite")
        .save(
            GOLD_PATH + "merchant_category_summary/"
        )
    )


# -----------------------------------------------------------------------------
# Transform Card Usage Method Summary
# -----------------------------------------------------------------------------

def transform_card_usage_method_summary(df):

    card_usage_method_summary = (
        df
        .groupBy("card_usage_method")

        # Aggregating transaction metrics by card usage method
        .agg(

            # Total transactions
            F.count("*").alias("total_transactions"),

            # Transaction amount
            F.sum("amount").alias("total_transaction_amount")
        )

        # Calculating transaction percentage
        .withColumn(
            "transaction_percentage",
            F.col("total_transactions") /
            F.sum("total_transactions").over(Window.partitionBy()) * 100
        )
    )

    return card_usage_method_summary


# =============================================================================
# Write Card Usage Method Summary to Gold
# =============================================================================

def write_card_usage_method_summary_to_gold(
    card_usage_method_summary
):

    (
        card_usage_method_summary
        .write
        .format("delta")
        .mode("overwrite")
        .save(
            GOLD_PATH + "card_usage_method_summary/"
        )
    )


# -----------------------------------------------------------------------------
# Transform Card Performance Summary
# -----------------------------------------------------------------------------

def transform_card_summary(df):

    card_summary = (
        df
        .groupBy(
            "card_brand",
            "card_type"
        )

        # Aggregating transaction metrics by card brand and type
        .agg(

            # Total transactions
            F.count("*").alias("total_transactions"),

            # Transaction amount metrics
            F.sum("amount").alias("total_transaction_amount"),
            F.avg("amount").alias("average_transaction_amount"),

            # Successful transactions
            F.sum(
                F.when(
                    F.col("is_successful") == 1,
                    1
                ).otherwise(0)
            ).alias("successful_transactions"),

            # Failed transactions
            F.sum(
                F.when(
                    F.col("is_successful") == 0,
                    1
                ).otherwise(0)
            ).alias("failed_transactions")
        )

        # Calculating success and failure rates
        .withColumn(
            "success_rate",
            F.col("successful_transactions")
            / F.col("total_transactions")
        )
        .withColumn(
            "failure_rate",
            F.col("failed_transactions")
            / F.col("total_transactions")
        )
    )

    return card_summary


# =============================================================================
# Write Card Summary to Gold
# =============================================================================

def write_card_summary_to_gold(card_summary):

    (
        card_summary
        .write
        .format("delta")
        .mode("overwrite")
        .save(
            GOLD_PATH + "card_summary/"
        )
    )


# -----------------------------------------------------------------------------
# Transform Geographic Summary
# -----------------------------------------------------------------------------

def transform_geographic_summary(df):

    geographic_summary = (
        df
        .groupBy(
            "merchant_state",
            "merchant_city"
        )

        # Aggregating transaction metrics by geographic location
        .agg(

            # Total transactions
            F.count("*").alias("total_transactions"),

            # Transaction amount metrics
            F.sum("amount").alias("total_transaction_amount"),
            F.avg("amount").alias("average_transaction_amount"),

            # Successful transactions
            F.sum(
                F.when(
                    F.col("is_successful") == 1,
                    1
                ).otherwise(0)
            ).alias("successful_transactions"),

            # Failed transactions
            F.sum(
                F.when(
                    F.col("is_successful") == 0,
                    1
                ).otherwise(0)
            ).alias("failed_transactions")
        )

        # Calculating success and failure rates
        .withColumn(
            "success_rate",
            F.col("successful_transactions")
            / F.col("total_transactions")
        )
        .withColumn(
            "failure_rate",
            F.col("failed_transactions")
            / F.col("total_transactions")
        )
    )

    return geographic_summary


# =============================================================================
# Write Geographic Summary to Gold
# =============================================================================

def write_geographic_summary_to_gold(geographic_summary):

    (
        geographic_summary
        .write
        .format("delta")
        .mode("overwrite")
        .save(
            GOLD_PATH + "geographic_summary/"
        )
    )


# -----------------------------------------------------------------------------
# Transform Transaction Error Summary
# -----------------------------------------------------------------------------

def transform_transaction_error_summary(df):

    transaction_error_summary = (
        df

        # Keep only failed transactions with an error
        .filter(
            F.col("transaction_error").isNotNull()
        )

        .groupBy("transaction_error")

        # Aggregating error metrics
        .agg(

            # Total occurrences of each error
            F.count("*").alias("total_errors"),

            # Total transaction amount affected by the error
            F.sum("amount").alias("total_transaction_amount")
        )
    )

    return transaction_error_summary


# =============================================================================
# Write Transaction Error Summary to Gold
# =============================================================================

def write_transaction_error_summary_to_gold(
    transaction_error_summary
):

    (
        transaction_error_summary
        .write
        .format("delta")
        .mode("overwrite")
        .save(
            GOLD_PATH + "transaction_error_summary/"
        )
    )

# =============================================================================
# GLOBAL MICRO-BATCH PROCESSOR (Unifies Pipeline)
# =============================================================================

def process_micro_batch(batch_df, batch_id):

    # Check if the micro-batch is empty to avoid unnecessary processing
    # ( No new data in the Silver layer )
    if batch_df.isEmpty():
        return

    # Otherwise, process the micro-batch as usual

    # 1. Process the fact table
    fact_transactions = transform_fact_transactions(batch_df)
    merge_fact_transactions(fact_transactions, batch_id)

    # 2. Process the summaries from the updated fact table
    fact_transactions_gold = (
        spark.read
        .format("delta")
        .load(GOLD_PATH + "fact_transactions/")
    )

    (
        daily_amount_summary,
        monthly_amount_summary
    ) = transform_amount_summary(fact_transactions_gold)

    (
        daily_status_summary,
        monthly_status_summary
    ) = transform_status_summary(fact_transactions_gold)

    merchant_category_summary = transform_merchant_category_summary(
        fact_transactions_gold
    )

    card_usage_method_summary = transform_card_usage_method_summary(
        fact_transactions_gold
    )

    card_summary = transform_card_summary(
        fact_transactions_gold
    )

    geographic_summary = transform_geographic_summary(
        fact_transactions_gold
    )

    transaction_error_summary = transform_transaction_error_summary(
        fact_transactions_gold
    )

    write_amount_summaries_to_gold(
        daily_amount_summary,
        monthly_amount_summary
    )

    write_status_summaries_to_gold(
        daily_status_summary,
        monthly_status_summary
    )

    write_merchant_category_summary_to_gold(
        merchant_category_summary
    )

    write_card_usage_method_summary_to_gold(
        card_usage_method_summary
    )

    write_card_summary_to_gold(
        card_summary
    )

    write_geographic_summary_to_gold(
        geographic_summary
    )

    write_transaction_error_summary_to_gold(
        transaction_error_summary
    )


# =============================================================================
# Execute Gold Pipeline (CORRECTION: Single Stream Triggered)
# =============================================================================

if __name__ == "__main__":

    df_silver = read_silver_stream()

    # A single stream manages the entire pipeline cleanly and efficiently
    query = (
        df_silver.writeStream
        .foreachBatch(process_micro_batch)
        .option(
            "checkpointLocation",
            GOLD_PATH + "pipeline/_checkpoints/"
        )
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()