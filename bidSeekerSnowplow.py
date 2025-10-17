# Import necessary Snowpark functions
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, flatten, parse_json, max as max_, when, current_timestamp, lit, call_function

def main(session: snowpark.Session):
    """
    Analyzes auction data from the last 10 days to find leads where a specific broker bid but lost,
    and calculates the loss amount.
    """
    # --- Single Variable to Control the Broker ID ---
    target_broker_id = 359
#2427,2165,531,359,43
    #1764, 2256,2241,223,2267
    # 1. Reference the source table in Snowflake
    table_name = "atlas_prod.ds_sandbox.leads_comprehensive"
    base_table = session.table(table_name)

    # 2. Convert the Table object into a DataFrame object
    base_df = base_table.select("*")

    # 3. Filter the DataFrame to include only the last 10 days
    # This uses the corrected 'call_function' import
    base_df = base_df.filter(
        col("RECORD_DATE_TIME_UTC") >= call_function("DATEADD", lit("day"), lit(-5), current_timestamp())
    )

    # 4. Correctly flatten the AUCTION_SUMMARY by joining with 'join_table_function'
    flattened_df = base_df.join_table_function(
        flatten(parse_json(col("AUCTION_SUMMARY")))
    )

    # 5. Find the bid for the target broker for each lead from the flattened data
    broker_bids_df = flattened_df.group_by("LEAD_ID").agg(
        max_(
            when(col("value").getField("BROKER_ID") == target_broker_id, col("value").getField("BID"))
        ).alias("broker_bid")
    )

    # 6. Join the broker's bid back to the main DataFrame
    df_with_bids = base_df.join(
        broker_bids_df,
        on="LEAD_ID",
        how="left"
    )

    # 7. Filter for leads that the target broker lost
    df_lost_leads = df_with_bids.filter(
        (col("broker_bid").is_not_null()) &
        (col("BROKER_ID") != target_broker_id) &
        (col("BROKER_ID").is_not_null())
    )

    # 8. Calculate the loss amount and select final columns
    df_loss_analysis = df_lost_leads.select(
        col("LEAD_ID"),
        col("BROKER_ID").alias("winning_broker_id"),
        col("BID_VALUE").alias("winning_bid"),
        col("broker_bid").cast("float").alias("loser_broker_bid"),
        (col("BID_VALUE").cast("float") - col("broker_bid").cast("float")).alias("loss_amount")
    )
    print('complete')
    return df_loss_analysis
