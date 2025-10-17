import snowflake.snowpark as snowpark

def main(session: snowpark.Session):
    # Your SQL query is stored in a multi-line string for readability.
    sql_query = """
select *

from atlas_prod.ds_sandbox.filtersets_comprehensive

    """

    # Use the provided session to execute the query.
    print("Executing query...")
    results_df = session.sql(sql_query)
    #results_df2=results_df.select('UNIQUE_KEY')
    # Return the DataFrame to display the results in the worksheet.
    return results_df
