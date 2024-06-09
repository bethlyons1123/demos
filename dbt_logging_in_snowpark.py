import snowflake.snowpark as snowpark
import pandas as pd
import numpy as np
from datetime import datetime 

def compare_orders(orders, session, session_id):
    results = []
    
    # Create log table if it doesn't exist
    session.sql(f"""
    CREATE TABLE IF NOT EXISTS query_logs (
        order_id INT,
        query_string STRING, 
        session_id TIMESTAMP
    )
    """).collect()

    for idx, row in orders.iterrows():
        # Ensure the date is correctly formatted and quoted 
        query_string = (
            f"O_CUSTKEY == {row.O_CUSTKEY} "
            f"and O_ORDERDATE > '{row.O_ORDERDATE}' "
            f"and O_ORDERDATE < '{row.O_ORDERDATE + pd.Timedelta(days=365)}'"
        ) 

         # Log the query_string
        session.sql(f"""
        INSERT INTO query_logs (order_id, query_string, session_id)
        VALUES ({row.O_ORDERKEY}, '{query_string.replace("'", "''")}', '{session_id}')
        """).collect()

        df_orders_to_compare = orders.query(query_string)
        df_orders_to_compare['ORDER_ID'] = row.O_ORDERKEY
        results.append(df_orders_to_compare)

    if results: 
        results_df = pd.concat(results, ignore_index=True)
    else: 
        results_df=pd.DataFrame()
    return results_df
 
def model(dbt, session): 
    # Generate a timestamp-based session ID for this run
    session_id = datetime.now()

    # Set the model configuration
    dbt.config(materialized='table')

    # Reference the view
    view_df = dbt.ref('Orders')

    pandas_df = view_df.to_pandas()
    
    # Ensure the O_ORDERDATE is in datetime format
    pandas_df['O_ORDERDATE'] = pd.to_datetime(pandas_df['O_ORDERDATE'])
    
    result_df = compare_orders(pandas_df, session, session_id)

    # Convert the resulting DataFrame back to a Snowflake DataFrame and return it
    return session.create_dataframe(result_df)
