import pandas as pd
from sqlalchemy import text
from src.utils.db_context import mssql_connector
import logging

QUERY = '''
    SELECT TOP 1 PERCENTILE_DISC(0.9)
                                WITHIN GROUP (ORDER BY o.order_gmv2)
                                OVER () AS max_basket_value,
                PERCENTILE_DISC(0.9)
                                WITHIN GROUP (ORDER BY DATEDIFF(MINUTE, trip_created_at, assigned_to_courier_at))
                                OVER () AS max_searching_time
    FROM dw.fact.orders o
    WHERE o.order_date >= CAST(GETDATE() - 2 AS DATE);
'''

def cache_maximum_statistics(surge_engine):
    try:
        mssql_engine = mssql_connector(db_name='DW')
        with mssql_engine.begin() as connection:
            ms = pd.read_sql(text(QUERY), connection)
            (max_basket_value, max_searching_time) = ms.iloc[0, 0], ms.iloc[0, 1]

        surge_engine.set_cached_maximum_statistics((max_basket_value, max_searching_time))

    except Exception as e:
        logging.error(f"Caching Error: {str(e)}")