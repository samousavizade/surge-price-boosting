import pandas as pd
from sqlalchemy import text
from src.utils.db_context import mssql_connector
import logging

QUERY = '''
    SELECT user_id,
        CASE
            WHEN COUNT(order_id) > 10 AND
                    DATEDIFF(DAY, CAST(MAX(created_at) AS DATE), CAST(GETDATE() AS DATE)) < 91 AND
                    CAST(ROUND((((SUM(IIF((voucher_value = 0), 1, 0))) * 1.0) / (COUNT(order_id))),
                            2) AS DECIMAL(10, 2)) > 0.70 THEN CAST(1 AS BIT)
            WHEN COUNT(order_id) < 11 AND COUNT(order_id) > 5 AND
                    DATEDIFF(DAY, CAST(MIN(created_at) AS DATE), CAST(GETDATE() AS DATE)) < 181 AND
                    CAST(ROUND((((SUM(IIF((voucher_value = 0), 1, 0))) * 1.0) / (COUNT(order_id))),
                            2) AS DECIMAL(10, 2)) > 0.70 THEN CAST(1 AS BIT)
            ELSE CAST(0 AS BIT)
            END             AS is_organic_user,
        IIF(SUM(IIF((is_otd = 0), 1, 0)) * 1.0 / COUNT(order_id) >= 0.3, CAST(1 AS BIT),
            CAST(0 AS BIT)) AS is_otd_bad_experience
    FROM dw.fact.orders WITH (NOLOCK)
    WHERE is_gross = 1
    AND order_date >= CAST(GETDATE() - 360 AS DATE)
    GROUP BY user_id;
'''

def cache_user_details(surge_engine):
    try:
        mssql_engine = mssql_connector(db_name='DW')
        df_user_details = pd.read_sql(text(QUERY), mssql_engine)
        mssql_engine.dispose()

        surge_engine.set_cached_user_organic_segment(df_user_details.set_index('user_id')['is_organic_user'].to_dict())
        surge_engine.set_cached_user_otd_experience_segment(df_user_details.set_index('user_id')['is_otd_bad_experience'].to_dict())

        del df_user_details

    except Exception as e:
        logging.error(f"Caching Error: {str(e)}")