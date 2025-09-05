import pandas as pd
from sqlalchemy import text
from src.utils.db_context import mssql_connector
import numpy as np
import logging
import threading
import requests
import json

SETUP_QUERY = '''
    DROP TABLE IF EXISTS #lma;
    DROP TABLE IF EXISTS #sma;

    WITH last_10_orders AS (
        SELECT o.order_id,
               ms.polygon_id,
               delivery_time,
               ROW_NUMBER() OVER (PARTITION BY polygon_id ORDER BY finalized_at DESC) AS rn
        FROM dw.fact.orders o WITH (NOLOCK)
        JOIN dw.dim.merchant_shop ms WITH (NOLOCK)
          ON o.merchant_shop_id = ms.id
        WHERE o.delivery_provider = 'miare'
          AND is_gross = 1
          AND delivery_status = 'delivered'
          AND order_date >= CAST(GETDATE() - 1 AS DATE)
    )
    SELECT polygon_id,
           AVG(delivery_time) AS short_rolling_average
    INTO #sma
    FROM last_10_orders
    WHERE rn <= 20
    GROUP BY polygon_id;

    WITH last_100_orders AS (
        SELECT o.order_id,
               ms.polygon_id,
               delivery_time,
               ROW_NUMBER() OVER (PARTITION BY polygon_id ORDER BY finalized_at DESC) AS rn
        FROM dw.fact.orders o WITH (NOLOCK)
        JOIN dw.dim.merchant_shop ms WITH (NOLOCK)
          ON o.merchant_shop_id = ms.id
        WHERE o.delivery_provider = 'miare'
          AND o.is_gross = 1
          AND o.delivery_status = 'delivered'
          AND o.order_date >= CAST(GETDATE() - 1 AS DATE)
    )
    SELECT polygon_id,
           AVG(delivery_time) AS long_moving_average
    INTO #lma
    FROM last_100_orders
    WHERE rn <= 100
    GROUP BY polygon_id;
'''

MAIN_QUERY = '''
    SELECT l.polygon_id, s.short_rolling_average - l.long_moving_average AS sma_lma_difference
    FROM #lma l
    JOIN #sma s ON l.polygon_id = s.polygon_id
'''

def cache_polygon_deliveries(surge_engine):
    try:
        mssql_engine = mssql_connector(db_name='DW')
        with mssql_engine.begin() as connection:
            connection.execute(text(SETUP_QUERY))
            df_polygon_deliveries = pd.read_sql(text(MAIN_QUERY), connection)


        surge_engine.set_cached_polygon_deliveries(df_polygon_deliveries.set_index('polygon_id')['sma_lma_difference'].to_dict())
        
        del df_polygon_deliveries

    except Exception as e:
        logging.error(f"Caching Error: {str(e)}")