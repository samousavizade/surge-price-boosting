import pandas as pd
from sqlalchemy import text
from src.utils.db_context import mssql_connector
import numpy as np
import logging
import threading
import requests
import json

QUERY = '''
    WITH filtered_orders AS (SELECT o.order_id,
                                    ms.polygon_id
                            FROM dw.fact.orders o WITH (NOLOCK)
                                    JOIN dw.dim.merchant_shop ms WITH (NOLOCK)
                                        ON o.merchant_shop_id = ms.id AND
                                            o.order_shipment_status IN ('new', 'submitted', 'confirmed', 'processing') AND
                                            o.delivery_provider = 'miare' AND
                                            is_gross = 1 AND
                                            o.finalized_at >= CAST(DATEADD(HOUR, -2, GETDATE()) AS DATETIME))
    SELECT polygon_id, COUNT(*) AS open_orders
    FROM filtered_orders
    GROUP BY polygon_id;
'''

def cache_polygon_open_orders(surge_engine):
    try:
        mssql_engine = mssql_connector(db_name='DW')
        df_polygon_open_orders = pd.read_sql(text(QUERY), mssql_engine)
        mssql_engine.dispose()

        surge_engine.set_cached_polygon_open_orders(df_polygon_open_orders.set_index('polygon_id')['open_orders'].to_dict())

        del df_polygon_open_orders
        
    except Exception as e:
        logging.error(f"Caching Error: {str(e)}")
