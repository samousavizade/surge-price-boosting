from fastapi import FastAPI, BackgroundTasks, Header, HTTPException
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from pytz import timezone
from src.caching.polygon_deliveries import cache_polygon_deliveries
from src.caching.polygon_open_orders import cache_polygon_open_orders
from src.caching.user_details import cache_user_details
from src.caching.maximum_statistics import cache_maximum_statistics
from src.surge_engine import SurgeEngine
import logging
from datetime import datetime
import pandas as pd
import numpy as np
from pydantic import BaseModel
from pydantic import BaseModel
from src.utils.db_context import mssql_connector
import json
from sqlmodel import Field, SQLModel, Session
import numpy as np
from prometheus_fastapi_instrumentator import Instrumentator


app = FastAPI()
Instrumentator(
    excluded_handlers=[".*admin.*", "/metrics"],
).instrument(
    app,
    # metric_namespace='surge_project',
    # metric_subsystem='surge_service'
).expose(app)
surge_engine = SurgeEngine()

class BoostRequest(BaseModel):
    basket_value: int
    activity_type: int
    user_id: int
    remaining_budget: int
    order_id: int
    polygon_id: int
    boost_request_count: int
    current_boost_level: int

from sqlmodel import SQLModel, Field
from datetime import datetime

class SurgeDecision(SQLModel, table=True):
    __tablename__ = "surge_decisions_log"
    __table_args__ = {"schema": "ops"}  

    id: int = Field(default=None, primary_key=True)  
    decision_date_time: datetime = Field(sa_column_kwargs={"nullable": False}, sa_type="decision_date_time") 
    decision_date: datetime = Field(sa_column_kwargs={"nullable": False}, sa_type="decision_date") 
    data: str = Field(sa_type="data") 

def convert_np(obj):
    if isinstance(obj, (np.integer, np.floating, np.bool_)):
        return obj.item()
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def log_surge_decision_data(data: dict):
    try:
        decision = SurgeDecision(
            decision_date_time=datetime.now(tehran_tz).strftime('%Y-%m-%d %H:%M:%S'), 
            decision_date=datetime.now(tehran_tz).date().strftime('%Y-%m-%d'),  
            data=json.dumps(data, default=convert_np)
        )
        with Session(surge_engine.mssql_engine) as session:
            session.add(decision)
            session.commit()

    except Exception as e:
        logging.error(f"Error Log Surge Decision: {str(e)}")
        surge_engine.mssql_engine = mssql_connector('DS')


EXPECTED_TOKEN = "1ad02993980ade66080b61e5e9a6a48335ac7a01ecaa9c5a219492cd77411e42"

@app.post("/api/surge/boost_request/")
async def boost_request(request: BoostRequest, background_tasks: BackgroundTasks, authorization: str = Header(..., alias="Authorization")):
    if authorization != EXPECTED_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid auth token")

    try:
        data = request.model_dump()
        from datetime import datetime

        data.update({
            'decision_timestamp': datetime.now(tehran_tz).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            'polygon_open_orders': surge_engine.cached_polygon_open_orders[data['polygon_id']],
            'max_polygon_open_orders': max(surge_engine.cached_polygon_open_orders.values()),
            'polygon_sma_lma_difference': surge_engine.cached_polygon_deliveries[data['polygon_id']],
            'max_polygon_sma_lma_difference': max(surge_engine.cached_polygon_deliveries.values()),
            'is_organic_user': surge_engine.cached_user_organic_segment[data['user_id']],
            'is_otd_bad_experience': surge_engine.cached_user_otd_experience_segment[data['user_id']],
            'max_basket_value': surge_engine.cached_maximum_statistics[0],
            'max_searching_time': surge_engine.cached_maximum_statistics[1],
            'current_searching_time': data['boost_request_count'] * 3.0
        })

        decision, sampled_prob = surge_engine.should_boost(data)
        data.update({'decision': decision, 'sampled_prob': sampled_prob})
        data['boost_level'] = min(5, max(0, data.get('current_boost_level', 0) + int(decision)))
        background_tasks.add_task(log_surge_decision_data, data)

        return {
            "order_id": data.get('order_id', 'unknown'),
            "boost_level": data.get('boost_level', 'unknown'),
            "decision_timestamp": data.get("decision_timestamp", 'unknown'),
        }
        
    except Exception as e:
        logging.error(f"Error: \n{e}")
        return {
            "order_id": data.get('order_id', 'unknown'),
            "boost_level": data.get('current_boost_level', 'unknown'),
            "decision_timestamp": datetime.now(tehran_tz).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        }
    

from typing import Dict
class BoostLevelsDetails(BaseModel):
    boost_level_details_dict: Dict[int, int]

@app.post("/api/surge/update_boost_level_details/")
async def update_boost_level_details(boost_level_details_dict: BoostLevelsDetails, authorization: str = Header(..., alias="Authorization")):
    if authorization != EXPECTED_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid auth token")

    try:
        data = boost_level_details_dict.model_dump()
        surge_engine.boost_level_details_dict = data['boost_level_details_dict']
        logging.info(f"Surge data {surge_engine.boost_level_details_dict}")
        return {
            "response": "Boost level details updated successfully",
        }
    
    except Exception as e:
        logging.error(f"Error processing boost request: {str(e)}")
        return {
            "response": "Boost level details update error",
        }

jobstores = {
    'default': MemoryJobStore()
}
executors = {
    'default': ThreadPoolExecutor(20)
}
job_defaults = {
    'coalesce': False,
    'max_instances': 3
}
tehran_tz = timezone('Asia/Tehran')
scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults, timezone=tehran_tz)

scheduler.add_job(
    cache_polygon_deliveries,
    'interval',
    minutes=10,
    args=[surge_engine], 
    next_run_time=pd.Timestamp.now(tz='Asia/Tehran')
)

scheduler.add_job(
    cache_polygon_open_orders,
    'interval',
    minutes=10,
    args=[surge_engine],
    next_run_time=pd.Timestamp.now(tz='Asia/Tehran')
)

scheduler.add_job(
    cache_user_details,
    'interval',
    days=1,
    args=[surge_engine],
    next_run_time=pd.Timestamp.now(tz='Asia/Tehran')
)

scheduler.add_job(
    cache_maximum_statistics,
    'interval',
    days=1,
    args=[surge_engine],
    next_run_time=pd.Timestamp.now(tz='Asia/Tehran')
)

scheduler.start()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0", 
        port=8070,
    )
    # uvicorn.run(
    #     app,
    #     host="127.0.0.1",
    #     port=8000,
    #     # log_level="critical"
    # )

