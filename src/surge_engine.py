from src.utils.db_context import mssql_connector
from scipy.stats import beta
import random
import logging


class SurgeEngine:
    _instance = None
    prediction_pipeline = None
    cached_inferences = None
    boost_level_details_dict = None
    mssql_engine = mssql_connector('DS')

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SurgeEngine, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    @staticmethod
    def W():
        
        # Run bandit_train.ipynb
        return {
            'polygon_load_factor': 0.1,
            'polygon_sma_lma_difference_factor': 0.3,
            'organic_user_factor': 0.1,
            'otd_bad_experience_factor': 0.1,
            'searching_time_factor': 0.3,
            'basket_value_factor': 0.1
        }

    @staticmethod
    def F(data):
        searching_time_factor = 0 if data['current_searching_time'] <= 3 else (
            (data['current_searching_time'] - 3) / data['max_searching_time']
            if data['current_searching_time'] <= data['max_searching_time'] else 1
        )

        basket_value_factor = 0 if data['basket_value'] < 1_000_000 else (
            (data['basket_value'] - 1_000_000) / data['max_basket_value']
            if data['basket_value'] <= data['max_basket_value'] else 1
        )

        return {
            'polygon_load_factor': (
                data['polygon_open_orders'] / data['max_polygon_open_orders'] if data['polygon_open_orders'] <= data[
                    'max_polygon_open_orders'] else 1
            ),
            'polygon_sma_lma_difference_factor': max(
                min(data['polygon_sma_lma_difference'] / data['max_polygon_sma_lma_difference'], +1), -1
            ),  # cap
            'organic_user_factor': 1 if data['is_organic_user'] else 0,
            'otd_bad_experience_factor': 1 if data['is_otd_bad_experience'] else 0,
            'searching_time_factor': searching_time_factor,
            'basket_value_factor': basket_value_factor
        }

    def set_cached_polygon_deliveries(self, cached_polygon_deliveries):
        self.cached_polygon_deliveries = cached_polygon_deliveries

    def set_cached_polygon_open_orders(self, cached_polygon_open_orders):
        self.cached_polygon_open_orders = cached_polygon_open_orders

    def set_cached_user_organic_segment(self, cached_user_organic_segment):
        self.cached_user_organic_segment = cached_user_organic_segment

    def set_cached_user_otd_experience_segment(self, cached_user_otd_experience_segment):
        self.cached_user_otd_experience_segment = cached_user_otd_experience_segment

    def set_cached_maximum_statistics(self, cached_maximum_statistics):
        self.cached_maximum_statistics = cached_maximum_statistics

    def should_boost(self, data):
        signal = sum(self.W()[k] * self.F(data)[k] for k in self.F(data))
        exploration_scale = sum(self.W()[k] * 1.0 for k in self.F(data)) + (data['current_boost_level'] / 4)
        a = signal if signal > 0 else 1e-5
        b = exploration_scale

        decision = (sampled_prob := beta(a, b).rvs()) >= 0.5 and random.random() <= sampled_prob

        data.update(self.F(data))
        data.update(self.W())
        data.update({
            'signal': signal,
            'exploration': exploration_scale,
            'alpha': a,
            'beta': b
        })

        return decision, sampled_prob
