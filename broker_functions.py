import datetime
import time

import pymysql
import pyotp
from SmartApi import SmartConnect
from pymysql.cursors import DictCursor
import pandas as pd

from database_config import db_config


class BrokerFunctionsController:
    def __init__(self):
        self.conn = pymysql.connect(**db_config, cursorclass=DictCursor)

    @staticmethod
    def get_refresh_totp(totp_token_value):
        totp = pyotp.TOTP(totp_token_value)
        return totp.now()

    def get_angel_broker_obj(self, broker_config):
        smart_api = SmartConnect(api_key=broker_config['api_key'])
        session_data = smart_api.generateSession(broker_config['client_id'], broker_config['password'],
                                                 self.get_refresh_totp(broker_config['totp_token']))
        refresh_token = session_data['data']['refreshToken']
        smart_api.generateToken(refresh_token)
        return smart_api, refresh_token

    @staticmethod
    def get_historic_data_angel(instrument, broker, interval):
        from_datetime = datetime.datetime.now() - datetime.timedelta(days=10)
        to_datetime = datetime.datetime.now()
        from_datetime_formatted = from_datetime.strftime('%Y-%m-%d %H:%M')
        to_datetime_formatted = to_datetime.strftime('%Y-%m-%d %H:%M')
        candle_data = broker.getCandleData(historicDataParams={
            "exchange": instrument['angel_exchange_segment'],
            "symboltoken": instrument['angel_token'],
            "interval": interval,
            "fromdate": from_datetime_formatted,
            "todate": to_datetime_formatted
        })
        candle_data = pd.DataFrame(candle_data['data'])
        candle_data.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        return candle_data

    @staticmethod
    def get_ltp_angel(exchange, broker, instrument):
        val = broker.getMarketData(
            mode="LTP",
            exchangeTokens={exchange: [instrument['angel_token']]})['data']['fetched'][0]['ltp']
        return val
