from contextlib import closing

import pymysql
from pymysql.cursors import DictCursor

from database_config import db_config


class SettingsController:
    def __init__(self):
        self.conn = pymysql.connect(**db_config, cursorclass=DictCursor)

    def get_time_frame_settings(self):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute('SELECT setting_value as active_time_frame FROM settings WHERE setting_name = %s',
                           'active_time_frame')
            time_frame = cursor.fetchone()
        return time_frame

    def get_broker_time_frame_config(self):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute('SELECT broker_time_frames FROM brokers WHERE broker_id = %s',
                           '2')
            time_frames = cursor.fetchone()
        return time_frames

    def get_broker_creds_config(self):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute('SELECT broker_config_params FROM brokers WHERE broker_id = %s',
                           '2')
            config_params = cursor.fetchone()
        return config_params
