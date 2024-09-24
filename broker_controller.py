import pymysql
from pymysql.cursors import DictCursor

from database_config import db_config


class BrokerController:
    def __init__(self):
        self.conn = pymysql.connect(**db_config, cursorclass=DictCursor)
        self.create_brokers_table()
        self.create_settings_table()

    def create_brokers_table(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS brokers (
                                broker_id INT AUTO_INCREMENT PRIMARY KEY,
                                broker_name VARCHAR(255),
                                broker_logo VARCHAR(255),
                                broker_config_params LONGTEXT,
                                broker_system_use_status INT,
                                broker_public_allowed INT,
                                broker_system_allowed INT,
                                broker_time_frames LONGTEXT
                            )
                        ''')
            self.conn.commit()

    def create_settings_table(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS settings (
                                setting_name VARCHAR(255),
                                setting_value VARCHAR(255)
                            )
                        ''')
            self.conn.commit()

    def insert_broker(self, broker_name, broker_logo, broker_config_params, broker_system_use_status,
                      broker_public_allowed, broker_system_allowed, broker_time_frames):
        with self.conn.cursor() as cursor:
            cursor.execute('''
            INSERT INTO brokers (broker_name,broker_logo,broker_config_params,broker_system_use_status,broker_public_allowed,broker_system_allowed,broker_time_frames)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ''', (broker_name, broker_logo, broker_config_params, broker_system_use_status, broker_public_allowed,
                  broker_system_allowed,
                  broker_time_frames))
        self.conn.commit()

    def update_broker(self, broker_id, broker_name=None, broker_logo=None, broker_config_params=None,
                      broker_system_use_status=None, broker_public_allowed=None, broker_system_allowed=None,
                      broker_time_frames=None):
        fields = []
        values = []
        if broker_name is not None:
            fields.append("broker_name = %s")
            values.append(broker_name)
        if broker_logo is not None:
            fields.append("broker_logo = %s")
            values.append(broker_logo)
        if broker_config_params is not None:
            fields.append("broker_config_params = %s")
            values.append(broker_config_params)
        if broker_system_use_status is not None:
            fields.append("broker_system_use_status = %s")
            values.append(broker_system_use_status)
        if broker_public_allowed is not None:
            fields.append("broker_public_allowed = %s")
            values.append(broker_public_allowed)
        if broker_system_allowed is not None:
            fields.append("broker_system_allowed = %s")
            values.append(broker_system_allowed)
        if broker_time_frames is not None:
            fields.append("broker_time_frames = %s")
            values.append(broker_time_frames)

        values.append(broker_id)

        if fields:
            with self.conn.cursor() as cursor:
                sql_query = f'''
                UPDATE brokers 
                SET {', '.join(fields)}
                WHERE broker_id = %s
                '''
                cursor.execute(sql_query, values)
            self.conn.commit()

    def broker_change_system_use_status(self, broker_id, broker_system_use_status):
        with self.conn.cursor() as cursor:
            cursor.execute('''
                UPDATE brokers 
                SET broker_system_use_status = CASE 
                    WHEN broker_id = %s THEN %s
                    ELSE 0
                END
            ''', (broker_id, broker_system_use_status))
        self.conn.commit()

    def get_all_brokers(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''SELECT * FROM brokers''')
            all_brokers = cursor.fetchall()
        return all_brokers

    def get_active_broker(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''SELECT * FROM brokers WHERE broker_system_use_status = 1''')
            all_active_brokers = cursor.fetchone()
        return all_active_brokers

    def get_all_non_active_brokers(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''SELECT * FROM brokers WHERE broker_system_use_status = 0''')
            all_non_active_brokers = cursor.fetchall()
        return all_non_active_brokers

    def get_broker_by_id(self, broker_id):
        with self.conn.cursor() as cursor:
            cursor.execute('''SELECT * FROM brokers WHERE broker_id = %s''', broker_id)
            broker_details = cursor.fetchone()
        return broker_details

    def get_settings(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''SELECT * FROM settings''')
            time_frame = cursor.fetchone()
        return time_frame

    def get_time_frame_settings(self):
        with self.conn.cursor() as cursor:
            cursor.execute('SELECT setting_value as active_time_frame FROM settings WHERE setting_name = %s',
                           'active_time_frame')
            time_frame = cursor.fetchone()
        return time_frame

    def insert_default_time_frame(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''
            INSERT INTO settings (setting_name,setting_value)
            VALUES (%s,%s)
            ''', ('active_time_frame', '1_minute,3_minute'))
        self.conn.commit()

    def change_active_time_frame(self, active_time_frame):
        with self.conn.cursor() as cursor:
            cursor.execute('''
            UPDATE settings SET setting_value = %s WHERE setting_name = %s
            ''', (active_time_frame, 'active_time_frame'))
        self.conn.commit()
