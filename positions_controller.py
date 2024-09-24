import datetime
from contextlib import closing

import pymysql
from pymysql.cursors import DictCursor

from database_config import db_config
from mqtt_publisher import MqttPublisher


class PositionsController:
    def __init__(self):
        self.conn = pymysql.connect(**db_config, cursorclass=DictCursor)
        self.create_positions_table()

    @staticmethod
    def get_current_price(broker, instrument):
        val = broker.getMarketData(
            mode="LTP",
            exchangeTokens={"NFO": [instrument['angel_token']]})['data']['fetched'][0]['ltp']
        return val

    def create_positions_table(self):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS positions (
                                position_id INT AUTO_INCREMENT PRIMARY KEY,
                                observable_instrument_id INT,
                                zerodha_instrument_token INT,
                                zerodha_trading_symbol VARCHAR(255),
                                zerodha_name VARCHAR(255),
                                zerodha_exchange VARCHAR(255),
                                angel_token INT,
                                angel_symbol VARCHAR(255),
                                angel_name VARCHAR(255),
                                angel_exchange VARCHAR(255),
                                shoonya_token INT,
                                shoonya_trading_symbol VARCHAR(255),
                                shoonya_name VARCHAR(255),
                                shoonya_exchange VARCHAR(255),
                                alice_token VARCHAR(255),
                                alice_trading_symbol VARCHAR(255),
                                alice_name VARCHAR(255),
                                alice_exchange VARCHAR(255),
                                instrument_position_type INT COMMENT 
                                '1 = FUT BUY\r\n2 = FUT SELL\r\n3 = OPT BUY\r\n4 = OPT SELL',
                                position_type INT COMMENT 
                                '1 = LONG\r\n2 = SHORT',
                                position_entry_time DATETIME,
                                position_entry_price FLOAT,
                                position_exit_time DATETIME,
                                position_exit_price FLOAT,
                                profit FLOAT,
                                lot_size INT,
                                position_qty INT,
                                time_frame VARCHAR(255),
                                search_key VARCHAR(255),
                                expiry DATE  
                            )
                        ''')
            self.conn.commit()

    def exit_existing_position(self, existing_position, broker):
        try:
            exit_price = float(self.get_current_price(broker, existing_position))
            entry_price = float(existing_position['position_entry_price'])
            position_type = existing_position['instrument_position_type']
            profit = (exit_price - entry_price) if (position_type == 1 or position_type == 3) else (
                    entry_price - exit_price)
            with self.conn.cursor() as cursor:
                # Update the future position
                cursor.execute('''
                    UPDATE positions
                    SET position_exit_price = %s, position_exit_time = NOW(), profit = %s
                    WHERE position_id = %s
                ''', (exit_price, profit, existing_position['position_id']))
                self.conn.commit()
            return True, 'Success', {
                "trade_type": "exit",
                "exit_existing_position": existing_position,
                "exit_price": exit_price,
                "position_type": position_type
            }
        except Exception as e:
            self.conn.rollback()  # Roll back in case of any error
            return False, str(e), None

    def check_for_existing_position(self, instrument, interval_key):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                'SELECT * FROM positions WHERE search_key = %s AND time_frame = %s AND position_exit_time IS NULL',
                (instrument['search_key'], interval_key))
            active_trades = cursor.fetchall()
        return active_trades

    def analyze_to_take_positions(self, applied_data_frame, instrument, interval_key, broker_obj):
        mqtt_publisher = MqttPublisher()
        existing_positions = self.check_for_existing_position(instrument, interval_key)
        current_candle = applied_data_frame.iloc[-2]
        previous_candle = applied_data_frame.iloc[-3]

        def create_position(position_type, interval):
            buy_option_data = self.get_option_for_buying(
                instrument, position_type, current_candle.close
            )
            buy_option_current_price = self.add_opt_to_positions(
                buy_option_data=buy_option_data,
                interval=interval,
                broker=broker_obj,
                instrument=instrument,
                position_type=position_type,
            )
            return {
                "trade_type": "entry",
                "position_type": position_type,
                "interval": interval,
                "opt_buy": {
                    "buy_option_data": buy_option_data,
                    "buy_option_current_price": buy_option_current_price
                },
            }

        # current_candle.pos = 1
        # previous_candle.pos = -1

        if current_candle.pos == 1 and previous_candle.pos != 1:
            if not existing_positions:
                payload = create_position(1, interval_key)
                mqtt_publisher.publish_payload(payload, interval_key)
            else:
                take_position_flag = False
                for existing_position in existing_positions:
                    if existing_position['position_type'] != 1:
                        take_position_flag = True
                        status, message, exit_payload = self.exit_existing_position(existing_position, broker_obj)
                        if status:
                            mqtt_publisher.publish_payload(exit_payload, interval_key)
                        else:
                            print(message)
                    else:
                        log_msg = f"Long Position for instrument already exists for long OID: {existing_position['observable_instrument_id']}"
                        print(log_msg)
                if take_position_flag:
                    payload = create_position(1, interval_key)
                    mqtt_publisher.publish_payload(payload, interval_key)
        elif current_candle.pos == -1 and previous_candle.pos != -1:
            if not existing_positions:
                payload = create_position(2, interval_key)
                mqtt_publisher.publish_payload(payload, interval_key)
            else:
                take_position_flag = False
                for existing_position in existing_positions:
                    if existing_position['position_type'] != 2:
                        take_position_flag = True
                        status, message, exit_payload = self.exit_existing_position(existing_position, broker_obj)
                        if status:
                            mqtt_publisher.publish_payload(exit_payload, interval_key)
                        else:
                            print(message)
                    else:
                        log_msg = f"Short Position for instrument already exists for short OID: {existing_position['observable_instrument_id']}"
                        print(log_msg)
                if take_position_flag:
                    payload = create_position(2, interval_key)
                    mqtt_publisher.publish_payload(payload, interval_key)
        else:
            print("No SIGNAL YET", instrument, datetime.datetime.now())

    def get_option_for_buying(self, instrument, position_type, fut_current_price):
        instrument_types = {
            1: 'CE',
            2: 'PE'
        }
        instrument_type = instrument_types.get(position_type, 'Unknown')

        queries = {
            "zerodha_long_query": """ SELECT * FROM zerodha_instruments WHERE zerodha_segment IN ('NFO-OPT', 'BFO-OPT') AND zerodha_name = %s AND zerodha_instrument_type = %s AND zerodha_expiry >= CURDATE() AND zerodha_strike > %s ORDER BY zerodha_expiry ASC, zerodha_strike ASC LIMIT 1; """,
            "zerodha_short_query": """ SELECT * FROM zerodha_instruments WHERE zerodha_segment IN ('NFO-OPT', 'BFO-OPT') AND zerodha_name = %s AND zerodha_instrument_type = %s AND zerodha_expiry >= CURDATE() AND zerodha_strike < %s ORDER BY zerodha_expiry ASC, zerodha_strike DESC LIMIT 1; """,
            "angel_long_query": """SELECT * FROM angel_instruments WHERE angel_instrument_type = 'OPTIDX' AND angel_name = %s AND angel_expiry >= CURDATE() AND angel_strike > %s AND angel_symbol LIKE %s ORDER BY angel_expiry ASC, angel_strike ASC LIMIT 1;""",
            "angel_short_query": """SELECT * FROM angel_instruments WHERE angel_instrument_type = 'OPTIDX' AND angel_name = %s AND angel_expiry >= CURDATE() AND angel_strike < %s AND angel_symbol LIKE %s ORDER BY angel_expiry ASC, angel_strike DESC LIMIT 1;""",
            "shoonya_long_query": """SELECT * FROM shoonya_instruments WHERE shoonya_instrument_type = 'OPTIDX' AND shoonya_symbol = %s AND shoonya_expiry >= CURDATE() AND shoonya_strike_price > %s AND shoonya_option_type = %s ORDER BY shoonya_expiry ASC, shoonya_strike_price ASC LIMIT 1;""",
            "shoonya_short_query": """SELECT * FROM shoonya_instruments WHERE shoonya_instrument_type = 'OPTIDX' AND shoonya_symbol = %s AND shoonya_expiry >= CURDATE() AND shoonya_strike_price < %s AND shoonya_option_type = %s ORDER BY shoonya_expiry ASC, shoonya_strike_price DESC LIMIT 1;""",
            "alice_long_query": """SELECT * FROM alice_blue_instruments WHERE alice_instrument_type = 'OPTIDX' AND alice_symbol = %s AND alice_expiry_date >= CURDATE() AND alice_strike_price > %s AND alice_option_type = %s ORDER BY alice_expiry_date ASC, alice_strike_price ASC LIMIT 1;""",
            "alice_short_query": """SELECT * FROM alice_blue_instruments WHERE alice_instrument_type = 'OPTIDX' AND alice_symbol = %s AND alice_expiry_date >= CURDATE() AND alice_strike_price < %s AND alice_option_type = %s ORDER BY alice_expiry_date ASC, alice_strike_price DESC LIMIT 1;"""
        }
        zerodha_query = queries.get('zerodha_long_query' if position_type == 1 else 'zerodha_short_query', 'Unknown')
        angel_query = queries.get('angel_long_query' if position_type == 1 else 'angel_short_query', 'Unknown')
        shoonaya_query = queries.get('shoonya_long_query' if position_type == 1 else 'shoonya_short_query', 'Unknown')
        alice_query = queries.get('alice_long_query' if position_type == 1 else 'alice_short_query', 'Unknown')

        with closing(self.conn.cursor()) as cursor:
            cursor.execute(zerodha_query,
                           (instrument['search_key'], instrument_type, fut_current_price))
            zerodha_option = cursor.fetchone()

            cursor.execute(angel_query,
                           (instrument['search_key'], str(int(float(fut_current_price))) + "00", "%" + instrument_type))
            angel_option = cursor.fetchone()

            cursor.execute(shoonaya_query, (instrument['search_key'], str(fut_current_price), instrument_type))
            shoonya_option = cursor.fetchone()

            cursor.execute(alice_query, (instrument['search_key'], str(fut_current_price), instrument_type))
            alice_option = cursor.fetchone()
            return {
                "zerodha_option": zerodha_option, "angel_option": angel_option, "shoonya_option": shoonya_option,
                "alice_option": alice_option
            }

    def add_opt_to_positions(self, buy_option_data, interval, broker,
                             instrument, position_type):

        buy_option_current_price = self.get_current_price(broker, buy_option_data['angel_option'])
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                "INSERT INTO positions (observable_instrument_id, zerodha_instrument_token, zerodha_trading_symbol, zerodha_name, zerodha_exchange, "
                "angel_token, angel_symbol, angel_name, angel_exchange, "
                "shoonya_token, shoonya_trading_symbol, shoonya_name, shoonya_exchange, "
                "alice_token, alice_trading_symbol, alice_name, alice_exchange, "
                "instrument_position_type,position_type, position_entry_time, position_entry_price, "
                "lot_size,position_qty, time_frame,expiry,search_key) VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s,%s,%s,%s,%s)",
                (instrument['o_id'], buy_option_data['zerodha_option']['zerodha_instrument_token'],
                 buy_option_data['zerodha_option']['zerodha_trading_symbol'],
                 buy_option_data['zerodha_option']['zerodha_name'],
                 buy_option_data['zerodha_option']['zerodha_exchange'],
                 buy_option_data['angel_option']['angel_token'], buy_option_data['angel_option']['angel_symbol'],
                 buy_option_data['angel_option']['angel_name'],
                 buy_option_data['angel_option']['angel_exchange_segment'],
                 buy_option_data['shoonya_option']['shoonya_token'],
                 buy_option_data['shoonya_option']['shoonya_trading_symbol'],
                 buy_option_data['shoonya_option']['shoonya_symbol'],
                 buy_option_data['shoonya_option']['shoonya_exchange'],
                 buy_option_data['alice_option']['alice_token'],
                 buy_option_data['alice_option']['alice_trading_symbol'],
                 buy_option_data['alice_option']['alice_symbol'], buy_option_data['alice_option']['alice_exchange'],
                 3,
                 position_type,
                 buy_option_current_price, buy_option_data['zerodha_option']['zerodha_lot_size'], 1, interval,
                 buy_option_data['zerodha_option']['zerodha_expiry'], instrument['search_key']))
        self.conn.commit()
        return buy_option_current_price
