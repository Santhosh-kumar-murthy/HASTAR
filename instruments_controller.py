import datetime
import glob
import os
import zipfile

import pymysql
import pyotp
import requests
from pymysql.cursors import DictCursor

from broker_libs.kite_trade import KiteApp, get_enctoken
from database_config import db_config


def get_refresh_totp(totp_token):
    totp = pyotp.TOTP(totp_token)
    return totp.now()


class InstrumentsController:
    def __init__(self):
        self.conn = pymysql.connect(**db_config, cursorclass=DictCursor)
        self.create_zerodha_instruments_table()
        self.create_angel_instruments_table()
        self.create_shoonya_instruments_table()
        self.create_alice_blue_instruments_table()

    def create_zerodha_instruments_table(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS zerodha_instruments (
                                zerodha_instrument_token INT,
                                zerodha_exchange_token INT,
                                zerodha_trading_symbol VARCHAR(255),
                                zerodha_name VARCHAR(255),
                                zerodha_last_price FLOAT,
                                zerodha_expiry DATE,
                                zerodha_strike FLOAT,
                                zerodha_tick_size FLOAT,
                                zerodha_lot_size INT,
                                zerodha_instrument_type VARCHAR(255),
                                zerodha_segment VARCHAR(255),
                                zerodha_exchange VARCHAR(255)
                            )
                        ''')
            self.conn.commit()

    def create_angel_instruments_table(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS angel_instruments (
                            angel_token VARCHAR(255),
                            angel_symbol VARCHAR(255),
                            angel_name VARCHAR(255),
                            angel_expiry DATE,
                            angel_strike DECIMAL,
                            angel_lot_size INT,
                            angel_instrument_type VARCHAR(255),
                            angel_exchange_segment VARCHAR(255),
                            angel_tick_size DECIMAL
                        )
                    ''')
            self.conn.commit()

    def create_shoonya_instruments_table(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS shoonya_instruments (
                                shoonya_exchange VARCHAR(255),
                                shoonya_token INT,
                                shoonya_lot_size INT,
                                shoonya_symbol VARCHAR(255),
                                shoonya_trading_symbol VARCHAR(255),
                                shoonya_expiry DATE,
                                shoonya_instrument_type VARCHAR(255),
                                shoonya_option_type VARCHAR(255),
                                shoonya_strike_price FLOAT,
                                shoonya_tick_size FLOAT
                            )
                        ''')
            self.conn.commit()

    def create_alice_blue_instruments_table(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS alice_blue_instruments (
                                alice_exchange VARCHAR(255),
                                alice_exchange_segment VARCHAR(255),
                                alice_expiry_date DATE,
                                alice_formatted_ins_name VARCHAR(255),
                                alice_instrument_type VARCHAR(255),
                                alice_lot_size INT,
                                alice_option_type VARCHAR(255),
                                alice_pdc DECIMAL(10, 2) NULL,
                                alice_strike_price DECIMAL(10, 2),
                                alice_symbol VARCHAR(255),
                                alice_tick_size VARCHAR(255),
                                alice_token VARCHAR(255),
                                alice_trading_symbol VARCHAR(255)
                            )
                        ''')
            self.conn.commit()

    def clear_zerodha_instruments(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''TRUNCATE TABLE zerodha_instruments''')
            self.conn.commit()

    def clear_angel_instruments(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''TRUNCATE TABLE angel_instruments''')
            self.conn.commit()

    def clear_shoonya_instruments(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''TRUNCATE TABLE shoonya_instruments''')
            self.conn.commit()

    def clear_alice_blue_instruments(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''TRUNCATE TABLE alice_blue_instruments''')
            self.conn.commit()

    def load_zerodha_instruments(self, kite_config):
        try:
            kite = KiteApp(enctoken=get_enctoken(kite_config['kite_client_id'], kite_config['kite_password'],
                                                 get_refresh_totp(kite_config['totp_token'])))
            all_instruments = kite.instruments()
            insert_query = """INSERT INTO zerodha_instruments (zerodha_instrument_token, zerodha_exchange_token, zerodha_trading_symbol, zerodha_name, zerodha_last_price, zerodha_expiry, zerodha_strike, zerodha_tick_size, zerodha_lot_size, zerodha_instrument_type, zerodha_segment, zerodha_exchange ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            with self.conn.cursor() as cursor:
                for instrument in all_instruments:
                    data = (
                        instrument['instrument_token'],
                        instrument['exchange_token'],
                        instrument['tradingsymbol'],
                        instrument['name'],
                        instrument['last_price'],
                        instrument['expiry'],
                        instrument['strike'],
                        instrument['tick_size'],
                        instrument['lot_size'],
                        instrument['instrument_type'],
                        instrument['segment'],
                        instrument['exchange']
                    )
                    cursor.execute(insert_query, data)
                    self.conn.commit()
            return True, "Zerodha instruments load successful"
        except Exception as e:
            return False, str(e)

    def load_angel_instruments(self):
        try:
            instruments = requests.get(
                "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json").json()
            with self.conn.cursor() as cursor:
                insert_query = """
                            INSERT INTO angel_instruments (angel_token,angel_symbol, angel_name, angel_expiry, angel_strike, angel_lot_size, angel_instrument_type, angel_exchange_segment, angel_tick_size)
                            VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """
                for instrument in instruments:
                    try:
                        expiry_date = datetime.datetime.strptime(instrument['expiry'], "%d%b%Y").date()
                    except ValueError:
                        expiry_date = None
                    cursor.execute(
                        insert_query,
                        (instrument['token'], instrument["symbol"], instrument['name'], expiry_date,
                         instrument['strike'], instrument['lotsize'], instrument['instrumenttype'],
                         instrument['exch_seg'],
                         instrument['tick_size']))
            self.conn.commit()
            return True, "Angel instruments load successful"
        except Exception as e:
            return False, str(e)

    def load_shoonya_instruments(self):
        try:
            root = 'https://api.shoonya.com/'
            masters = ['NSE_symbols.txt.zip', 'NFO_symbols.txt.zip',
                       'BSE_symbols.txt.zip', 'BFO_symbols.txt.zip']

            # Ensure the sh_inst directory exists
            if not os.path.exists('sh_inst'):
                os.makedirs('sh_inst')

            for zip_file in masters:
                url = root + zip_file
                r = requests.get(url, allow_redirects=True)
                with open(zip_file, 'wb') as f:
                    f.write(r.content)

                try:
                    with zipfile.ZipFile(zip_file) as z:
                        z.extractall(path='sh_inst')
                except Exception as e:
                    print("Invalid file", e)

                os.remove(zip_file)

            def load_data_into_db(file_path, instrument_type):
                with open(file_path, 'r') as file:
                    next(file)
                    for line in file:
                        values = line.strip().split(',')
                        with self.conn.cursor() as cursor:
                            if instrument_type == 'opt':
                                date_str = values[5]
                                date_obj = datetime.datetime.strptime(date_str, '%d-%b-%Y')
                                values[5] = date_obj.strftime('%Y-%m-%d')
                                values = values[:10]
                                cursor.execute(
                                    'INSERT INTO shoonya_instruments (shoonya_exchange,shoonya_token,shoonya_lot_size,shoonya_symbol,shoonya_trading_symbol,shoonya_expiry,shoonya_instrument_type,shoonya_option_type,shoonya_strike_price,shoonya_tick_size) VALUES'
                                    '(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', values)
                                self.conn.commit()

                            if instrument_type == 'eq':
                                values = values[:7]
                                cursor.execute(
                                    'INSERT INTO shoonya_instruments (shoonya_exchange,shoonya_token,shoonya_lot_size,shoonya_symbol,shoonya_trading_symbol,shoonya_instrument_type,shoonya_tick_size) VALUES'
                                    '(%s, %s, %s, %s, %s, %s, %s)', values)
                                self.conn.commit()

            for txt_file in glob.glob(os.path.join('sh_inst', '*.txt')):
                if txt_file.endswith('BFO_symbols.txt') or txt_file.endswith('NFO_symbols.txt'):
                    load_data_into_db(txt_file, 'opt')
                elif txt_file.endswith('NSE_symbols.txt') or txt_file.endswith('BSE_symbols.txt'):
                    load_data_into_db(txt_file, 'eq')
                os.remove(txt_file)

            os.rmdir('sh_inst')
            return True, "Shoonya instruments load successful"
        except Exception as e:
            return False, str(e)

    def load_alice_blue_instruments(self):
        try:
            base_url = "https://v2api.aliceblueonline.com/restpy/contract_master?exch="
            master_instruments_segments = [
                "NSE",
                "NFO",
                "BSE",
                "BFO",
                "INDICES"
            ]
            for segment in master_instruments_segments:
                response = requests.get(base_url + segment)
                instruments_data = response.json()

                # Check if the segment key is present in the response
                if segment not in instruments_data:
                    continue

                with self.conn.cursor() as cursor:
                    insert_query = """
                        INSERT INTO alice_blue_instruments 
                        (alice_exchange, alice_exchange_segment, alice_expiry_date, alice_formatted_ins_name, 
                        alice_instrument_type, alice_lot_size, alice_option_type, alice_pdc, alice_strike_price, 
                        alice_symbol, alice_tick_size, alice_token, alice_trading_symbol) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

                    for instrument in instruments_data[segment]:
                        expiry_date = None

                        if "expiry_date" in instrument and instrument["expiry_date"]:
                            try:
                                expiry_date = datetime.datetime.fromtimestamp(
                                    instrument["expiry_date"] / 1000
                                ).strftime("%Y-%m-%d")
                            except (ValueError, TypeError):
                                expiry_date = None

                        cursor.execute(
                            insert_query,
                            (
                                instrument.get('exch'),
                                instrument.get("exchange_segment"),
                                expiry_date,
                                instrument.get('formatted_ins_name'),
                                instrument.get('instrument_type'),
                                instrument.get('lot_size'),
                                instrument.get('option_type', 'XX'),
                                # Default value 'XX' for option type if not present
                                instrument.get('pdc'),
                                instrument.get('strike_price'),
                                instrument.get('symbol'),
                                instrument.get('tick_size'),
                                instrument.get('token'),
                                instrument.get('trading_symbol')
                            )
                        )
            self.conn.commit()
            return True, "Alice Blue instruments load successful"
        except Exception as e:
            return False, str(e)
