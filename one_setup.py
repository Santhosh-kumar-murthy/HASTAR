import json

from broker_controller import BrokerController


def broker_setup():
    broker_manager = BrokerController()
    broker_manager.insert_broker('Zerodha',
                                 'https://cdn.zeebiz.com/sites/default/files/2022/08/11/194563-zerodha-logo.png',
                                 json.dumps({'kite_client_id': "RI4984", 'kite_password': "98453847",
                                             'totp_token': "ECMHGXA2QFSRMKETTY5U4WHY4EMPEI5C"}), 1, 1, 1, json.dumps(
            {"1_minute": "minute", "3_minute": "3minute", "5_minute": "5minute", "10_minute": "10minute",
             "15_minute": "15minute", "30_minute": "30minute",
             "1_hour": "60minute", "2_hour": "2hour", "3_hour": "3hour", "4_hour": "4hour", "1_day": "day"}))

    broker_manager.insert_broker('Angel One', 'https://asset.brandfetch.io/idDA95rr8l/idWGpFYO3-.png', json.dumps(
        {"api_key": "izudZxIa", "totp_token": "27WRYLFKTZTDMJ2JIGRQWGVRQQ", "client_id": "M489596",
         "password": "2580"}), 0, 1, 1, json.dumps(
        {"1_minute": "ONE_MINUTE", "3_minute": "THREE_MINUTE", "5_minute": "FIVE_MINUTE", "10_minute": "TEN_MINUTE",
         "15_minute": "FIFTEEN_MINUTE", "30_minute": "THIRTY_MINUTE",
         "1_hour": "ONE_HOUR", "2_hour": "", "3_hour": "", "4_hour": "", "1_day": "ONE_DAY"}))

    broker_manager.insert_broker('Shoonya',
                                 'https://cxotoday.com/wp-content/uploads/2023/05/shoonya_by_finvasia-Logo.png',
                                 json.dumps({"user": "FA329812", "pwd": "Sandy@#12345",
                                             "factor2": 'F462T2IC47J76N724E7PX5T3QSCHQ3O3', "vc": "FA329812_U",
                                             "app_key": "58e4edd319eb1625b9f0cf4ef6867bd5", "imei": "abc1234"}), 0, 1,
                                 1,
                                 json.dumps(
                                     {"1_minute": "1", "3_minute": "3", "5_minute": "5",
                                      "10_minute": "10",
                                      "15_minute": "15", "30_minute": "30",
                                      "1_hour": "60", "2_hour": "120", "3_hour": "180", "4_hour": "240",
                                      "1_day": "360"}))
    broker_manager.insert_broker('Alice Blue',
                                 'https://aliceblueonline.com/wp-content/uploads/2022/10/logo.webp',
                                 json.dumps({"username": "1424639", "password": "Pooja@#123",
                                             "twoFA": '1997', "app_id": "mcmTujlzBiJSasE",
                                             "api_secret": "EhBYIuGVuAEUIyLIZbMzbILUZRbSLnLEIErIURasqMYsMrFKQQQTdtPeeXVZxpkpVVRYpjxFHxhYFYjabKosggERdSjQXUkANqFZ"}),
                                 0, 1,
                                 0,
                                 json.dumps(
                                     {"1_minute": "1", "3_minute": "3", "5_minute": "5",
                                      "10_minute": "10",
                                      "15_minute": "15", "30_minute": "30",
                                      "1_hour": "60", "2_hour": "120", "3_hour": "180", "4_hour": "240",
                                      "1_day": "360"}))
    broker_manager.broker_change_system_use_status(1, 1)
    print("BROKER SETUP SUCCESSFUL")


def insert_default_time_frame():
    broker_manager = BrokerController()
    broker_manager.insert_default_time_frame()


broker_setup()
insert_default_time_frame()
