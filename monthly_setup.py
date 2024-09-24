import json
import threading

from broker_controller import BrokerController
from instruments_controller import InstrumentsController


def zerodha_instrument_setup():
    instrument_load_manager = InstrumentsController()
    broker_manager = BrokerController()
    kite_config = json.loads(broker_manager.get_broker_by_id(1)['broker_config_params'])
    instrument_load_manager.clear_zerodha_instruments()
    status, log_text = instrument_load_manager.load_zerodha_instruments(kite_config)
    print(status, log_text)


def angel_one_instrument_setup():
    instrument_load_manager = InstrumentsController()
    instrument_load_manager.clear_angel_instruments()
    status, log_text = instrument_load_manager.load_angel_instruments()
    print(status, log_text)


def shoonya_instrument_setup():
    instrument_load_manager = InstrumentsController()
    instrument_load_manager.clear_shoonya_instruments()
    status, log_text = instrument_load_manager.load_shoonya_instruments()
    print(status, log_text)


def alice_blue_instrument_setup():
    instrument_load_manager = InstrumentsController()
    instrument_load_manager.clear_alice_blue_instruments()
    status, log_text = instrument_load_manager.load_alice_blue_instruments()
    print(status, log_text)


def async_zerodha_instrument_setup():
    thread = threading.Thread(target=zerodha_instrument_setup)
    thread.start()


def async_angel_one_instrument_setup():
    thread = threading.Thread(target=angel_one_instrument_setup)
    thread.start()


def async_shoonya_instrument_setup():
    thread = threading.Thread(target=shoonya_instrument_setup)
    thread.start()


def async_alice_blue_instrument_setup():
    thread = threading.Thread(target=alice_blue_instrument_setup)
    thread.start()


async_zerodha_instrument_setup()
async_angel_one_instrument_setup()
async_shoonya_instrument_setup()
async_alice_blue_instrument_setup()
