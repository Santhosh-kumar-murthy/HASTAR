import datetime
import json
import time

import analyzer
from broker_functions import BrokerFunctionsController
from positions_controller import PositionsController
from settings_controller import SettingsController
from utils import observable_instruments

if __name__ == '__main__':

    broker_functions = BrokerFunctionsController()
    settings_controller = SettingsController()
    active_time_frames = settings_controller.get_time_frame_settings()['active_time_frame']
    interval_minutes = [int(time_frame.split('_')[0]) for time_frame in active_time_frames.split(',')]
    last_executed_minutes = [-1] * len(interval_minutes)  # Track last executed minutes for each interval
    while True:
        try:
            current_hour = datetime.datetime.now().hour
            current_minute = datetime.datetime.now().minute
            for i, interval_minute in enumerate(interval_minutes):
                # login_broker
                broker_creds = json.loads(settings_controller.get_broker_creds_config()['broker_config_params'])
                broker_obj, refresh_token = broker_functions.get_angel_broker_obj(broker_creds)
                time.sleep(1)
                # interval key
                interval_key = json.loads(settings_controller.get_broker_time_frame_config()['broker_time_frames'])[
                    active_time_frames.split(',')[i]]
                if current_minute % interval_minute == 0 and current_minute != last_executed_minutes[i]:
                    last_executed_minutes[i] = current_minute
                    time.sleep(4)
                    for instrument in observable_instruments:
                        if current_hour == 15 and current_minute >= 15:
                            positions = PositionsController().check_for_existing_position(instrument, interval_key)
                            for position in positions:
                                PositionsController().exit_existing_position(position, broker_obj)
                        else:
                            data_frame = broker_functions.get_historic_data_angel(instrument, broker_obj, interval_key)
                            if interval_minute == 1:
                                applied_data_frame = analyzer.calculate_signals(data_frame, 2, 1)
                            elif interval_minute == 3:
                                applied_data_frame = analyzer.calculate_signals(data_frame, 2, 30)

                            PositionsController().analyze_to_take_positions(applied_data_frame, instrument,
                                                                            interval_key,
                                                                            broker_obj)
        except Exception as e:
            print(f"Error occurred: {e}")
        time.sleep(60 - datetime.datetime.now().second)
