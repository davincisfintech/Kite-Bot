import json
import time as t
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date, timedelta, time

import pandas as pd
from dateutil.parser import parse

from trading_bot.clients.kite_client import KiteClient
from trading_bot.database.db import engine
from trading_bot.database.db_handler import save_trade
from trading_bot.settings import logger, CONFIG_DIR, TZ, BASE_DIR
from trading_bot.strategies.strike_selection import StrikeSelection
from trading_bot.streamers.kite_streamer import KiteStreamer
from trading_bot.trade_managers.opt_trade_manager import OptTradeManager

"""
controller to connect and control client, streamer, db and trade manager
"""


class Controller:
    def __init__(self, streamer: KiteStreamer, trade_managers: list):
        """
        Controller to connect and control client, streamer, db and trade manager
        :param streamer: instance of streamer class
        :param trade_managers: list trader manager instanes
        """
        self.streamer = streamer
        self.trade_managers = trade_managers

    def start_streaming(self):
        """
        Start streaming
        """
        self.streamer.start_streaming()

    @staticmethod
    def run_instance(args: tuple):
        """
        Run trade manager's trade function
        """
        obj, tick, order_data = args
        return obj.trade(tick, order_data)

    def run(self):
        """
        Get ticks, orders data from streamer and pass it to trade manager instance
        """
        # If no ticks found
        if self.streamer.ticks_queue.empty():
            return
        # Create trading instances using ticks and trade manager instances
        ticks = self.streamer.ticks_queue.get()
        self.trade_managers = [obj for obj in self.trade_managers if not obj.trade_ended]
        trade_instances = []
        for tick in ticks:
            trade_instances.extend(
                [(obj, tick, self.streamer.orders_queue.get(obj.symbol)) for obj in self.trade_managers if
                 obj.instrument_token == tick['instrument_token']])

        # Run trade instances
        with ThreadPoolExecutor() as executor:
            res = executor.map(self.run_instance, trade_instances)
        res = [r for r in res if r is not None]

        for r in res:
            # Store trade details if trade data received
            if isinstance(r, dict):
                if r['msg']:
                    for i in r['msg']:
                        if i:
                            for k, v in i.items():
                                save_trade(k, v)
            else:
                # Remove instance if trade is ended for it
                if r.trade_ended:
                    logger.debug(f'{r.symbol} instance removed from trading manager')

        if not len(self.trade_managers):
            logger.debug('All instances closed, Trading ended')
            return 'trade_ended'


def run():
    """
    Run function to initialize client, streamer, trading managers, controller
    and connect them with user specified inputs
    """
    try:
        # Read parameters
        df = pd.read_excel(BASE_DIR / 'parameters.xlsx', engine='openpyxl')
    except FileNotFoundError as e:
        logger.exception(e)
        t.sleep(5)
        return

    try:
        # Parse inputs
        df['expiry_date'] = df['expiry_date'].apply(lambda x: parse(str(x)).date())
        df['entry start time'] = df['start_time'].apply(lambda x: parse(str(x)).time())
        df['end_time'] = df['end_time'].apply(lambda x: parse(str(x)).time())
        df['trail_sl'] = df['trail_sl'].apply(lambda x: True if x.lower() == 'yes' else False)
        df['opt_type'] = df['opt_type'].str.upper()
        df['direction'] = df['direction'].str.upper()
        df = list(df.T.to_dict().values())
    except Exception as e:
        logger.exception(e)
        logger.debug('Please provide valid inputs')
        t.sleep(5)
        return

    try:
        # Read kite credentials
        with open(CONFIG_DIR / 'kite_config.json') as config:
            config = json.load(config)
            user_id = config["USER_ID"]
            password = config["PASSWORD"]
            mfa_secret_key = config["MFA_SECRET_KEY"]
    except (FileNotFoundError, KeyError) as e:
        logger.exception(e)
        t.sleep(5)
        return

    # Initialize kite client
    kite_client = KiteClient(user_id=user_id, password=password, mfa_secret_key=mfa_secret_key)
    kite_client.login()
    kite_client.load_instruments(exchanges=['NFO', 'NSE'])

    # Check if any open order/position
    open_pos_stock_list = pd.read_sql('trades_data', engine)
    mask = ((open_pos_stock_list['entry_order_time'].dt.date == date.today()) & (
            (open_pos_stock_list['position_status'] == "OPEN") | (
            open_pos_stock_list['entry_order_status'] == 'OPEN')))
    open_pos_stock_list = open_pos_stock_list[mask]
    open_pos_stock_list = list(open_pos_stock_list.T.to_dict().values())
    if not len(open_pos_stock_list) and not len(df):
        logger.debug('No symbols found for trading')
        t.sleep(5)
        return

    # Initialize kite streamer
    kite_streamer = KiteStreamer(user_id=user_id, ws_token=kite_client.ws_token)
    # Initialize trade_managers
    trade_managers = list()

    # Initialize controller
    controller = Controller(streamer=kite_streamer, trade_managers=trade_managers)

    # Start streaming
    controller.start_streaming()

    open_pos_tokens = []
    # Creating trading instances for open position/order
    for trade in open_pos_stock_list:
        instrument_token = int(trade['instrument_token'])
        if instrument_token not in open_pos_tokens:
            open_pos_tokens.append(instrument_token)
        logger.info(f"open position/order found in {trade['symbol']}, reading parameters...")
        entry_order_filled = False if trade['entry_order_status'] == 'OPEN' else True
        exit_pending = True if trade['exit_order_status'] == 'OPEN' else False
        bought = True if trade['side'] == 'BUY' else False
        sold = True if trade['side'] == 'SELL' else False
        kwargs = {
            'client': kite_client, 'symbol': trade['symbol'], 'instrument_token': instrument_token,
            'exchange': trade['exchange'], 'direction': trade['direction'], 'lot_size': trade['lot_size'],
            'lots': trade['lots'],
            'underlying_symbol': trade['underlying_symbol'], 'end_time': trade['end_time'],
            'stop_loss': trade['stop_loss_percent'], 'trail_sl': trade['trail_sl'],
            'entered': True, 'entry_order_filled': entry_order_filled, 'bought': bought, 'sold': sold,
            'instruction': trade['instruction'], 'qty': trade['quantity'],
            'entry_order_id': trade['entry_order_id'], 'exit_order_id': trade['exit_order_id'],
            'sl': trade['stop_loss'], 'exit_pending': exit_pending, 'final_sl': trade['final_stop_loss'],
            'entry_order_price': trade['entry_order_price'],
            'exit_order_price': trade['exit_order_price']}

        controller.trade_managers.append(OptTradeManager(**kwargs))

    # Subscribe for data for open_pos_tokens
    t.sleep(3)
    if len(open_pos_tokens):
        controller.streamer.subscribe(instruments=open_pos_tokens)

    # Initialize strategies list for strike selection
    strats = list()
    df = {i['symbol']: i for i in df}
    if len(df):
        # Map instruments for given symbols
        instruments = kite_client.map_instruments(symbols=list(df.keys()))
        if not len(instruments):
            logger.debug(f'No instruments found for symbols: {list(df.keys())}, make sure you entered valid symbols')
            if not len(open_pos_stock_list):
                t.sleep(5)
                return
        else:
            for inst in instruments:
                params = df[inst['tradingsymbol']]
                if params['num_batches'] > params['lots']:
                    logger.debug(f"{inst['tradingsymbol']}: number lots must be greater or equal to number of batches")
                    continue
                params['num_batches'] = int(params['num_batches'])
                start_time_range = [
                    (datetime.combine(date.today(), params['start_time']) + timedelta(
                        minutes=(params['entry_interval'] * i))).time() for i in range(params['num_batches'])]
                end_time_range = [
                    (datetime.combine(date.today(), params['end_time']) + timedelta(
                        minutes=(params['end_interval'] * i))).time() for i in range(params['num_batches'])]
                lots_batches = [
                    params['lots'] // params['num_batches'] + (1 if x < params['lots'] % params['num_batches'] else 0)
                    for x in range(params['num_batches'])]
                strats.extend([StrikeSelection(client=kite_client, symbol=inst['tradingsymbol'],
                                               exchange=inst['exchange'], expiry_date=params['expiry_date'],
                                               strike_dist=params['strike_dist'],
                                               strike_diff=params['strike_diff'], call_premium=params['call_premium'],
                                               put_premium=params['put_premium'], opt_type=params['opt_type'],
                                               start_time=start_time_range[j],
                                               end_time=(min(time(15, 20), end_time_range[j])),
                                               lots=lots_batches[j]) for j in range(params['num_batches']) if
                               end_time_range[j] > datetime.now(tz=TZ).time()])
            if not len(strats):
                logger.debug('No strategy instances created, make sure you entered right parameters, '
                             'end time must be greater than start time and start time must be grater than current time')
                if not len(open_pos_stock_list):
                    t.sleep(5)
                    return
                else:
                    logger.debug('Trading existing instances')

    while True:
        instruments = []
        strats = [s for s in strats if not s.strikes_retrieved]
        for s in strats:
            # Retrieve strikes
            opt_instruments = s.get_strikes()
            if isinstance(opt_instruments, list):  # If strikes retrieved
                params = df[s.symbol]
                for inst in opt_instruments:
                    # Set parameters for instruments
                    inst['stop_loss'] = params['stop_loss']
                    inst['trail_sl'] = params['trail_sl']
                    inst['direction'] = params['direction']
                    inst['underlying_symbol'] = s.symbol
                instruments.extend(opt_instruments)
                logger.debug(f'Strikes retrieved for {s.symbol}, starting trading instances')
            elif opt_instruments is None:  # If strikes not retrieved
                logger.debug(f'No option contracts found for symbol: {s.symbol}, '
                             f'make sure you provided right parameters')

        if len(instruments):
            # Subscribe for data for instruments
            tokens = [i['instrument_token'] for i in instruments if
                      i['instrument_token'] not in kite_streamer.subscribed_instruments]
            controller.streamer.subscribe(instruments=tokens)

        # Creating trading instances for instruments
        for i in instruments:
            kwargs = {
                'client': kite_client, 'symbol': i['tradingsymbol'], 'instrument_token': i['instrument_token'],
                'underlying_symbol': i['underlying_symbol'],
                'exchange': i['exchange'], 'direction': i['direction'], 'lot_size': i['lot_size'], 'lots': i['lots'],
                'stop_loss': i['stop_loss'], 'end_time': i['end_time'],
                'trail_sl': i['trail_sl']
            }
            controller.trade_managers.append(OptTradeManager(**kwargs))

        # Run
        if len(controller.trade_managers):
            msg = controller.run()
            # If trade_ended message returned from controller then stop trading
            if msg == 'trade_ended':
                logger.debug('Trading ended')
                break
