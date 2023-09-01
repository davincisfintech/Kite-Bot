from typing import Union
from datetime import date, timedelta
from json.decoder import JSONDecodeError

import requests
import pyotp
import pandas as pd
from dateutil.parser import parse

from kiteconnect import KiteConnect
from kiteconnect.exceptions import DataException

from trading_bot.settings import logger

"""
Kite rest client to handle api requests to kite connect
"""


class KiteClient:
    def __init__(self, user_id: str, password: str, mfa_secret_key: str, api_key: str = 'xyz'):
        """
        KiteClient class to handle trading api endpoints functions
        :param user_id: zerodha kite user id
        :param password: zerodha kitepassword
        :param pin: zerodha kite pin
        :param api_key:
        """
        self.user_id = user_id
        self.password = password
        self.mfa_secret_key = mfa_secret_key
        self.api_key = api_key
        self.ws_url = 'wss://ws.zerodha.com'
        self.ua_string = 'user-agent=kite3-web&version=2.6.2'
        self.key_string = 'kitefront'
        self.root_trade_url = 'https://kite.zerodha.com/oms'
        self.auth_url = 'https://kite.zerodha.com/api'
        self.data_url = 'https://kite.zerodha.com/oms/instruments/historical'
        self.uid = "1605085892719"
        self.random_id = "1600839345062"
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0'
        }
        # Initialize KiteConnect
        self.rest_client = KiteConnect(api_key=self.api_key)
        # Kite available exchanges
        self.available_exchanges = ['NFO', 'NSE', 'CDS', 'MCX']

        # Variables for storing exchange, instruments and orders data
        self.exchange_dict = dict()
        self.all_instruments = list()
        self.ws_token = None
        self.all_orders = []

    def login(self):
        """
        Login to kite
        """
        try:
            with requests.session() as s:
                # Login
                data = {'user_id': self.user_id, 'password': self.password}
                res = s.post(f'{self.auth_url}/login', data=data, headers=self.headers)
                res = res.json()['data']

                # Two FA
                pin = pyotp.TOTP(self.mfa_secret_key).now()
                data = {'user_id': res['user_id'], 'request_id': res['request_id'], 'twofa_value': pin}
                res = s.post(f'{self.auth_url}/twofa', data=data, headers=self.headers)

                # Token retrieval
                auth = res.headers['Set-Cookie'].split(",")[2].split(';')[0].replace(" ", "").replace("enctoken=",
                                                                                                      "enctoken ")
                csrf_token = res.headers['Set-Cookie'].split(";")[0].replace("public_token=", "")
                self.headers['x-csrftoken'] = csrf_token
                self.headers['authorization'] = auth
                self.ws_token = auth.split()[1].replace('/', '%2F').replace('+', '%2B').replace('=', '%3D')
        except Exception as e:
            logger.exception(e)
            logger.debug("Error connecting with kite, please try again later")
            exit()

    def get_orders(self) -> dict:
        """
        Get all orders
        :return orders data if retrieved successfully else None
        """
        for i in range(2):
            try:
                all_orders = requests.get(f"{self.root_trade_url}/orders", headers=self.headers)
                self.all_orders = all_orders.json()['data']
                return self.all_orders
            except Exception as e:
                logger.exception(e)
                self.login()

    def get_positions(self) -> dict:
        """
        Get all positions
        :return positions data if retrieved successfully else None
        """
        for i in range(2):
            try:
                all_positions = requests.get(f"{self.root_trade_url}/portfolio/positions", headers=self.headers)
                return all_positions.json()['data']['day']
            except Exception as e:
                logger.exception(e)
                self.login()

    def place_order(self, variety: str, tradingsymbol: str, quantity: str, transaction_type: str,
                    trigger_price: float = None, price: float = None,
                    exchange: str = 'NSE', order_type: str = 'MARKET', product: str = 'MIS', validity: str = None,
                    disclosed_quantity: int = None, square_off: str = None, stop_loss: float = None,
                    trailing_stop_loss: float = None, tag: str = 'placed by algo') -> str:
        """
        Place order
        :param variety: order variety i.e. regular
        :param tradingsymbol: trading symbol
        :param quantity: order quantity
        :param transaction_type: transaction type i.e. BUY/SELL
        :param trigger_price: trigger price for stop order
        :param price: order price
        :param exchange: exchange
        :param order_type: order type i.e. MARKET, LIMIT etc
        :param product: product i.e. MIS, CNC etc
        :param validity: order validity
        :param disclosed_quantity: disclosed quantity
        :param square_off: square off
        :param stop_loss: stop loss
        :param trailing_stop_loss: trailing stop loss
        :param tag: tag to specify order
        :return: order id if order placed successfully else None
        """
        params = locals()
        del params['self']
        # Set parameters
        params = {i: j for i, j in params.items() if j is not None}
        for i in range(2):
            try:
                order_details = requests.post(f"{self.root_trade_url}/orders/{variety}", headers=self.headers,
                                              data=params)
                return order_details.json()['data']['order_id']
            except Exception as e:
                logger.exception(e)
                self.login()

    def modify_order(self, variety: str, order_id: str, price: float = None, trigger_price: float = None,
                     quantity: int = None, parent_order_id: str = None, order_type: str = None, validity: str = None,
                     disclosed_quantity: str = None) -> str:
        """
        Modifies order
        :param variety: order variety i.e. regular
        :param order_id: order id
        :param quantity: order quantity
        :param parent_order_id: parent order id
        :param trigger_price: trigger price for stop order
        :param price: order price
        :param order_type: order type i.e. MARKET, LIMIT etc
        :param validity: order validity
        :param disclosed_quantity: disclosed quantity
        :return: order id if order modified successfully else None
        """
        params = locals()
        del params['self']
        # Set parameters
        params = {i: j for i, j in params.items() if j is not None}
        for i in range(2):
            try:
                order_details = requests.put(f"{self.root_trade_url}/orders/{variety}/{order_id}",
                                             headers=self.headers, data=params)
                logger.debug(order_details.text)
                return order_details.json()['data']['order_id']
            except Exception as e:
                logger.exception(e)
                self.login()

    def cancel_order(self, variety: str, order_id: str, parent_order_id: str = None) -> str:
        """
        Cancels order
        :param variety: order variety i.e. regular
        :param order_id: order id
        :param parent_order_id: parent order id
        :return: order id if order modified successfully else None
        """
        params = locals()
        del params['self']
        for i in range(2):
            try:
                order_details = requests.delete(f"{self.root_trade_url}/orders/{variety}/{order_id}",
                                                headers=self.headers, data=params)
                return order_details.json()['data']['order_id']
            except Exception as e:
                logger.exception(e)
                self.login()

    @staticmethod
    def get_date_range(start_date, end_date):
        start_date, end_date = parse(start_date).date(), parse(end_date).date()
        date_ranges = []
        while start_date <= end_date and start_date <= date.today():
            temp_date = start_date + timedelta(days=60)
            if end_date < (start_date + timedelta(days=60)):
                temp_date = end_date
            date_ranges.append((str(start_date), str(temp_date)))
            start_date = start_date + timedelta(days=61)
        return date_ranges

    def get_data(self, symbol, token, start_date, end_date, time_frame):
        date_ranges = self.get_date_range(start_date, end_date)
        all_results = []
        if not len(date_ranges):
            return
        for st_dt, en_dt in date_ranges:
            for i in range(2):
                try:
                    res = requests.get(
                        f"{self.data_url}/{token}/{time_frame}?user_id={self.user_id}&oi=1&from={st_dt}&to={en_dt}"
                        f"&ciqrandom={self.random_id}", headers=self.headers)
                    res = res.json()['data']['candles']
                    break
                except (TypeError, KeyError, JSONDecodeError):
                    logger.debug(f"Error getting data for {symbol} for {st_dt} to {en_dt}, trying again")
                    self.login()
            else:
                logger.debug(f"Error getting data for {symbol} for {st_dt} to {en_dt}")
                return
            all_results.extend(res)
        if not len(all_results):
            logger.debug(f"Empty data for {symbol}")
            return
        df = pd.DataFrame(all_results)
        df[0] = pd.to_datetime(df[0])
        df[0] = df[0].dt.tz_localize(None)
        df.index = df[0]
        df.index.name = 'datetime'
        df = df[[1, 2, 3, 4, 5]]
        df.columns = ["open", "high", "low", "close", "volume"]
        return df


    def load_instruments(self, exchanges: list = None):
        """
        Load all instruments
        :param exchanges: list of exchanges
        """
        logger.debug('Loading instruments, please wait...')
        if exchanges is None:
            exchanges = ['NFO', 'NSE', 'CDS', 'MCX']
        if not isinstance(exchanges, list) or not len([e for e in exchanges if e in self.available_exchanges]):
            logger.debug(f'Invalid exchanges: {exchanges}, Please provide list of exchange '
                         f'and it must contain one or more exchange from {self.available_exchanges}')
            return

        for exchange in exchanges:
            instruments = None
            while not instruments:
                try:
                    instruments = self.rest_client.instruments(exchange=exchange)
                    logger.debug(f'Exchange: {exchange}, No. of instruments: {len(instruments)}')
                except DataException as e:
                    logger.debug(e)
            # Add instruments to exchange_dict and all_instruments
            self.exchange_dict[exchange] = instruments
            self.all_instruments += instruments

        logger.debug('Instruments loaded')

    def map_instruments(self, symbols: list) -> list:
        """
        :param symbols: list of symbols
        :return: list of instruments containing token and other details for given list of symbols
        """
        logger.debug('Mapping instruments with parameters, please wait...')
        symbols = [s.upper() for s in symbols]
        instruments = [i for i in self.all_instruments if (i.get('tradingsymbol') in symbols)]
        logger.debug('Instruments mapped')
        return instruments

    def get_ltp(self, params: str) -> dict:
        """
        :param params: parameters to get ltp
        :return: ltp for given parameters
        """
        for i in range(10):
            try:
                ltp = requests.get(f'{self.root_trade_url}/quote/ltp?{params}', headers=self.headers)
                data = ltp.json()['data']
                if data is None or not len(data):
                    logger.debug(f'params: {params}, data: {data}, text: {ltp.text}')
                    continue
                return data
            except Exception as e:
                logger.exception(e)
                self.login()

    @staticmethod
    def map_option_strikes(ltp: float, strike_dist: float, strike_diff: int, ce_opts: list, pe_opts: list) -> list:
        """
        :param ltp: ltp
        :param strike_dist: distance from ATM strike
        :param strike_diff: strike diff, i.e.if 100 then only select strikes divisible by 100, for ex. 10100, 10200 etc
        :param ce_opts: call instruments
        :param pe_opts: put instruments
        :return: list of selected strike call and put instrument
        """
        ce_strikes = [i['strike'] for i in ce_opts]
        # Get closest call strike
        closest_ce_strike = min([i for i in ce_strikes if (i >= (ltp + strike_dist) and not i % strike_diff)])
        pe_strikes = [i['strike'] for i in pe_opts]
        # Get closest put strike
        closest_pe_strike = max([i for i in pe_strikes if (i <= (ltp - strike_dist) and not i % strike_diff)])
        call_instrument, put_instrument = None, None
        # Find option contracts with selected closest strikes
        for i in ce_opts:
            if i['strike'] == closest_ce_strike:
                call_instrument = i
                break
        for i in pe_opts:
            if i['strike'] == closest_pe_strike:
                put_instrument = i
                break

        if call_instrument is None or put_instrument is None:
            logger.exception(
                "could not find strike, Make sure you entered right strike diff and strike range values")
        else:
            return [call_instrument, put_instrument]

    def map_strikes_based_on_premium(self, opts: list, premium: float) -> Union[dict, None]:
        """
        :param opts: list of option instruments
        :param premium: premium amount
        :return: selected option instrument
        """
        # Create parameter to get ltp for all options
        opts = {f"{i['exchange']}:{i['tradingsymbol']}": i for i in opts}
        opts_params = ''.join([f"i={i}&" for i in opts])
        ltps = self.get_ltp(params=opts_params)
        if ltps is None or not len(ltps):
            logger.exception(f"could not find ltp for given options: {opts_params}")
            return
        diff = float('inf')
        instrument = None
        # Find option who's premium is closest to premium amount specified
        for i in ltps:
            ltp = ltps[i]['last_price']
            if abs(ltp - premium) < diff and i in opts:
                diff = abs(ltp - premium)
                instrument = opts[i]
        return instrument
