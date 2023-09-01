from datetime import datetime

from trading_bot.settings import TZ, logger

"""
Strike selection strategy
"""


class StrikeSelection:
    def __init__(self, client, symbol, exchange, expiry_date, start_time, end_time, lots,
                 opt_type='both', call_premium=None, put_premium=None, strike_dist=0, strike_diff=1):
        """
        StrikeSelection class to select strikes based on given parameter
        :param client: trading client
        :param symbol: trading symbol
        :param exchange: exchange
        :param expiry_date: contract expiry date
        :param start_time: start time
        :param end_time: end time
        :param lots: number of lots
        :param opt_type: option type i.e. call/put
        :param call_premium: call premium amount
        :param put_premium: put premium amount
        :param strike_dist: distance from ATM strike
        :param strike_diff: strike diff, i.e.if 100 then only select strikes divisible by 100, for ex. 10100, 10200 etc
        """
        self.client = client
        self.symbol = symbol
        self.exchange = exchange
        self.expiry_date = expiry_date
        self.call_premium = call_premium
        self.put_premium = put_premium
        self.strike_dist = strike_dist
        self.strike_diff = strike_diff
        self.opt_type = opt_type
        self.start_time = start_time
        self.end_time = end_time
        self.lots = lots
        self.strikes_retrieved = False

        # Create list of call and put instruments
        index_name_mapper = {'NIFTY 50': 'NIFTY', 'NIFTY BANK': 'BANKNIFTY'}
        symbol = self.symbol if self.symbol not in index_name_mapper else index_name_mapper[self.symbol]
        opts = [i for i in self.client.all_instruments if
                (symbol == i['name'] and i['segment'] == 'NFO-OPT' and i['expiry'] == self.expiry_date)]
        self.ce_opts = [i for i in opts if i['instrument_type'] == 'CE']
        self.pe_opts = [i for i in opts if i['instrument_type'] == 'PE']
        logger.debug(f"""Strategy instance created, symbol: {self.symbol}, exchange: {self.exchange}, 
                         expiry_date: {self.expiry_date}, lots: {self.lots},
                         start_time: {self.start_time}, end_time: {self.end_time}""")

    def get_strikes(self):
        """
        Find strikes
        :return: list of option instruments if strikes retrieved, str if start time not reached yet
        """
        if not len(self.ce_opts) or not len(self.pe_opts):
            logger.debug(f'{self.symbol}: No option contracts found for expiry date: {self.expiry_date}, '
                         f'Make sure you provided right parameters')
            self.strikes_retrieved = True
            return
        # Wait until start time reached
        if datetime.now(tz=TZ).time() < self.start_time:
            return 'wait'
        self.strikes_retrieved = True

        # If call premium and put_premium specified then retrieve strikes based on that
        if self.call_premium and self.put_premium:
            call_instrument = self.client.map_strikes_based_on_premium(self.ce_opts, self.call_premium)
            put_instrument = self.client.map_strikes_based_on_premium(self.pe_opts, self.put_premium)
            if call_instrument is None or put_instrument is None:
                return
        else:  # Else retrieve strikes based on distance to atm i.e. using strike_dist and strike_diff parameters
            # Get ltp of underlying
            key = f'{self.exchange}:{self.symbol}'
            params = f'i={key}'
            ltp = self.client.get_ltp(params=params)
            if ltp is None:
                logger.exception(f"could not find ltp for given params: {params}")
                return
            ltp = ltp[key]['last_price']
            # Get option instrument based underlying ltp and strike_dist and strike_diff parameters
            instruments = self.client.map_option_strikes(ltp, self.strike_dist, self.strike_diff, self.ce_opts,
                                                         self.pe_opts)
            if instruments is None:
                return
            call_instrument, put_instrument = instruments

        call_instrument['lots'] = put_instrument['lots'] = self.lots
        call_instrument['end_time'] = put_instrument['end_time'] = self.end_time
        if self.opt_type == 'CALL':
            return [call_instrument]
        elif self.opt_type == 'PUT':
            return [put_instrument]
        return [call_instrument, put_instrument]
