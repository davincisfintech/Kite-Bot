import json
from collections import defaultdict
from queue import Queue
from threading import Thread

import websocket
from kiteconnect import KiteTicker

from trading_bot.settings import logger

"""
Kite streamer to get real time data feed
"""


class KiteStreamer:
    def __init__(self, user_id: str, ws_token: str):
        """
        KiteStreamer class to stream real time data for subscribed instruments
        :param user_id: zerodha kite user id
        :param ws_token: authentication token
        """
        self.user_id = user_id
        self.ws_url = 'wss://ws.zerodha.com'
        self.ua_string = 'user-agent=kite3-web&version=2.9.3'
        self.key_string = 'kitefront'
        self.uid = "1605085892719"
        self.ws_token = ws_token
        self.connection_string = f'{self.ws_url}/?api_key={self.key_string}&user_id={self.user_id}&' \
                                 f'enctoken={self.ws_token}&uid={self.uid}&{self.ua_string}'
        self.ws_client = KiteTicker(self.key_string, self.ws_token)
        self.ws = None

        # Variable for string orders, ticks and subscribed instruments
        self.orders_queue = defaultdict(list)
        self.ticks_queue = Queue()
        self.subscribed_instruments = []

    def on_message(self, ws, message):
        """
        Receive web socket message
        """
        if isinstance(message, str):
            # If it's order update
            message = json.loads(message)
            if 'tradingsymbol' in message:
                # Store order data
                self.orders_queue[message['tradingsymbol']].append(message)
            return
        # If it's ticks data
        ticks = self.ws_client._parse_binary(message)  # Parse binary data
        self.ticks_queue.put(ticks)

    def subscribe(self, instruments: list):
        """
        sunscribe instruments for live data
        :param instruments: list of instrument tokens
        """
        if len(instruments):
            self.ws.send(json.dumps({"a": "mode", "v": ["full", instruments]}))
            self.subscribed_instruments.extend(instruments)

    def on_error(self, ws, error):
        """
        Handle error
        """
        logger.debug(error)
        self.start_streaming()

    def on_close(self, ws, code, reason):
        """
        Handle WS close event
        """
        pass

    def on_open(self, ws):
        """
        Handle WS close event
        """
        pass

    def start_streaming(self):
        """
        Start streaming data
        """
        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(self.connection_string,
                                         on_message=self.on_message,
                                         on_error=self.on_error,
                                         on_close=self.on_close)
        self.ws.on_open = self.on_open
        self.ws.on_message = self.on_message
        self.ws.on_close = self.on_close

        # Create new thread and run it in background
        wst = Thread(target=self.ws.run_forever)
        wst.daemon = True
        wst.start()
