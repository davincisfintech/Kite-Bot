from datetime import datetime

from dateutil.parser import parse

from trading_bot.settings import logger, TZ

"""
Trade manager to handle order and trading related functions
"""


class OptTradeManager:
    def __init__(self, symbol, instrument_token, exchange, underlying_symbol, client, lot_size, lots, direction,
                 stop_loss, end_time, trail_sl, side=None, instruction=None, entered=False, entry_order_filled=False,
                 entry_order_status=None, entry_order_price=None, entry_order_id=None, bought=False, sold=False,
                 qty=None, sl=None, exit_order_id=None, exit_order_status=None, exit_order_price=None,
                 exit_pending=False, final_sl=None):
        """
        OptTradeManager class to handle trading operations

        Mandatory parameters
        :param symbol: trading symbol
        :param instrument_token: instrument token
        :param exchange: exchange
        :param underlying_symbol: underlying symbol
        :param client: trading client
        :param lot_size: lot size
        :param lots: number of lots
        :param direction: direction
        :param stop_loss: stop loss
        :param end_time: end time
        :param trail_sl: to specify trail sl or not
        """
        self.symbol = symbol
        self.exchange = exchange
        self.instrument_token = instrument_token
        self.underlying_symbol = underlying_symbol
        self.client = client
        self.lots = lots
        self.lot_size = lot_size
        self.direction = direction
        self.stop_loss = stop_loss
        self.end_time = end_time
        self.trail_sl = trail_sl
        self.variety = 'regular'
        self.product = 'MIS'
        self.ltp = None
        self.ltp_time = None
        self.side = side
        self.instruction = instruction
        self.qty = qty
        self.entered = entered
        self.bought = bought
        self.sold = sold
        self.sl = sl
        self.entry_order_time = None
        self.entry_order_type = 'LIMIT'
        self.entry_order_price = entry_order_price
        self.entry_order_id = entry_order_id
        self.entry_order_status = entry_order_status
        self.entry_order_filled = entry_order_filled
        self.entry_time = None
        self.entry_price = None
        self.position_status = None
        self.exit_order_type = 'SL'
        self.exit_order_time = None
        self.exit_order_price = exit_order_price
        self.exit_order_id = exit_order_id
        self.exit_order_status = exit_order_status
        self.exit_type = None
        self.exit_time = None
        self.exit_price = None
        self.exit_pending = exit_pending
        self.trade_ended = False
        self.start_price = None
        self.final_sl = final_sl
        self.messages = []
        logger.debug(f"""Trading instance created, symbol: {self.symbol}, exchange: {self.exchange}, 
                         instrument_token: {self.instrument_token}, lot_size: {self.lot_size}, lots: {self.lots},
                         underlying: {self.underlying_symbol}, end_time: {self.end_time}, stop_loss: {self.stop_loss}, 
                         end_time: {self.end_time}, trail_sl: {self.trail_sl}, direction: {self.direction}""")

    def __repr__(self):
        return f"<symbol: {self.symbol}, exchange: {self.exchange}, instrument_token: {self.instrument_token}>"

    def trade(self, tick: dict, order_data: dict):
        """
        :param tick: tick data
        :param order_data: order data
        :return: self or dict containing messages
        """
        # Return if trade is ended
        if self.trade_ended:
            return self

        # Set ltp and ltp_time
        self.ltp = tick['last_price']
        self.ltp_time = tick['last_trade_time']
        if not self.ltp or not self.ltp_time:
            return

        # Variable to store messages
        self.messages = []

        if self.is_valid_entry():  # If entry conditions match then take entry
            self.make_entry()

        if self.entered and not self.entry_order_filled:  # If entry order open then wait until it's complete
            self.confirm_entry(orders_data=order_data)

        if self.is_valid_exit():  # If exit conditions match then take exit
            self.make_exit()

        if self.entered and self.exit_pending:  # If exit order open then wait until it's complete
            self.confirm_exit(orders_data=order_data)
            if self.entered and self.exit_pending:
                # If end time is reached or trail sl set to true then check for order modification
                # and confirm exit again after that
                if datetime.now(tz=TZ).time() > self.end_time or self.trail_sl:
                    modify_reason = 'exit_time_reached' if datetime.now(tz=TZ).time() > self.end_time else 'trail_sl'
                    self.modify_exit(modify_reason=modify_reason)
                    self.confirm_exit(orders_data=order_data)

        return {'msg': self.messages}

    def is_valid_entry(self):
        """
        Check entry conditions
        """
        # If not already entered
        if self.entered:
            return

        # Based on direction specified set instruction take entry
        if self.direction == 'LONG':
            logger.info(f'Long signal generated for {self.symbol} at {datetime.now(tz=TZ)}, price: {self.ltp}')
            self.bought = True
            self.instruction = 'BUY'
            return True
        if self.direction == 'SHORT':
            logger.info(f'Short signal generated for {self.symbol} at {datetime.now(tz=TZ)}, price: {self.ltp}')
            self.sold = True
            self.instruction = 'SELL'
            return True

    def is_valid_exit(self):
        """
        Check exit conditions
        """
        if not self.entered or not self.entry_order_filled or self.exit_pending:
            return False

        positions = self.client.get_positions()
        if not positions:
            logger.debug(f'{self.symbol}: error retrieving positions')
            return

        close_entry_in_db = False

        # Check if open positions exist for exit
        if self.bought:
            for p in positions:
                if p['tradingsymbol'] == self.symbol and p['quantity'] > 0 and abs(p['quantity']) >= self.qty:
                    self.instruction = 'SELL'
                    return True
            else:
                close_entry_in_db = True
                logger.debug(f'{self.symbol} No long position exist for qty: {self.qty}, closing instance')

        elif self.sold:
            for p in positions:
                if p['tradingsymbol'] == self.symbol and p['quantity'] < 0 and abs(p['quantity']) >= self.qty:
                    self.instruction = 'BUY'
                    return True
            else:
                close_entry_in_db = True
                logger.debug(f'{self.symbol} No short position exist for qty: {self.qty}, closing instance')

        # If no open positions exist for exit then close trade in db
        if close_entry_in_db:
            self.exit_type, self.exit_time, self.exit_price = None, None, None
            self.exit_order_status, self.position_status = None, None
            self.entered, self.bought, self.sold, self.exit_pending = False, False, False, False
            confirm_exit_data = self.save_trade(action='confirm_exit')
            self.messages.append(confirm_exit_data)
            self.trade_ended = True

    def make_entry(self):
        """
        Make entry
        """
        # Set price and quantity
        price = float("{:0.1f}".format(self.ltp))
        self.qty = int(self.lots * self.lot_size)
        if self.qty <= 0:
            logger.debug(f'{self.symbol}, closing instance, qty less than or equal to 0')
            self.trade_ended = True
            return

        # Place order
        order_id = self.client.place_order(tradingsymbol=self.symbol, exchange=self.exchange,
                                           variety=self.variety, transaction_type=self.instruction,
                                           quantity=self.qty, order_type=self.entry_order_type,
                                           product=self.product, tag='algo_order', price=price, trigger_price=price)

        # If error in placing order then close instance
        if order_id is None:
            logger.debug(f'{self.symbol}: Error placing entry order, Closing instance')
            self.trade_ended = True
            return
        # If error placed successfully then set order details
        self.entry_order_id = order_id
        self.entry_order_price = price
        self.entered = True
        self.entry_order_filled = False
        self.entry_order_time = datetime.now(tz=TZ)
        self.entry_order_status = 'OPEN'
        self.side = self.instruction
        logger.debug(
            f"""Entry order placed to {self.instruction} {self.symbol}, qty: {self.qty}, price: {price}, 
                time: {self.entry_order_time}, order id:{self.entry_order_id}""")
        entry_data = self.save_trade(action='make_entry')
        self.messages.append(entry_data)

    def confirm_entry(self, orders_data: dict = None):
        """
        Confirm entry order
        :param orders_data: order data
        """
        # Fetch order data if specified orders_data is None
        if orders_data is None:
            orders_data = self.client.get_orders() if not self.client.all_orders else self.client.all_orders
            if orders_data is None:
                logger.debug(f'{self.symbol}:Error retrieving order')
                return
        for o in orders_data:
            # If order rejected or cancelled then close instance
            if o['order_id'] == self.entry_order_id and o['status'] in ['REJECTED', 'CANCELLED']:
                logger.debug(f"Entry Order Got {o['status']} in {self.symbol}, Reason: {o.get('status_message')}, "
                             f"closing instance")
                self.trade_ended = True
                self.entered = False
                self.bought, self.sold = False, False
                self.entry_time = None
                self.entry_price = None
                self.entry_order_status = o['status']
                self.position_status = None
                entry_data = self.save_trade(action='confirm_entry')
                self.messages.append(entry_data)
                return

            # If order complete then set order + sl details
            if o['order_id'] == self.entry_order_id and o['status'] == 'COMPLETE':
                self.entry_order_filled = True
                self.entry_order_status = o['status']
                self.entry_price = o['average_price']
                self.start_price = self.entry_price
                self.entry_time = parse(o['order_timestamp']) if 'order_timestamp' in o else datetime.now(tz=TZ)
                self.position_status = 'OPEN'
                if self.bought:
                    self.sl = self.entry_price * (1 - (self.stop_loss / 100))
                else:
                    self.sl = self.entry_price * (1 + (self.stop_loss / 100))
                self.final_sl = self.sl
                logger.debug(
                    f"""Entry order Filled to {self.instruction} {self.symbol}, qty: {self.qty}, 
                        price: {self.entry_price}, time: {self.entry_time}, order_id:{self.entry_order_id}, 
                        SL set to {self.sl}""")
                entry_data = self.save_trade(action='confirm_entry')
                self.messages.append(entry_data)
                return

    def make_exit(self):
        """
        Make exit
        """
        # Set price
        price = float("{:0.1f}".format(self.sl))
        order_id = self.client.place_order(tradingsymbol=self.symbol, exchange=self.exchange,
                                           variety=self.variety, transaction_type=self.instruction,
                                           quantity=self.qty, order_type=self.exit_order_type,
                                           product=self.product, tag='algo_order', price=price, trigger_price=price)
        if order_id is None:  # If error in placing order
            logger.debug(f'{self.symbol}: Error placing exit order')
            return

        # If error placed successfully then set order details
        self.exit_order_id = order_id
        self.exit_pending = True
        self.exit_order_time = datetime.now(tz=TZ)
        self.exit_order_price = price
        self.exit_order_status = 'OPEN'
        self.position_status = 'OPEN'
        logger.debug(
            f"""Exit order placed to {self.instruction} {self.symbol}, qty: {self.qty}, sl trigger price: {price}, 
                time: {self.exit_order_time}, order id:{self.exit_order_id}""")
        exit_data = self.save_trade(action='make_exit')
        self.messages.append(exit_data)

    def modify_exit(self, modify_reason: str):
        """
        Modifies exit order
        :param modify_reason: reason for modification
        """
        if modify_reason == 'trail_sl':
            # If reason is trail sl and trailing conditions match then trail sl
            # and modify exit order trigger and order price according to new sl
            if (self.bought and self.ltp > self.start_price) or (self.sold and self.ltp < self.start_price):
                prev_sl = self.final_sl
                if self.bought:
                    self.final_sl = self.sl + (self.ltp - self.start_price)
                else:
                    self.final_sl = self.sl - (self.start_price - self.ltp)
                self.exit_order_price = float("{:0.1f}".format(self.final_sl))
                order_id = self.client.modify_order(variety=self.variety, order_id=self.exit_order_id,
                                                    price=self.exit_order_price, trigger_price=self.exit_order_price)
                if not order_id:  # If error in modifying order
                    return

                # If order modified successfully
                logger.debug(
                    f'{self.symbol}: Exit {self.instruction} SL order price'
                    f' successfully modified from {prev_sl} to to {self.exit_order_price}, '
                    f' order id: {self.exit_order_id}')
                entry_data = self.save_trade(action='modify_exit')
                self.messages.append(entry_data)
            return

        # If reason is exit time reached then modify order type to market to make exit
        order_id = self.client.modify_order(variety=self.variety, order_id=self.exit_order_id, order_type='MARKET')
        if not order_id:  # If error in modifying order
            return
        logger.debug(f'{self.symbol}: exit time reached to exit order type modified to market, '
                     f'order id: {order_id}')

    def confirm_exit(self, orders_data: dict = None):
        """
        Confirm exit
        :param orders_data: order data
        """
        # Fetch order data if specified  orders_data is None
        if orders_data is None:
            orders_data = self.client.get_orders() if not self.client.all_orders else self.client.all_orders
            if orders_data is None:
                logger.debug(f'{self.symbol}:Error retrieving order')
                return
        for o in orders_data:
            # If order rejected or cancelled then place again
            if o['order_id'] == self.exit_order_id and o['status'] in ['REJECTED', 'CANCELLED']:
                logger.debug(f"Exit Order Got {o['status']} in {self.symbol}, Reason: {o.get('status_message')}")
                self.exit_pending = False
                return

            # If order complete then set order+position details and close instance
            if o['order_id'] == self.exit_order_id and o['status'] == 'COMPLETE':
                self.exit_pending = False
                self.exit_order_status = o['status']
                self.exit_price = o['average_price']
                self.exit_time = parse(o['order_timestamp']) if 'order_timestamp' in o else datetime.now(tz=TZ)
                self.exit_type = 'SL'
                self.position_status = 'CLOSED'
                self.bought, self.sold = False, False
                self.entered = False
                self.exit_pending = False
                logger.debug(
                    f"""Exit order Filled to {self.instruction} {self.symbol}, qty: {self.qty}, 
                        price: {self.exit_price}, time: {self.exit_time}, order_id:{self.exit_order_id}""")
                exit_data = self.save_trade(action='confirm_exit')
                self.messages.append(exit_data)

                self.trade_ended = True
                logger.debug(f'{self.symbol} Trade completed, closing instance')
                return

    def save_trade(self, action: str) -> dict:
        """
        :param action: state of trade, i.e. make_entry, make_exit etc
        :return: dict containing trade parameters
        """
        message = dict()
        if action == 'make_entry':
            message[action] = {'symbol': self.symbol, 'entry_order_time': self.entry_order_time,
                               'entry_order_price': self.entry_order_price, 'instruction': self.instruction,
                               'entry_order_id': self.entry_order_id, 'entry_order_status': self.entry_order_status,
                               'side': self.side, 'quantity': self.qty, 'exchange': self.exchange,
                               'direction': self.direction, 'underlying_symbol': self.underlying_symbol,
                               'end_time': self.end_time, 'lots': self.lots, 'lot_size': self.lot_size,
                               'stop_loss_percent': self.stop_loss, 'instrument_token': self.instrument_token,
                               'trail_sl': self.trail_sl}
            return message
        elif action == 'confirm_entry':
            message[action] = {'symbol': self.symbol, 'entry_order_id': self.entry_order_id,
                               'entry_order_status': self.entry_order_status, 'entry_time': self.entry_time,
                               'entry_price': self.entry_price, 'stop_loss': self.sl,
                               'position_status': self.position_status}
            return message
        elif action == 'make_exit':
            message[action] = {'symbol': self.symbol, 'entry_order_id': self.entry_order_id,
                               'position_status': self.position_status, 'exit_order_id': self.exit_order_id,
                               'exit_order_time': self.exit_order_time, 'exit_order_status': self.exit_order_status,
                               'exit_order_price': self.exit_order_price}
            return message
        elif action == 'modify_exit':
            message[action] = {'symbol': self.symbol, 'entry_order_id': self.entry_order_id,
                               'final_stop_loss': self.final_sl,
                               'exit_order_price': self.exit_order_price}
            return message
        elif action == 'confirm_exit':
            message[action] = {'symbol': self.symbol, 'entry_order_id': self.entry_order_id,
                               'position_status': self.position_status, 'exit_time': self.exit_time,
                               'exit_price': self.exit_price, 'exit_type': self.exit_type,
                               'exit_order_status': self.exit_order_status}
            return message
