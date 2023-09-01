import warnings

from trading_bot.database.db import TradesData, session
from trading_bot.settings import logger

warnings.filterwarnings('ignore')

"""
Database handler to store trades based on given action
"""


def save_trade(action: str, params: dict) -> None:
    """
    :param action: Specifies action type for trade i.e. make_entry, make_exit etc
    :param params: details to be stored based on given action
    :return: None
    """
    if action == 'make_entry':
        obj = TradesData(**params)
        obj.save_to_db()
        logger.debug(f'Trade Saved for {params["symbol"]} for action: {action}')
    elif action == 'confirm_entry':
        obj = session.query(TradesData).filter(TradesData.symbol == params['symbol'],
                                               TradesData.entry_order_id == params['entry_order_id'],
                                               TradesData.entry_order_status == 'OPEN').first()
        if not obj:
            logger.debug(f'Trade not found for {params["symbol"]}, entry_order_id: {params["entry_order_id"]}')
            return
        obj.entry_time = params['entry_time']
        obj.entry_price = params['entry_price']
        obj.stop_loss = params['stop_loss']
        obj.entry_order_status = params['entry_order_status']
        obj.position_status = params['position_status']
        obj.commit_changes()
        logger.debug(f'Trade modified for {params["symbol"]} for action: {action}')
    elif action == 'make_exit':
        obj = session.query(TradesData).filter(TradesData.symbol == params['symbol'],
                                               TradesData.entry_order_id == params['entry_order_id'],
                                               TradesData.position_status == 'OPEN').first()
        if not obj:
            logger.debug(f'Position not found for {params["symbol"]}, entry_order_id: {params["entry_order_id"]}')
            return
        obj.exit_order_id = params['exit_order_id']
        obj.exit_order_time = params['exit_order_time']
        obj.exit_order_price = params['exit_order_price']
        obj.exit_order_status = params['exit_order_status']
        obj.commit_changes()
        logger.debug(f'Trade modified for {params["symbol"]} for action: {action}')
    elif action == 'modify_exit':
        obj = session.query(TradesData).filter(TradesData.symbol == params['symbol'],
                                               TradesData.entry_order_id == params['entry_order_id'],
                                               TradesData.exit_order_status == 'OPEN').first()
        if not obj:
            logger.debug(f'Trade not found for {params["symbol"]}, entry_order_id: {params["entry_order_id"]}')
            return
        obj.final_stop_loss = params['final_stop_loss']
        obj.exit_order_price = params['exit_order_price']
        obj.commit_changes()
        logger.debug(f'Trade modified for {params["symbol"]} for action: {action}')
    elif action == 'confirm_exit':
        obj = session.query(TradesData).filter(TradesData.symbol == params['symbol'],
                                               TradesData.entry_order_id == params['entry_order_id'],
                                               TradesData.position_status == 'OPEN').first()
        if not obj:
            logger.debug(f'Open Position not found for {params["symbol"]}, entry_order_id: {params["entry_order_id"]}')
            return
        obj.position_status = params['position_status']
        obj.exit_time = params['exit_time']
        obj.exit_price = params['exit_price']
        obj.exit_type = params['exit_type']
        obj.exit_order_status = params['exit_order_status']
        obj.commit_changes()
        logger.debug(f'Trade modified for {params["symbol"]} for action: {action}')
