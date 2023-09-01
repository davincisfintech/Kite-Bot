import warnings
import time

import pandas as pd

from trading_bot.database.db import engine
from trading_bot.settings import logger, BASE_DIR

warnings.filterwarnings('ignore')

"""
Metrics to export db data of closed positions to excel
"""


def calc_gross(entry_val: float, exit_val: float, side: str):
    """"
    :param entry_val: entry value
    :param exit_val: exit value
    :param side: side
    :return: gross profit
    """
    return exit_val - entry_val if side.upper() == 'BUY' else entry_val - exit_val


def generate_metrics():
    """
    Generate metrics and export it to trade_results.xlsx file
    """
    df = pd.read_sql('trades_data', engine)
    # df = df[df['position_status'] == "CLOSED"]
    if not len(df):
        logger.debug('No positions yet')
        return
    df['entry_value'] = df['entry_price'] * df['quantity']
    df['exit_value'] = df['exit_price'] * df['quantity']
    df['gross_profit'] = df[['entry_value', 'exit_value', 'side']].apply(lambda x: calc_gross(*x), axis=1)
    del df['instruction']

    df = df.round(2)
    df = df.sort_values(by='entry_time', ascending=True)
    df.index = df['entry_time']
    df.index.names = ['index']
    df['entry_time'] = df['entry_time'].astype(str)
    df['exit_time'] = df['exit_time'].astype(str)
    total_gross = df["gross_profit"].sum()
    df.loc["total_net"] = pd.Series([total_gross], index=['gross_profit'])
    df.to_excel(BASE_DIR / 'trade_results.xlsx', index=False)
    logger.info('results generated, check trade_results.xlsx file for it')
    time.sleep(5)


if __name__ == '__main__':
    generate_metrics()
