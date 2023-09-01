"""
Entry point of trading bot
"""
if __name__ == '__main__':
    kwargs = {i: j for i, j in locals().items() if not i.startswith('__')}

    from trading_bot.controller import run

    run()
