from sqlalchemy import Column, String, Integer, DECIMAL, DATETIME, TIME, BOOLEAN
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

from trading_bot.settings import BASE_DIR, logger

"""
Database schema and settings
"""

# Connection settings for sqlite db
db_connection_url = f'sqlite:///{str(BASE_DIR)}/trades.sqlite3'

engine = create_engine(db_connection_url, echo=False, connect_args={'check_same_thread': False})
base = declarative_base()


class TradesData(base):
    __tablename__ = 'trades_data'
    underlying_symbol = Column(String)
    symbol = Column(String, primary_key=True)
    instrument_token = Column(String)
    end_time = Column(TIME)
    lot_size = Column(Integer)
    lots = Column(Integer)
    trail_sl = Column(BOOLEAN)
    stop_loss_percent = Column(DECIMAL)
    direction = Column(String)
    exchange = Column(String)
    side = Column(String)
    instruction = Column(String)
    quantity = Column(Integer)
    entry_order_time = Column(DATETIME)
    entry_order_price = Column(DECIMAL)
    entry_order_status = Column(String)
    entry_order_id = Column(String, primary_key=True)
    entry_price = Column(DECIMAL, nullable=True)
    entry_time = Column(DATETIME, nullable=True)
    stop_loss = Column(DECIMAL, nullable=True)
    final_stop_loss = Column(DECIMAL, nullable=True)
    position_status = Column(String, nullable=True)
    exit_order_time = Column(DATETIME, nullable=True)
    exit_order_price = Column(DECIMAL, nullable=True)
    exit_order_status = Column(String, nullable=True)
    exit_time = Column(DATETIME, nullable=True)
    exit_type = Column(String, nullable=True)
    exit_price = Column(DECIMAL, nullable=True)
    exit_order_id = Column(String, nullable=True)

    def __repr__(self):
        return f"<symbol: {self.symbol}, side: {self.side}, qty: {self.quantity}>"

    def save_to_db(self):
        """
        Save object
        """
        try:
            session.add(self)
            session.commit()
        except Exception as e:
            logger.exception(e)
            session.rollback()
        finally:
            session.close()

    def delete_from_db(self):
        """
        Delete object
        """
        session.delete(self)
        session.commit()

    @staticmethod
    def commit_changes():
        """
        Commit changes
        """
        try:
            session.commit()
        except Exception as e:
            logger.exception(e)
            session.rollback()
        finally:
            session.close()


# Create session
Session = sessionmaker(engine)
Session = scoped_session(Session)
session = Session()

base.metadata.create_all(engine)
