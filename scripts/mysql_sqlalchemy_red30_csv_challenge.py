from sqlalchemy import Column, Integer, String, Float
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import func as sqlfunc
import pandas as pd

DATABASE = 'red30'
engine = create_engine(f'mysql+mysqlconnector://root:@localhost:3306/{DATABASE}')

Base = declarative_base()

class Sale(Base):
    __tablename__ = 'sales'
    __table_args__ = {'schema': DATABASE}

    order_num = Column(Integer, primary_key=True)
    order_type = Column(String(255))
    cust_name = Column(String(255))
    cust_state = Column(String(255))
    prod_category = Column(String(255))
    prod_number = Column(String(255))
    prod_name = Column(String(255))
    quantity = Column(Integer)
    price = Column(Float)
    discount = Column(Float)
    order_total = Column(Float)


    def __repr__(self):
        cls = self.__class__.__name__
        cols = self.__table__.columns.keys()
        attr_vals_str = ','.join([f'{c}={repr(self.__getattribute__(c))}' for c in cols])
        return f'{cls}({attr_vals_str})'

Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)


df = pd.read_csv('data/red30.csv')
df.to_sql(con=engine, name=Sale.__tablename__, if_exists='replace', index=False)

Session = sessionmaker()
Session.configure(bind=engine)


with Session() as session:
    max_total = session.query(sqlfunc.max(Sale.order_total)).scalar()
    order_max = session.query(Sale).where(Sale.order_total == max_total).first()

    print(order_max)
