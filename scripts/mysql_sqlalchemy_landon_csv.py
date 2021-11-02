import pandas as pd
from sqlalchemy import Column, Integer, Float, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy import create_engine

DATABASE = 'landon'
engine = create_engine(f'mysql+mysqlconnector://root:@localhost:3306/{DATABASE}')

Base = declarative_base()

class Purchase(Base):
    __tablename__ = 'purchases'
    __table_args__ = {'schema': DATABASE}

    order_id = Column(Integer, primary_key=True)
    property_id = Column(Integer)
    property_city = Column(String(250))
    property_state = Column(String(250))
    product_id = Column(Integer)
    product_category = Column(String(250))
    product_name = Column(String(250))
    quantity = Column(Integer)
    product_price = Column(Float)
    order_total = Column(Float)

    def __repr__(self):
        cols = self.__table__.columns.keys()
        cls = self.__class__.__name__
        attr_val_str = ', '.join([f'{c}={repr(self.__getattribute__(c))}' for c in cols])
        return f'{cls}({attr_val_str})'

Base.metadata.create_all(engine)

fpath = 'data/landon.csv'
df = pd.read_csv(fpath)
df.to_sql(con=engine, name=Purchase.__tablename__, if_exists='replace', index=False)

Session = sessionmaker()
Session.configure(bind=engine)

with Session() as session:
    results = session.query(Purchase).limit(10).all()


for row in results:
    print(row)