from sqlalchemy import create_engine, func as sqlfunc
from sqlalchemy.sql import text as sqltext
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

DATABASE = 'red30'
engine = create_engine(f'postgresql://postgres:@localhost:5432/{DATABASE}', isolation_level='AUTOCOMMIT')
Base = declarative_base(engine)
Base.metadata.reflect(engine)


class Sale(Base):
    __table__ = Base.metadata.tables['sales']

    def __repr__(self):
        cls = self.__class__.__name__
        cols = self.__table__.columns.keys()
        attr_vals_str = ', '.join([f'{c}={repr(self.__getattribute__(c))}' for c in cols])
        return f'{cls}({attr_vals_str})'

def load_session():
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

def add_filler():
    print('-'*10)


if __name__ == '__main__':
    session = load_session()

    # Retrieve max order_num
    print('Retrieving previous maximum order_num...'); add_filler();
    max_order_num = session.query(sqlfunc.max(Sale.order_num)).scalar()
    print(max_order_num); add_filler();

    # Insert new sale
    sale_data = dict(
        order_num=max_order_num+1,
        order_type='Retail',
        cust_name='Seema',
        cust_state='UP',
        prod_category='Misc',
        prod_number='MS123',
        prod_name='Diwali Moon',
        quantity=3,
        price=20,
        discount_perc=0
    )

    # Calculate other fields
    initial_total = sale_data['price']*sale_data['quantity']
    sale_data['discount'] = initial_total*sale_data['discount_perc']/100
    sale_data['order_total'] = initial_total - sale_data['discount']
    del sale_data['discount_perc']

    # Create new Sale object
    print('Inserting new sale...'); add_filler();
    new_sale = Sale(**sale_data)
    session.add(new_sale)
    session.commit()

    # Retrieve inserted record
    inserted_sale = session.query(Sale).where(Sale.order_num == max_order_num + 1).first()
    print(inserted_sale)

    # Return items
    print('Returning items in inserted sale...'); add_filler();
    session.execute(sqltext('CALL return_nondiscounted_items(:order_num, :quantity);'), 
                    dict(order_num=max_order_num+1, quantity=1))
    
    # Retrieve inserted record
    inserted_sale = session.query(Sale).where(Sale.order_num == max_order_num + 1).first()
    print(inserted_sale)


    # Rebuy items
    print('Rebuying items in inserted sale...'); add_filler();
    session.execute(sqltext('CALL buy_more_nondiscounted_items(:order_num, :quantity)'),
                    dict(order_num=max_order_num+1, quantity=2))


    # Retrieve inserted record
    inserted_sale = session.query(Sale).where(Sale.order_num == max_order_num + 1).first()
    print(inserted_sale); add_filler();


    # Deleting inserted record
    print('Deleting inserted record...'); add_filler();
    session.delete(inserted_sale)
    session.commit();