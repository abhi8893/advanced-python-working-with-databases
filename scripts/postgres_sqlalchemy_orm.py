from sqlalchemy import create_engine, func as sqlfunc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

DATABASE = 'red30'
engine = create_engine(f'postgresql://postgres:@localhost:5432/{DATABASE}')
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

    # Smallest sale
    print('Retrieving smallest sale...'); add_filler();
    smallest_sales = session.query(Sale).order_by(Sale.order_total).limit(10)
    print(smallest_sales[0]); add_filler();

    # Retrieve max order_num
    print('Retrieving previous maximum order_num...'); add_filler();
    max_order_num = session.query(sqlfunc.max(Sale.order_num)).scalar()
    print(max_order_num); add_filler();

    # Insert new sale
    sale_data = dict(
        order_num=max_order_num+1,
        order_type='Retail',
        cust_name='Abhishek',
        cust_state='UP',
        prod_category='Electronics',
        prod_number='EL980',
        prod_name='IPhone 13 Pro Max',
        quantity=1,
        price=899,
        discount_perc=20
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

    # Reselect inserted sale data
    print('Retrieving inserted sale...'); add_filler();
    inserted_sale = session.query(Sale).where(Sale.order_num == max_order_num+1);
    print(inserted_sale[0]); add_filler();

    # Update inserted sale data    
    print('Updating inserted sale data...'); add_filler();
    new_sale.cust_name = 'Abhishek Bhatia'
    session.commit()
    updated_sale = session.query(Sale).filter(Sale.order_num == max_order_num+1).first()
    print(updated_sale); add_filler();

    # Deleting inserted sale data
    print('Deleting inserted sale data...'); add_filler();
    recent_sale = session.query(Sale).filter(Sale.order_num == max_order_num+1).first()
    session.delete(recent_sale)
    session.commit()
    add_filler();
