from operator import add
from sqlalchemy import create_engine, MetaData, Table, func as sqlfunc
from sqlalchemy.orm import sessionmaker

DATABASE = 'red30'
engine = create_engine(f'postgresql://postgres:@localhost:5432/{DATABASE}')
Session = sessionmaker()
session = Session()

def get_last_row(session, table):
    row_count = session.query(table).count()
    query = sales_table.select().offset(row_count - 1)
    return session.execute(query).first()


def add_filler():
    print('-'*10)


with engine.connect() as conn:

    meta = MetaData(engine)
    sales_table = Table('sales', meta, autoload=True, autoload_with=engine)

    # Select first 10 rows
    print('Selecting first 10 rows...')
    add_filler()
    select_statement = sales_table.select().limit(10)
    result = conn.execute(select_statement)
    for row in result:
        print(row)

    add_filler()

    # Delete previously inserted data
    delete_statement = sales_table.delete().where(sales_table.c.cust_name == 'Akshay Bhatia')
    conn.execute(delete_statement)

    # Get max of order_num
    max_order_num = session.query(sqlfunc.max(sales_table.c.order_num)).scalar()

    # Create data to insert
    sale_data = dict(
        order_num=max_order_num+1,
        order_type='Retail',
        cust_name='Akshay',
        cust_state='Prague',
        prod_category='Electronics',
        prod_number='EL420',
        prod_name='PS5 Digital Edition',
        quantity=1,
        price=499,
        discount_perc=20
    )

    # Calculate other fields
    initial_total = sale_data['price']*sale_data['quantity']
    sale_data['discount'] = initial_total*sale_data['discount_perc']/100
    sale_data['order_total'] = initial_total - sale_data['discount']
    del sale_data['discount_perc']

    # Insert data
    print('Inserting new sale data...')
    add_filler()
    insert_statement = sales_table.insert().values(**sale_data)
    conn.execute(insert_statement)

    # Show inserted data
    print('Selecting inserted data...')
    add_filler()
    select_statement = sales_table.select().where(sales_table.c.order_num == max_order_num+1)
    result = conn.execute(select_statement)
    for row in result:
        print(row)
    
    add_filler()

    # Update Akshay's name
    print('Updating cust_name in inserted data...')
    add_filler()
    update_statement = sales_table.update().where(sales_table.c.order_num == max_order_num+1).values(cust_name='Akshay Bhatia')
    conn.execute(update_statement)

    # Reselect inserted data
    print('Reselecting updated data...')
    select_statement = sales_table.select().where(sales_table.c.order_num == max_order_num+1)
    result = conn.execute(select_statement)
    for row in result:
        print(row)
    
    add_filler()


