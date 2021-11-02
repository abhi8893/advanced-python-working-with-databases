import psycopg2
from functools import reduce

DATABASE = 'red30'

def get_column_names(conn, table):
    cur = conn.cursor()
    cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='{table}'")
    cols = reduce(lambda x, y: x+y, cur.fetchall())
    return cols

def get_last_row(conn, table):
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM {table} OFFSET ((SELECT count(1) FROM {table}) - 1)')
    return cur.fetchall()

def insert_sale(conn, order_type, cust_name, cust_state, prod_category,
                prod_number, prod_name, quantity, price, discount_perc):

    last_row = get_last_row(conn, 'sales')
    order_num = last_row[0][0] + 1
    
    discount = quantity*price*discount_perc
    order_total = quantity*price - discount
    sale_data = (order_num, order_type, cust_name, cust_state, prod_category,
                 prod_number, prod_name, quantity, price, discount, order_total)

    cur = conn.cursor()
    cur.execute('''
    INSERT INTO sales
    (order_num, order_type, cust_name, cust_state, prod_category,
     prod_number, prod_name, quantity, price, discount, order_total)
    VALUES
    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''', sale_data)
    conn.commit()

    cur.execute('''SELECT cust_name, order_total FROM sales WHERE order_num=%s''', (order_num,))
    print(cur.fetchall())


if __name__ == "__main__":
    conn = psycopg2.connect(
        host='localhost',
        user='postgres',
        password='',
        port='5432',
        database=DATABASE
    )

    cur = conn.cursor()
    headers = list(get_column_names(conn, 'sales')[1:-1])
    headers[-1] = 'discount_perc'

    print('Enter sale info ->\n', '-'*20)
    sale_input = {} 
    for header in headers:
        sale_input[header] = input(f'{header}: ')

    sale_input['quantity'] = int(sale_input['quantity'])
    sale_input['price'] = float(sale_input['price'])
    sale_input['discount_perc'] = float(sale_input['quantity'])

    insert_sale(conn, **sale_input)