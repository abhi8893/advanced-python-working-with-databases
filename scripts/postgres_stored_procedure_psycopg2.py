import psycopg2

DATABASE = 'red30'

def add_filler():
    print('-'*10)

if __name__ == '__main__':
    conn = psycopg2.connect(
        database=DATABASE,
        host='localhost',
        user='postgres',
        password='',
        port='5432')


    cur = conn.cursor()

    # NOTE: These two stored procedures do not have COMMIT; statement
    #       as that would require the connection to be in autocommit mode.

    # Stored Procedure return_nondiscounted_items
    print('Creating stored procedure return_nondiscounted_items...'); add_filler();
    cur.execute('''
    CREATE OR REPLACE PROCEDURE return_nondiscounted_items(INT, INT)
    LANGUAGE plpgsql
    AS $$
    BEGIN
    UPDATE sales
    SET quantity = quantity - $2, order_total = order_total - price*$2
    WHERE discount = 0 AND order_num = $1;
    END;
    $$;
    ''')

    # Stored Procedure buy_more_nondiscounted_items
    print('Creating stored procedure buy_more_nondiscounted_items...'); add_filler();
    cur.execute('''
    CREATE OR REPLACE PROCEDURE buy_more_nondiscounted_items(INT, INT)
    LANGUAGE plpgsql
    AS $$
    BEGIN
    UPDATE sales
    SET quantity = quantity + $2, order_total = order_total + price * $2
    WHERE discount = 0 AND order_num = $1;
    END;
    $$;
    ''')

    conn.commit();

    # Get previous maximum order_num
    cur.execute('''SELECT max(order_num) FROM sales''')
    max_order_num = cur.fetchone()[0]

    # Insert a record with 0 discount
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
    
    # Insert the sale data
    print('Inserting new sale data...'); add_filler();
    cur.execute('''
    INSERT INTO sales
    (order_num, order_type, cust_name, cust_state, prod_category,
     prod_number, prod_name, quantity, price, discount, order_total)
    VALUES
    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ''', tuple(sale_data.values()))
    conn.commit()

    # Show newly inserted sale data
    cur.execute('SELECT * FROM sales WHERE order_num = %s', (max_order_num+1,))
    print(cur.fetchall()[0]); add_filler();

    # Return nondiscounted item for previous insert
    print('Returning nondiscounted item...'); add_filler();
    cur.execute('CALL return_nondiscounted_items(%s, %s)', (max_order_num+1, 1))
    conn.commit()

    # Show again
    cur.execute('SELECT * FROM sales WHERE order_num = %s', (max_order_num+1,))
    print(cur.fetchall()[0]); add_filler();

    # Buy again
    print('Buying more nondiscounted items...'); add_filler();
    cur.execute('CALL buy_more_nondiscounted_items(%s, %s)', (max_order_num+1, 2))
    conn.commit()

    # Show again
    cur.execute('SELECT * FROM sales WHERE order_num = %s', (max_order_num+1,))
    print(cur.fetchall()[0]); add_filler();

    # Delete
    print('Deleting inserted record...'); add_filler();
    cur.execute('DELETE FROM sales WHERE order_num = %s', (max_order_num+1,))
    conn.commit()

    conn.close();