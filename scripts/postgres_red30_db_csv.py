import psycopg2

connection = psycopg2.connect(
    database='red30',
    user='postgres',
    password='',
    host='localhost',
    port='5432'
)

cursor = connection.cursor()
cursor.execute('DROP TABLE IF EXISTS Sales;')
cursor.execute('''
CREATE TABLE Sales(
    order_num INT PRIMARY KEY,
    order_type TEXT,
    cust_name TEXT,
    cust_state TEXT,
    prod_category TEXT,
    prod_number TEXT,
    prod_name TEXT,
    quantity INT,
    price REAL,
    discount REAL,
    order_total REAL
);
''')

connection.commit()

# Refer: https://stackoverflow.com/questions/25566386/copy-from-csv-with-heades-in-postgres-with-python
with open(r'data/red30.csv', 'r') as f:
    copy_sql = """COPY sales FROM stdin WITH CSV HEADER DELIMITER AS ',' """
    cursor.copy_expert(sql=copy_sql, file=f)
    connection.commit()


cursor.execute('SELECT * FROM sales LIMIT 10;')
print(cursor.fetchall())
connection.close()