import mysql.connector as mysql
import csv

connection = mysql.connect(user='root', password='',
                           host='localhost', database='sales',
                           allow_local_infile=True)

cursor = connection.cursor()

create_query = """
CREATE TABLE salespeople(
    id INT(11) NOT NULL AUTO_INCREMENT,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    email_address VARCHAR(255) NOT NULL,
    city VARCHAR(255) NOT NULL,
    state VARCHAR(255) NOT NULL,
    PRIMARY KEY (id)
);
"""
cursor.execute('DROP TABLE IF EXISTS salespeople;')
cursor.execute(create_query)

# One way of loading in data from csv file
# with open('data/salespeople.csv', 'r') as f:
#     csv_data = csv.reader(f)
#     for row in csv_data:
#         row_tuple = tuple(row)
#         cursor.execute(
#             "INSERT INTO salespeople(first_name, last_name, email_address, city, state) VALUES(%s,%s,%s,%s,%s)",
#             row_tuple)

# Other way
# See https://stackoverflow.com/questions/10935219/mysql-load-data-infile-works-but-unpredictable-line-terminator
# for handling \r character at end
csv_load_query = """
    LOAD DATA LOCAL INFILE 'data/salespeople.csv'
    INTO TABLE salespeople
    FIELDS TERMINATED BY ',' ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    (first_name, last_name, email_address, city, @var)
    SET state = TRIM(TRAILING '\r' FROM @var)
    """

cursor.execute(csv_load_query)
connection.commit()

cursor.execute("SELECT * FROM salespeople LIMIT 10;")
print(cursor.fetchall())

connection.close()
