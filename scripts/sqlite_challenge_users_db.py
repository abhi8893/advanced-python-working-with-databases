import sqlite3
import os

DATABASE_PATH = 'data/users.db'

try:
    os.remove(DATABASE_PATH)
except FileNotFoundError:
    pass

connection = sqlite3.connect('data/users.db')
cursor = connection.cursor()

cursor.execute("""
CREATE TABLE users(
    user_id INTEGER PRIMARY KEY AUTO INCREMENT,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT GENERATED ALWAYS AS
    (lower(first_name) || '.' || lower(last_name) || '@email.com')
)
""")

users_details = [
    ('Abhishek', 'Bhatia'),
    ('Seema', 'Bhatia'),
    ('Dinesh', 'Bhatia'),
    ('Akshay', 'Bhatia'),
    ('Divya', 'Bhatia'),
    ('Anurima', 'Dey')
]

cursor.executemany('INSERT INTO users (first_name, last_name) values(?,?)',
                   users_details)
connection.commit()

cursor.execute('SELECT * FROM users;')
print(cursor.fetchall())

connection.close()
