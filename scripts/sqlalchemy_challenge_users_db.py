import sqlalchemy as db
import os

DATABASE_PATH = 'data/users.db'
try:
    os.remove(DATABASE_PATH)
except FileNotFoundError:
    pass

engine = db.create_engine(f'sqlite:///{DATABASE_PATH}')
connection = engine.connect()
metadata = db.MetaData()

users = db.Table(
    'Users', metadata,
    db.Column('user_id', db.INTEGER, primary_key=True),
    db.Column('first_name', db.TEXT),
    db.Column('last_name', db.TEXT),
    db.Column('email', db.TEXT)
    )

metadata.create_all(engine)

users_details = [
    ('Abhishek', 'Bhatia'),
    ('Seema', 'Bhatia'),
    ('Dinesh', 'Bhatia'),
    ('Akshay', 'Bhatia'),
    ('Divya', 'Bhatia'),
    ('Anurima', 'Dey')
]


query = users.insert().values([
    {'first_name': f, 'last_name': l,
     'email': f'{f.lower()}.{l.lower()}@email.com'}
    for f, l in users_details
    ])

connection.execute(query)

query = db.select(users)
result_proxy = connection.execute(query)
result_set = result_proxy.fetchall()

print(result_set)

connection.close()
