import sqlalchemy as db
import os

DATABASE_PATH = 'data/users.db'
try:
    os.remove(DATABASE_PATH)
except FileNotFoundError:
    pass

engine = db.create_engine(f'sqlite:///{DATABASE_PATH}')
connection = engine.connect()