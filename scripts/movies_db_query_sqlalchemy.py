import sqlalchemy as db
from pathlib import Path

DATABASE_PATH = Path(__file__).parents[1].absolute().joinpath('data/movies.db')

engine = db.create_engine(f'sqlite:///{DATABASE_PATH}')
connection = engine.connect()

metadata = db.MetaData()
movies = db.Table('movies', metadata, autoload=True, autoload_with=engine)

print('Selecting all records from movies table...')
query = db.select(movies)
result_proxy = connection.execute(query)
result_set = result_proxy.fetchall()
print(result_set, '\n')

director = 'Steven Spielberg'
print(f'Selecting records where director is {director}...')
query = db.select(movies).where(movies.c.director == director)
result_proxy = connection.execute(query)
result_set = result_proxy.fetchall()
print(result_set, '\n')

print('Inserting a Psycho movie into the movies table...')
query = movies.insert().values(title='Psycho',
                               director='Alfred Hitchcock',
                               year=1960)
connection.execute(query)
print('Done!', '\n')

print('Selecting all records from movies table again...')
query = db.select(movies)
result_proxy = connection.execute(query)
result_set = result_proxy.fetchall()
print(result_set, '\n')


print('Deleting previously inserted record...')
query = db.delete(movies).where(movies.c.title == 'Psycho')
connection.execute(query)
print('Done!', '\n')

print('Selecting all records from movies table once again...')
query = db.select(movies)
result_proxy = connection.execute(query)
result_set = result_proxy.fetchall()
print(result_set, '\n')

connection.close()