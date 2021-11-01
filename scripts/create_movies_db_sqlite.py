import sqlite3
import os

DATABASE_PATH = 'data/movies.db'

try:
    print('Removing previous MOVIES database')
    os.remove(DATABASE_PATH)
    print('Done!')
except FileNotFoundError:
    print('Not Found!')
    pass

print()
connection = sqlite3.connect('data/movies.db')
cursor = connection.cursor()
cursor.execute('''
CREATE TABLE IF NOT EXISTS movies(
    title text,
    director text,
    year int
)
''')

famous_films = [
    ('Pulp Fiction', 'Quentin Tarantino', 1994),
    ('Back to the Future', 'Steven Spielberg', 1985),
    ('Moonrise Kingdom', 'Wes Anderson', 2012)
]

print('Inserting famous movies into the database...')
cursor.executemany('INSERT INTO movies VALUES (?,?,?)', famous_films)
connection.commit()

print('Fetching all values from movies table...')
records = cursor.execute("SELECT * FROM movies;")

print()
# print(cursor.fetchall(), '\n')

# Or iterate over the cursor execute result
for record in records:
    print(record)

print()

release_year = 1985
print(f'Fetching movies in year {release_year}...')
cursor.execute('SELECT * FROM movies WHERE year=?', (release_year,))
print(cursor.fetchall())
connection.close()