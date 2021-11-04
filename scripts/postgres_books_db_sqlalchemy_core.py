from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy import create_engine

DATABASE = 'books'
engine = create_engine(f'postgresql://postgres:@localhost:5432/{DATABASE}')
meta = MetaData()

# Create books table
books = Table(
    'books',
    meta,
    Column('id', Integer, primary_key=True),
    Column('title', String),
    Column('num_pages', Integer)
)

# Create authors table
authors = Table(
    'authors',
    meta,
    Column('id', Integer, primary_key=True),
    Column('first_name', String),
    Column('last_name', String)
)

# Create author_books table
author_books = Table(
    'author_books',
    meta,
    Column('id', Integer, primary_key=True),
    Column('author_id', Integer, ForeignKey('authors.id')),
    Column('book_id', Integer, ForeignKey('books.id'))
)


def add_filler():
    print('-'*10)

def insert_data(conn, title, num_pages, author_name):

    title = title.title()
    spltd = author_name.split(' ')
    if len(spltd) == 1:
        spltd[0] = spltd[0].title()
        spltd.append(None)
    else:
        spltd = [n.title() for n in spltd]


    existing_book = conn.execute(
        books.select().where(
            (books.c.title == title) &
            (books.c.num_pages == num_pages)
        )
    )

    existing_author = conn.execute(
        authors.select().where(
            (authors.c.first_name == spltd[0]) &
            (authors.c.last_name == spltd[1])
        )
    )

    existing_book_res = existing_book.fetchone()
    existing_author_res = existing_author.fetchone()

    if (existing_author_res is not None) and (existing_book_res is not None):
        return None

    if existing_book_res is None:
        res = conn.execute(books.insert().values(title=title, num_pages=num_pages))
        book_id = res.inserted_primary_key[0]
    else:
        book_id = dict(zip(existing_book.keys(), existing_book_res))['id']


    if existing_author_res is None:
        res = conn.execute(authors.insert().values(first_name=spltd[0], last_name=spltd[1]))
        author_id = res.inserted_primary_key[0]
    else:
        author_id = dict(zip(existing_author.keys(), existing_author_res))['id']


    conn.execute(author_books.insert().values(book_id=book_id, author_id=author_id))


if __name__ == '__main__':
    meta.drop_all(engine)
    meta.create_all(engine)


    with engine.connect() as conn:
        print('Inserting data...'); add_filler();
        insert_data(conn, title='Harry Potter 1', num_pages=500, author_name='JK Rowling')
        insert_data(conn, title='Harry Potter 2', num_pages=600, author_name='jk rowling')
        insert_data(conn, title='Harry Potter 3', num_pages=400, author_name='JK ROWLING')
        insert_data(conn, title='Tintin 1', num_pages=400, author_name='Herge')
        insert_data(conn, title='tintin 2', num_pages=400, author_name='herge')











