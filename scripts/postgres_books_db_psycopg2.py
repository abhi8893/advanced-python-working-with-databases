import psycopg2

DATABASE = 'books'

def _get_book_author_dict_from_data(title, num_pages, author_name):
    book_data = dict()
    book_data['title'] = title.title()
    book_data['num_pages'] = num_pages

    author_data = dict()
    spltd = author_name.split(' ')
    if len(spltd) == 1:
        spltd[0] = spltd[0].title()
        spltd.append(None)
    else:
        spltd = [n.title() for n in spltd]

    author_data['first_name'], author_data['last_name'] = spltd

    return book_data, author_data


def insert_data(conn, title, num_pages, author_name):

    cur = conn.cursor()
    book_data, author_data = _get_book_author_dict_from_data(title, num_pages, author_name)

    cur.execute('''
    INSERT INTO books (title, num_pages)
    SELECT %s, %s
    WHERE NOT EXISTS (
        SELECT id from books WHERE title = %s AND num_pages = %s
    );
    SELECT id from books WHERE title = %s AND num_pages = %s''', tuple(book_data.values())*3)
    conn.commit()

    book_id = cur.fetchone()[0]

    if author_data['last_name'] is None:
        op = 'IS'
    else:
        op = '='
    cur.execute('''
    INSERT INTO authors (first_name, last_name)
    SELECT %s, %s
    WHERE NOT EXISTS (
        SELECT id from authors WHERE first_name = %s AND last_name {} %s
    );
    SELECT id from authors WHERE first_name = %s AND last_name {} %s'''.format(op, op), tuple(author_data.values())*3)
    conn.commit()

    author_id = cur.fetchone()[0]

    cur.execute('''
    INSERT INTO author_books (author_id, book_id)
    VALUES (%s, %s)''', (author_id, book_id))
    conn.commit()

def add_filler():
    print('-'*10)

if __name__ == '__main__':

    conn = psycopg2.connect(
        user='postgres',
        password='',
        host='localhost',
        database=DATABASE
    )

    cur = conn.cursor()

    # Drop tables
    print('Dropping existing tables...'); add_filler();
    cur.execute('DROP TABLE IF EXISTS author_books;')
    cur.execute('DROP TABLE IF EXISTS authors;')
    cur.execute('DROP TABLE IF EXISTS books;')
    conn.commit()

    # Create authors table
    print('Creating authors table...'); add_filler();
    cur.execute('''
    CREATE TABLE authors(
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(20) NOT NULL,
        last_name VARCHAR(20)
    );
    ''')
    conn.commit()

    # Create books table
    print('Creating books table...'); add_filler();
    cur.execute('''
    CREATE TABLE books(
        id SERIAL PRIMARY KEY,
        title VARCHAR(40) NOT NULL,
        num_pages INT NOT NULL
    );
    ''')
    conn.commit()

    # Create author_books table
    print('Creating author_books table...'); add_filler();
    cur.execute('''
    CREATE TABLE author_books(
        id SERIAL PRIMARY KEY,
        author_id INT NOT NULL,
        book_id INT NOT NULL,
        FOREIGN KEY (author_id) REFERENCES authors(id),
        FOREIGN KEY (book_id) REFERENCES books(id)
    );
    ''')
    conn.commit()


    # Insert data
    print('Inserting data...'); add_filler();
    insert_data(conn, title='Harry Potter 1', num_pages=500, author_name='JK Rowling')
    insert_data(conn, title='Harry Potter 2', num_pages=600, author_name='jk rowling')
    insert_data(conn, title='Harry Potter 3', num_pages=400, author_name='JK ROWLING')
    insert_data(conn, title='Tintin 1', num_pages=400, author_name='Herge')
    insert_data(conn, title='tintin 2', num_pages=400, author_name='herge')


    conn.close()