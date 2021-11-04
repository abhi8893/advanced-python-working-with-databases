from sqlalchemy import create_engine, func as sqlfunc
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, Text, Column
from sqlalchemy.sql.schema import ForeignKey

DATABASE = 'books'
engine = create_engine(f'postgresql://postgres:@localhost:5432/{DATABASE}')
Base = declarative_base(bind=engine)
Base.metadata.reflect(engine)
Base.metadata.clear() # Refer: https://stackoverflow.com/questions/37908767/table-roles-users-is-already-defined-for-this-metadata-instance


def add_filler():
    print('-'*10)

class AutoRepr:

    def __repr__(self):
        cls = self.__class__.__name__
        cols = self.__table__.columns.keys()
        attr_vals_str = ', '.join([f'{c}:{repr(self.__getattribute__(c))}' for c in cols])
        return f'{cls}({attr_vals_str})'


class Book(Base, AutoRepr):
    __tablename__ = 'books'

    id = Column(Integer, primary_key=True)
    title = Column(Text)
    num_pages = Column(Integer)


class Author(Base, AutoRepr):
    __tablename__ = 'authors'

    id = Column(Integer, primary_key=True)
    first_name = Column(Text)
    last_name = Column(Text, nullable=True)


class AuthorBook(Base, AutoRepr):
    __tablename__ = 'author_books'

    id = Column(Integer, primary_key=True)
    author_id = Column(Integer, ForeignKey('authors.id'))
    book_id = Column(Integer, ForeignKey('books.id'))


    author = relationship('Author')
    book = relationship('Book')

def load_session():
    Session = sessionmaker()
    session = Session()
    return session

def insert_data(title, num_pages, author_name):

    title = title.title()

    spltd = author_name.split(' ')
    if len(spltd) == 1:
        spltd[0] = spltd[0].title()
        spltd.append(None)
    else:
        spltd = [n.title() for n in spltd]


    author = Author(first_name=spltd[0], last_name=spltd[1])
    book = Book(title=title, num_pages=num_pages)

    session = load_session()

    existing_book = session.query(Book).filter(
        Book.title == title, Book.num_pages == num_pages).first()

    existing_author = session.query(Author).filter(
        Author.first_name == author.first_name, 
        Author.last_name == author.last_name).first() # NOTE: Even for NULL/None comparison use ==

    if (existing_book is not None) and (existing_book is not None):
        return None

    try:
        if existing_author is None:
            session.add(author)
            session.flush()
            author_id = author.id
        else:
            author_id = existing_author.id
        
        if existing_book is None:
            session.add(book)
            session.flush()
            book_id = book.id
        else:
            book_id = existing_book.id

        author_book = AuthorBook(author_id=author_id, book_id=book_id)
        session.add(author_book)
        session.commit()

    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
    

Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)


if __name__ == '__main__':
    print('Inserting data...'); add_filler();
    insert_data(title='Harry Potter 1', num_pages=500, author_name='JK Rowling')
    insert_data(title='Harry Potter 2', num_pages=600, author_name='jk rowling')
    insert_data(title='Harry Potter 3', num_pages=400, author_name='JK ROWLING')
    insert_data(title='Tintin 1', num_pages=400, author_name='Herge')
    insert_data(title='tintin 2', num_pages=400, author_name='herge')




    