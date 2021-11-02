from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base

DATABASE = 'projects'
engine = create_engine(f"mysql+mysqlconnector://root:@localhost:3306/{DATABASE}", 
                       echo=True)

Base = declarative_base()


class Project(Base):
    __tablename__ = 'projects'
    __table_args__ = {'schema': DATABASE}

    project_id = Column(Integer(), primary_key=True)
    title = Column(String(length=50))
    description = Column(String(length=255))

    def __repr__(self):
        return f'Project(title={self.title}), description={self.description})'


class Task(Base):
    __tablename__ = 'tasks'
    __table_args__ = {'schema': DATABASE}

    task_id = Column(Integer(), primary_key=True)
    project_id = Column(Integer(), ForeignKey(f'{DATABASE}.projects.project_id'))
    description = Column(String(length=255))

    project = relationship("Project")

    def __repr__(self):
        return f'Task(description={self.description})'

Base.metadata.drop_all(bind=engine)
Base.metadata.create_all(engine)

session_maker = sessionmaker()
session_maker.configure(bind=engine)
session = session_maker()

project_details = {
    'MLOps Specialization': 'Complete deeplearning.ai MLOps specialization',
    'Learn Kedro': 'Learn to build ML pipelines in kedro'}

project_tasks = {
    'MLOps Specialization': ('Make an outline of the specialization for each course',
                             'Complete Week 1 and 2 of Course 1: Intro to MLOps'),
    'Learn Kedro': ('Watch the intro video by DataEngineeringOne on Kedro',
                    'Complete kedro github tutorial',
                    'Explore community projects using Kedro')
}

for project_title in project_details:

    project_description = project_details[project_title]
    project = Project(title=project_title, description=project_description)
    session.add(project)
    session.commit()

    tasks = [Task(project_id=project.project_id, description=d) for d in project_tasks[project_title]]
    session.bulk_save_objects(tasks)
    session.commit()


our_project = session.query(Project).filter_by(title='MLOps Specialization').first()
print(our_project)

all_tasks = session.query(Task).all()
print(all_tasks)