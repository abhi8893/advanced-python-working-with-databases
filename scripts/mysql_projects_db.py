import mysql.connector as mysql


def connect(db_name):
    try:
        connection = mysql.connect(
            host='localhost',
            user='root',
            password='',
            database=db_name
        )
    except Exception as e:
        print(e)

    return connection


def add_new_project(cursor, project_title,
                    project_description, tasks_description):

    project_data = (project_title, project_description)
    cursor.execute('INSERT INTO projects(title, description) VALUES(%s,%s)',
                   project_data)
    project_id = cursor.lastrowid

    tasks_data = [(project_id, desc) for desc in tasks_description]
    cursor.executemany('INSERT INTO tasks(project_id, description) VALUES(%s,%s)', tasks_data)


if __name__ == '__main__':
    connection = connect('projects')
    cursor = connection.cursor()

    cursor.execute('DROP TABLE IF EXISTS tasks;')
    connection.commit()
    cursor.execute('DROP TABLE IF EXISTS projects;')
    connection.commit()

    cursor.execute('''
    CREATE TABLE projects(
        project_id INT(11) NOT NULL AUTO_INCREMENT,
        title VARCHAR(30),
        description VARCHAR(255),
        PRIMARY KEY(project_id)
    );
    ''')

    cursor.execute('''
    CREATE TABLE tasks(
        task_id INT(11) NOT NULL AUTO_INCREMENT,
        project_id int(11) NOT NULL,
        description VARCHAR(255),
        PRIMARY KEY(task_id),
        FOREIGN KEY(project_id) REFERENCES projects(project_id)
    );
    ''')

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

    for project_title in project_tasks:
        project_description = project_details[project_title]
        tasks_description = project_tasks[project_title]
        add_new_project(cursor, project_title,
                        project_description, tasks_description)

    cursor.execute('SELECT * FROM projects;')
    print(cursor.fetchall(), '\n')

    cursor.execute('SELECT * FROM tasks;')
    print(cursor.fetchall(), '\n')

    connection.commit()
    connection.close()
