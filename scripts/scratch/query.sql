
DROP DATABASE university;
CREATE DATABASE university;

USE university;

CREATE TABLE students(
    id INT PRIMARY KEY,
    name VARCHAR(20) NOT NULL,
    roll_no CHAR(5) NOT NULL,
    year INT NOT NULL
);

INSERT INTO students VALUES
(1, 'Abhishek', 'Y4001', 4),
(2, 'Anurima', 'Y3012', 3);