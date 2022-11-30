CREATE SCHEMA IF NOT EXISTS foo;

CREATE TABLE IF NOT EXISTS foo.bar
(
    foo INT PRIMARY KEY,
    bar TEXT
);

CREATE TABLE IF NOT EXISTS foo.foo
(
    id  INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    bar TEXT,
    foo INT
);
