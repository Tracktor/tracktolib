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

CREATE TABLE IF NOT EXISTS foo.baz
(
    id  INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    bar JSON,
    baz JSONB
);

CREATE TABLE IF NOT EXISTS foo.generated
(
    id        INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
    bar       TEXT NOT NULL,
    bar_lower TEXT GENERATED ALWAYS AS (LOWER(bar)) STORED,
    CONSTRAINT bar_unique UNIQUE (bar_lower)
);
