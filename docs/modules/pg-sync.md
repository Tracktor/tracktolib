---
title: "PostgreSQL (Sync)"
---

# PostgreSQL (Sync)

Sync PostgreSQL helpers using [psycopg](https://www.psycopg.org/psycopg3/) (v3).

## Installation

```bash
uv add tracktolib[pg-sync]
```

## Dependencies

- [psycopg](https://www.psycopg.org/psycopg3/) (v3)

## Quick Start

```python
from psycopg import connect
from tracktolib.pg_sync import insert_many, fetch_one, fetch_all, fetch_count

conn = connect('postgresql://user:pass@localhost/db')

# Insert data
data = [
    {'name': 'Alice', 'value': 1},
    {'name': 'Bob', 'value': 2}
]
insert_many(conn, 'public.users', data)

# Fetch single row
user = fetch_one(conn, 'SELECT * FROM users WHERE id = %s', 1, required=True)

# Fetch all rows
users = fetch_all(conn, 'SELECT * FROM users ORDER BY id')

# Count rows
count = fetch_count(conn, 'public.users')
```

## Fetch Functions

### `fetch_all`

Fetch all rows from a query as a list of dictionaries.

```python
from tracktolib.pg_sync import fetch_all

# Simple query
users = fetch_all(conn, 'SELECT * FROM users')

# With parameters
active_users = fetch_all(
    conn,
    'SELECT * FROM users WHERE status = %s ORDER BY name',
    'active'
)
```

### `fetch_one`

Fetch a single row from a query.

```python
from tracktolib.pg_sync import fetch_one

# Optional result (returns None if not found)
user = fetch_one(conn, 'SELECT * FROM users WHERE id = %s', 42)

# Required result (raises ValueError if not found)
user = fetch_one(conn, 'SELECT * FROM users WHERE id = %s', 42, required=True)
```

### `fetch_count`

Count rows in a table with optional WHERE clause.

```python
from tracktolib.pg_sync import fetch_count

# Count all rows
total = fetch_count(conn, 'public.users')

# Count with condition
active_count = fetch_count(conn, 'public.users', 'active', where='status = %s')
```

## Insert Functions

### `insert_many`

Insert multiple rows into a table.

```python
from tracktolib.pg_sync import insert_many

data = [
    {'name': 'Alice', 'email': 'alice@example.com'},
    {'name': 'Bob', 'email': 'bob@example.com'},
]
insert_many(conn, 'public.users', data)
```

The function automatically:

- Extracts column names from the first dictionary
- Converts dict values to JSON when needed
- Uses `executemany` for efficient batch inserts

### `insert_one`

Insert a single row with optional RETURNING clause.

```python
from tracktolib.pg_sync import insert_one

# Simple insert
insert_one(conn, 'public.users', {'name': 'Charlie', 'email': 'charlie@example.com'})

# Insert with returning
result = insert_one(
    conn,
    'public.users',
    {'name': 'Charlie'},
    returning=['id', 'created_at']
)
print(result['id'])
```

### `insert_csv`

Bulk insert from a CSV file using PostgreSQL's COPY command.

```python
from pathlib import Path
from tracktolib.pg_sync import insert_csv

with conn.cursor() as cur:
    insert_csv(
        cur,
        schema='public',
        table='users',
        csv_path=Path('users.csv'),
        exclude_columns=['internal_id'],  # Columns to skip
        delimiter=',',
        on_conflict='ON CONFLICT DO NOTHING'
    )
conn.commit()
```

## Table Management

### `clean_tables`

Truncate multiple tables with options for identity reset and cascading.

```python
from tracktolib.pg_sync import clean_tables

# Truncate tables and reset sequences
clean_tables(conn, ['public.orders', 'public.order_items'])

# Without resetting sequences
clean_tables(conn, ['public.logs'], reset_seq=False)

# Without cascading
clean_tables(conn, ['public.users'], cascade=False)
```

### `get_tables`

Get all table names in specified schemas.

```python
from tracktolib.pg_sync import get_tables

# Get all tables in public schema
tables = get_tables(conn, ['public'])

# Exclude certain tables
tables = get_tables(
    conn,
    ['public', 'app'],
    ignored_tables=['public.migrations', 'public.schema_version']
)
```

### `drop_db`

Drop a database (ignores error if database doesn't exist).

```python
from tracktolib.pg_sync import drop_db

drop_db(conn, 'test_database')
```

## Sequence Management

### `set_seq_max`

Set a sequence to the maximum value in a table (useful after bulk inserts).

```python
from tracktolib.pg_sync import set_seq_max

# After inserting data with explicit IDs
set_seq_max(conn, 'users_id_seq', 'public.users')
```

## Helper Functions

### `get_insert_data`

Generate INSERT query and values from data.

```python
from tracktolib.pg_sync import get_insert_data

data = [{'name': 'Alice', 'value': 1}]
query, values = get_insert_data('public.users', data)
# query: "INSERT INTO public.users as t (name,value) VALUES (%s,%s)"
# values: [('Alice', 1)]
```

## Type Handling

The module automatically handles:

- **Dictionaries**: Converted to `Json` type for JSONB columns
- **None values**: Passed through as NULL
- **All other types**: Passed through unchanged
