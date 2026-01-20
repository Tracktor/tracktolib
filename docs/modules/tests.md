---
title: "Tests"
---

# Tests

Testing utilities using [deepdiff](https://github.com/seperman/deepdiff).

## Installation

```bash
uv add tracktolib[tests]
```

## Dependencies

- [deepdiff](https://github.com/seperman/deepdiff)

## Functions

### `assert_equals`

Deep comparison assertion for dictionaries and iterables with detailed diff output.

```python
from tracktolib.tests import assert_equals

# Compare dictionaries
expected = {'name': 'John', 'age': 30, 'tags': ['admin', 'user']}
actual = {'name': 'John', 'age': 30, 'tags': ['admin', 'user']}
assert_equals(expected, actual)

# Compare lists
expected = [{'id': 1}, {'id': 2}]
actual = [{'id': 1}, {'id': 2}]
assert_equals(expected, actual)

# Ignore list order
assert_equals(expected, actual, ignore_order=True)
```

## Examples

### Basic Usage

```python
from tracktolib.tests import assert_equals

def test_user_creation():
    user = create_user(name='John', email='john@example.com')

    assert_equals(
        {'name': 'John', 'email': 'john@example.com'},
        {'name': user.name, 'email': user.email}
    )
```

### Ignoring Order

```python
from tracktolib.tests import assert_equals

def test_tags():
    expected_tags = ['admin', 'user', 'moderator']
    actual_tags = ['user', 'admin', 'moderator']

    # This would fail without ignore_order
    # assert_equals(expected_tags, actual_tags)

    # This passes
    assert_equals(expected_tags, actual_tags, ignore_order=True)
```

### Nested Structures

```python
from tracktolib.tests import assert_equals

def test_nested_data():
    expected = {
        'user': {
            'profile': {
                'name': 'John',
                'settings': {'theme': 'dark', 'notifications': True}
            }
        }
    }

    actual = get_user_data()
    assert_equals(expected, actual)
```

### List of Dictionaries

```python
from tracktolib.tests import assert_equals

def test_api_response():
    expected = [
        {'id': 1, 'name': 'Item 1'},
        {'id': 2, 'name': 'Item 2'},
    ]

    response = client.get('/items')
    assert_equals(expected, response.json())
```

## Error Output

When assertion fails, `assert_equals` provides detailed diff information:

```python
from tracktolib.tests import assert_equals

expected = {'name': 'John', 'age': 30}
actual = {'name': 'Jane', 'age': 25}

assert_equals(expected, actual)
# AssertionError with output:
# {'values_changed': {"root['name']": {'new_value': 'Jane', 'old_value': 'John'},
#                     "root['age']": {'new_value': 25, 'old_value': 30}}}
```

## Integration with pytest

```python
import pytest
from tracktolib.tests import assert_equals

class TestUserAPI:
    def test_get_user(self, client):
        response = client.get('/users/1')

        assert_equals(
            {'id': 1, 'name': 'John', 'email': 'john@example.com'},
            response.json()
        )

    def test_list_users(self, client):
        response = client.get('/users')

        assert_equals(
            [{'id': 1, 'name': 'John'}, {'id': 2, 'name': 'Jane'}],
            response.json(),
            ignore_order=True
        )
```

## Why Use `assert_equals`?

1. **Detailed Diffs**: Shows exactly what differs between expected and actual values
2. **Deep Comparison**: Handles nested structures automatically
3. **Order Control**: Option to ignore order in lists and sets
4. **Pretty Output**: Uses `pprint` for readable error messages
