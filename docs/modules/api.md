---
title: "API (FastAPI)"
---

# API (FastAPI)

FastAPI utilities for building REST APIs with type-safe endpoints.

## Installation

```bash
uv add tracktolib[api]
```

## Dependencies

- [fastapi](https://fastapi.tiangolo.com/)
- [pydantic](https://docs.pydantic.dev/)

## Quick Start

```python
from fastapi import APIRouter
from pydantic import BaseModel
from tracktolib.api import Endpoint, add_endpoint, Depends

router = APIRouter()


class User(BaseModel):
    id: int
    name: str


users_endpoint = Endpoint()


@users_endpoint.get()
async def get_users() -> list[User]:
    """Get all users."""
    return [User(id=1, name='John')]


@users_endpoint.post(status_code=201)
async def create_user(user: User) -> User:
    """Create a new user."""
    return user


add_endpoint('/users', router, users_endpoint)
```

## Endpoint Class

The `Endpoint` class provides a clean way to define HTTP methods for a route.

### Available Methods

```python
from tracktolib.api import Endpoint

endpoint = Endpoint()


@endpoint.get()
async def get_item() -> Item: ...


@endpoint.post(status_code=201)
async def create_item(item: Item) -> Item: ...


@endpoint.put()
async def update_item(id: int, item: Item) -> Item: ...


@endpoint.patch()
async def partial_update(id: int, data: dict) -> Item: ...


@endpoint.delete(status_code=204)
async def delete_item(id: int) -> None: ...
```

### Path Extensions

```python
endpoint = Endpoint()


@endpoint.get()
async def list_items() -> list[Item]: ...


@endpoint.get(path='{item_id}')
async def get_item(item_id: int) -> Item: ...

# Results in:
# GET /items -> list_items
# GET /items/{item_id} -> get_item
```

## Type-Safe Dependencies

The `Depends` function provides type-safe dependency injection.

```python
from tracktolib.api import Depends


async def get_db() -> Database:
    return Database()


async def get_current_user(db: Database = Depends(get_db)) -> User:
    return User(id=1, name='John')


@endpoint.get()
async def get_profile(user: User = Depends(get_current_user)) -> User:
    """Get current user profile."""
    return user
```

## Adding Endpoints to Router

```python
from fastapi import APIRouter
from tracktolib.api import add_endpoint, Endpoint

router = APIRouter()

users = Endpoint()
# ... define methods

add_endpoint(
    path='/users',
    router=router,
    endpoint=users,
    dependencies=[Depends(verify_token)]  # Applied to all methods
)
```

## Response Utilities

### `JSONSerialResponse`

Custom JSON response with extended serialization support.

```python
from fastapi import FastAPI
from tracktolib.api import JSONSerialResponse
from datetime import datetime
from decimal import Decimal

app = FastAPI(
    title='My API',
    default_response_class=JSONSerialResponse,
)


# All routes now automatically handle:
# - datetime objects
# - Decimal
# - UUID
# - Custom objects with __json__ method

@app.get('/data')
async def get_data():
    return {
        'timestamp': datetime.now(),
        'amount': Decimal('99.99')
    }
```

### `check_status`

Assert response status in tests, otherwise raise AssertionError with
the response json content.

```python
from tracktolib.api import check_status

response = client.get('/users')
check_status(response)  # Asserts 200

response = client.post('/users', json={...})
check_status(response, status=201)
```

## Pydantic Utilities

### `CamelCaseModel`

Base model that converts field names to camelCase in JSON.

```python
from tracktolib.api import CamelCaseModel


class UserResponse(CamelCaseModel):
    user_id: int
    first_name: str
    created_at: datetime

# JSON output:
# {"userId": 1, "firstName": "John", "createdAt": "..."}
```

## OpenAPI Enhancements

### List Response Names

Automatically generates proper names for list responses in OpenAPI.

```python
@endpoint.get(model=list[User])
async def get_users() -> list[User]:
    """Get all users."""
    ...

# OpenAPI schema will show "Array[User]" instead of generic "List"
```

## Complete Example

```python
from fastapi import FastAPI, APIRouter
from pydantic import BaseModel
from tracktolib.api import Endpoint, add_endpoint, Depends, CamelCaseModel

app = FastAPI()
router = APIRouter(prefix='/api/v1')


# Models
class UserCreate(CamelCaseModel):
    first_name: str
    email: str


class UserResponse(CamelCaseModel):
    user_id: int
    first_name: str
    email: str


# Dependencies
async def get_db():
    yield database


# Endpoints
users = Endpoint()


@users.get()
async def list_users(db=Depends(get_db)) -> list[UserResponse]:
    """List all users."""
    return await db.fetch_all_users()


@users.get(path='{user_id}')
async def get_user(user_id: int, db=Depends(get_db)) -> UserResponse:
    """Get user by ID."""
    return await db.fetch_user(user_id)


@users.post(status_code=201)
async def create_user(user: UserCreate, db=Depends(get_db)) -> UserResponse:
    """Create a new user."""
    return await db.create_user(user)


@users.delete(path='{user_id}', status_code=204)
async def delete_user(user_id: int, db=Depends(get_db)) -> None:
    """Delete a user."""
    await db.delete_user(user_id)


# Register
add_endpoint('/users', router, users)
app.include_router(router)
```
