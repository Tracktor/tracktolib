import pytest
from pydantic import BaseModel
from fastapi import APIRouter
from fastapi.testclient import TestClient


@pytest.fixture()
def router():
    return APIRouter()


def test_add_endpoint(router):
    from tracktolib.api import add_endpoint, Depends, ReturnType, Endpoint
    from tracktolib.tests import assert_equals
    endpoint = Endpoint()

    def compute_sum():
        return 1 + 1

    class ReturnFoo(BaseModel):
        foo: int

    @endpoint.get()
    async def foo_endpoint(
            foo: int = Depends(compute_sum)
    ) -> ReturnType[ReturnFoo]:
        return {'foo': foo}

    add_endpoint('/foo', router, endpoint)

    with TestClient(router) as client:
        resp = client.get('/foo')
    assert_equals(resp.json(), {'foo': 2})
