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
    endpoint2 = Endpoint()

    def compute_sum():
        return 1 + 1

    class ReturnFoo(BaseModel):
        foo: int

    class ReturnBar(BaseModel):
        foo: int

    @endpoint.get()
    async def foo_endpoint(
            foo: int = Depends(compute_sum)
    ) -> ReturnType[ReturnFoo]:
        return {'foo': foo}

    @endpoint2.get()
    async def bar_endpoint() -> ReturnType[ReturnBar | None]:
        return None

    add_endpoint('/foo', router, endpoint)
    add_endpoint('/bar', router, endpoint2)

    with TestClient(router) as client:
        resp = client.get('/foo')
        resp2 = client.get('/bar')
    assert_equals(resp.json(), {'foo': 2})
    assert resp2.json() is None
