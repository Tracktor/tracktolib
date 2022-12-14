import pytest
from fastapi import FastAPI, APIRouter
from fastapi.testclient import TestClient
from pydantic import BaseModel
from starlette import status
from dataclasses import dataclass


@pytest.fixture()
def app():
    from tracktolib.api import JSONSerialResponse
    return FastAPI(
        openapi_tags=[{'name': 'foo', 'description': 'bar'}],
        default_response_class=JSONSerialResponse
    )


def test_get_return_type():
    from tracktolib.api import _get_return_type, Response
    class ReturnBar(BaseModel):
        foo: int

    async def bar_endpoint() -> Response[ReturnBar | None]: ...

    assert _get_return_type(bar_endpoint) == ReturnBar | None


def test_add_endpoint(app):
    import fastapi
    from tracktolib.api import add_endpoint, Response, Endpoint, Depends
    from tracktolib.tests import assert_equals

    endpoint = Endpoint()
    endpoint2 = Endpoint()
    endpoint3 = Endpoint()

    def compute_sum():
        return 1 + 1

    class ReturnFoo(BaseModel):
        foo: int

    class ReturnBar(BaseModel):
        foo: int

    class ReturnFooBar(BaseModel):
        foo: int
        bar: str

    @endpoint.get()
    async def foo_endpoint(
            foo: int = Depends(compute_sum)
    ) -> Response[ReturnFoo]:
        return {'foo': foo}

    depends_called = False

    def foo_depends():
        nonlocal depends_called
        depends_called = True

    @endpoint.post(dependencies=[fastapi.Depends(foo_depends)])
    async def foo2_endpoint() -> Response[list[ReturnFoo]]:
        return [{'foo': 1}]

    @endpoint2.get(status_code=status.HTTP_202_ACCEPTED)
    async def bar_endpoint(return_empty: bool = False) -> Response[ReturnBar | None]:
        return {'foo': 1} if not return_empty else None

    @endpoint3.get(path='foo/{foo}/bar/{bar}',
                   model=list[ReturnFooBar])
    async def path_endpoint(foo: int,
                            bar: str):
        return [{'foo': foo, 'bar': bar}]

    router = APIRouter()

    add_endpoint('/foo', router, endpoint)
    add_endpoint('/bar', router, endpoint2)
    add_endpoint('/path_endpoint', router, endpoint3)

    app.include_router(router)

    with TestClient(app) as client:
        resp = client.get('/foo')
        resp2 = client.post('/foo')
        resp3 = client.get('/bar')
        resp4 = client.get('/bar', params={'return_empty': True})
        resp5 = client.get('/path_endpoint/foo/2/bar/baz')

        doc_resp = client.get('/openapi.json')

    assert_equals(resp.json(), {'foo': 2})
    assert_equals(resp2.json(), [{'foo': 1}])
    assert resp3.json() == {'foo': 1}
    assert resp3.status_code == status.HTTP_202_ACCEPTED
    assert resp4.json() is None
    assert resp5.json() == [{'foo': 2, 'bar': 'baz'}]
    assert doc_resp.status_code == status.HTTP_200_OK

    assert depends_called


def check_json_serial_types():
    from tracktolib.api import JSONSerialResponse

    @dataclass
    class Bar:
        foo: int = 1

    def _json_serial(obj):
        if isinstance(obj, Bar):
            return str(obj.foo)
        raise TypeError(f'Object of type {type(obj)} is not JSON serializable')

    class Foo(JSONSerialResponse):
        json_serial = _json_serial
