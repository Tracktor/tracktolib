from dataclasses import dataclass

import pytest
from fastapi import FastAPI, APIRouter
from fastapi.testclient import TestClient
from pydantic import BaseModel
from starlette import status


@pytest.fixture()
def app():
    from tracktolib.api import JSONSerialResponse

    return FastAPI(
        openapi_tags=[{"name": "foo", "description": "bar"}],
        default_response_class=JSONSerialResponse,
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
    async def foo_endpoint(foo: int = Depends(compute_sum)) -> Response[ReturnFoo]:
        """Get route"""
        return {"foo": foo}

    depends_called = False

    def foo_depends():
        nonlocal depends_called
        depends_called = True

    @endpoint.post(dependencies=[fastapi.Depends(foo_depends)])
    async def foo2_endpoint() -> Response[list[ReturnFoo]]:
        """Post route"""
        return [{"foo": 1}]

    @endpoint2.get(status_code=status.HTTP_202_ACCEPTED)
    async def bar_endpoint(return_empty: bool = False) -> Response[ReturnBar | None]:
        """Get route"""
        return {"foo": 1} if not return_empty else None

    @endpoint3.get(path="foo/{foo}/bar/{bar}", model=list[ReturnFooBar])
    async def path_endpoint(foo: int, bar: str):
        """Get route"""
        return [{"foo": foo, "bar": bar}]

    router = APIRouter()

    add_endpoint("/foo", router, endpoint)
    add_endpoint("/bar", router, endpoint2)
    add_endpoint("/path_endpoint", router, endpoint3)

    app.include_router(router)

    with TestClient(app) as client:
        resp = client.get("/foo")
        resp2 = client.post("/foo")
        resp3 = client.get("/bar")
        resp4 = client.get("/bar", params={"return_empty": True})
        resp5 = client.get("/path_endpoint/foo/2/bar/baz")

        doc_resp = client.get("/openapi.json")

    assert_equals(resp.json(), {"foo": 2})
    assert_equals(resp2.json(), [{"foo": 1}])
    assert resp3.json() == {"foo": 1}
    assert resp3.status_code == status.HTTP_202_ACCEPTED
    assert resp4.json() is None
    assert resp5.json() == [{"foo": 2, "bar": "baz"}]
    assert doc_resp.status_code == status.HTTP_200_OK

    assert depends_called


def test_camelcase_model(app):
    from tracktolib.api import add_endpoint, Endpoint, CamelCaseModel
    from tracktolib.tests import assert_equals

    endpoint = Endpoint()

    class InputModel(CamelCaseModel):
        foo_bar: int

    class OutputModel(CamelCaseModel):
        foo_bar: str

    @endpoint.post(model=OutputModel)
    async def foo_endpoint(data: InputModel):
        """Post route"""
        return {"foo_bar": data.foo_bar}

    router = APIRouter()

    add_endpoint("/foo", router, endpoint)
    app.include_router(router)

    with TestClient(app) as client:
        resp = client.post("/foo", json={"foo_bar": 1})

    assert_equals(resp.json(), {"fooBar": "1"})


def check_json_serial_types():
    from tracktolib.api import JSONSerialResponse

    @dataclass
    class Bar:
        foo: int = 1

    def _json_serial(obj):
        if isinstance(obj, Bar):
            return str(obj.foo)
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    class Foo(JSONSerialResponse):
        json_serial = _json_serial


def test_update_array_metadata(app):
    from fastapi.openapi.utils import get_openapi
    from tracktolib.api import Endpoint, CamelCaseModel, add_endpoint
    from tracktolib.tests import assert_equals

    first_endpoint = Endpoint()
    second_endpoint = Endpoint()

    class Foo(CamelCaseModel):
        foo_int: int
        model_config = {"json_schema_extra": {"example": {"foo_int": 1}}}

    class Bar(CamelCaseModel):
        bar_int: int

        model_config = {"json_schema_extra": {"example": {"bar_int": 2}}}

    @first_endpoint.get(model=Foo)
    async def foo_endpoint():
        """
        Nice description
        """
        return {"foo_int": 1}

    @second_endpoint.get(model=list[Bar], status_code=200)
    async def bar_endpoint():
        """
        Another nice description
        """
        return [{"bar_int": 1}, {"bar_int": 2}]

    router = APIRouter()

    add_endpoint("/foo", router, first_endpoint)
    add_endpoint("/bar", router, second_endpoint)
    app.include_router(router)

    with TestClient(app) as client:
        resp_foo = client.get("/foo")
        resp_bar = client.get("/bar")
    assert_equals(resp_foo.json(), {"fooInt": 1})
    assert_equals(resp_bar.json(), [{"barInt": 1}, {"barInt": 2}])

    openapi_schema = get_openapi(title="title", version="0.1", routes=app.routes)

    schema_foo = openapi_schema["paths"]["/foo"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
    schema_bar = openapi_schema["paths"]["/bar"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
    title_response_foo = schema_foo.get("title", "No title found")
    title_response_bar = schema_bar.get("title", "No title found")
    assert_equals(title_response_foo, "Foo")
    assert_equals(title_response_bar, "Array[Bar]")


def test_warning_without_docstring(app):
    import warnings

    warnings.resetwarnings()
    from tracktolib.api import Endpoint, add_endpoint, CamelCaseModel

    class Foo(CamelCaseModel):
        foo_int: int

    first_endpoint = Endpoint()
    second_endpoint = Endpoint()
    third_endpoint = Endpoint()
    router = APIRouter()

    @first_endpoint.get(model=Foo)
    async def foo_endpoint():
        return {"foo_int": 1}

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        add_endpoint("/foo", router, first_endpoint)
        assert issubclass(w[0].category, UserWarning)
        assert "Docstring is missing for" in str(w[0].message)

    @second_endpoint.get(model=Foo)
    async def bar_endpoint():
        """ """
        return {"foo_int": 1}

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        add_endpoint("/bar", router, second_endpoint)

        assert issubclass(w[0].category, UserWarning)
        assert "Docstring is missing for" in str(w[0].message)

    @third_endpoint.get(model=Foo)
    async def foo_bar_endpoint():
        """
        Nice description
        """
        return {"foo_int": 1}

    add_endpoint("/foobar", router, third_endpoint)
    app.include_router(router)

    with TestClient(app) as client:
        resp_bar = client.get("/foobar")
    assert resp_bar.status_code == status.HTTP_200_OK
