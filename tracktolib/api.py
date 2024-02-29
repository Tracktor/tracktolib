import json
import os
import warnings
from dataclasses import field, dataclass
from inspect import getdoc
from typing import (
    TypeVar,
    Callable,
    Any,
    Literal,
    Sequence,
    AsyncIterator,
    Coroutine,
    get_type_hints,
    get_args,
    TypedDict,
    TypeAlias,
    Type,
    ClassVar,
    get_origin,
)

from .utils import json_serial

try:
    from fastapi import params, APIRouter
    from fastapi.routing import APIRoute
    from fastapi.responses import JSONResponse
    from pydantic.alias_generators import to_camel
    from pydantic import BaseModel, ConfigDict
    import starlette.status
except ImportError:
    raise ImportError('Please install fastapi, pydantic or tracktolib with "api" to use this module')

D = TypeVar("D")


# noqa: N802
def Depends(
    dependency: Callable[
        ...,
        Coroutine[Any, Any, D] | Coroutine[Any, Any, D | None] | AsyncIterator[D] | D,
    ]
    | None = None,
    *,
    use_cache: bool = True,
) -> D:
    """TODO: add support for __call__ (maybe see https://github.com/python/typing/discussions/1106 ?)"""
    return params.Depends(dependency, use_cache=use_cache)  # pyright: ignore [reportGeneralTypeIssues]


B = TypeVar("B", bound=BaseModel | None | Sequence[BaseModel])

Response = dict | list[dict] | B

Method = Literal["GET", "POST", "DELETE", "PATCH", "PUT"]

EnpointFn = Callable[..., Response]

Dependencies: TypeAlias = Sequence[params.Depends] | None
StatusCode: TypeAlias = int | None


class MethodMeta(TypedDict):
    fn: EnpointFn
    status_code: StatusCode
    dependencies: Dependencies
    path: str | None
    response_model: Type[BaseModel | None | Sequence[BaseModel]] | None
    openapi_extra: dict[str, Any] | None


@dataclass
class Endpoint:
    _methods: dict[Method, MethodMeta] = field(init=False, default_factory=dict)

    @property
    def methods(self):
        return self._methods

    def get(
        self,
        status_code: StatusCode = None,
        dependencies: Dependencies = None,
        path: str | None = None,
        model: Type[B] | None = None,
        openapi_extra: dict[str, Any] | None = None,
    ):
        return _get_method_wrapper(
            cls=self,
            method="GET",
            status_code=status_code,
            dependencies=dependencies,
            path=path,
            model=model,
            openapi_extra=openapi_extra,
        )

    def post(
        self,
        *,
        status_code: StatusCode = None,
        dependencies: Dependencies = None,
        path: str | None = None,
        model: Type[B] | None = None,
        openapi_extra: dict[str, Any] | None = None,
    ):
        return _get_method_wrapper(
            cls=self,
            method="POST",
            status_code=status_code,
            dependencies=dependencies,
            path=path,
            model=model,
            openapi_extra=openapi_extra,
        )

    def put(
        self,
        status_code: StatusCode = None,
        dependencies: Dependencies = None,
        path: str | None = None,
        model: Type[B] | None = None,
        openapi_extra: dict[str, Any] | None = None,
    ):
        return _get_method_wrapper(
            cls=self,
            method="PUT",
            status_code=status_code,
            dependencies=dependencies,
            path=path,
            model=model,
            openapi_extra=openapi_extra,
        )

    def delete(
        self,
        status_code: StatusCode = None,
        dependencies: Dependencies = None,
        path: str | None = None,
        model: Type[B] | None = None,
        openapi_extra: dict[str, Any] | None = None,
    ):
        return _get_method_wrapper(
            cls=self,
            method="DELETE",
            status_code=status_code,
            dependencies=dependencies,
            path=path,
            model=model,
            openapi_extra=openapi_extra,
        )

    def patch(
        self,
        status_code: StatusCode = None,
        dependencies: Dependencies = None,
        path: str | None = None,
        model: Type[B] | None = None,
        openapi_extra: dict[str, Any] | None = None,
    ):
        return _get_method_wrapper(
            cls=self,
            method="PATCH",
            status_code=status_code,
            dependencies=dependencies,
            path=path,
            model=model,
            openapi_extra=openapi_extra,
        )


def _get_method_wrapper(
    cls: Endpoint,
    method: Method,
    *,
    status_code: StatusCode = None,
    dependencies: Dependencies = None,
    path: str | None = None,
    model: Type[B] | None = None,
    openapi_extra: dict[str, Any] | None = None,
):
    def _set_method_wrapper(func: EnpointFn):
        if model is not None:
            _openapi_extra = {
                **(openapi_extra or {}),
                **generate_list_name_model(model, status_code),
            }
        else:
            _openapi_extra = openapi_extra
        _meta: MethodMeta = {
            "fn": func,
            "status_code": status_code,
            "dependencies": dependencies,
            "path": path,
            "response_model": model,
            "openapi_extra": _openapi_extra,
        }
        cls._methods[method] = _meta

    return _set_method_wrapper


_NoneType = type(None)


class IgnoreConfig(BaseModel):
    endpoints: dict[str, dict[Method, bool]]
    ignore_missing: bool = True


def get_ignore_config() -> IgnoreConfig | None:
    _config = os.getenv("IGNORE_CONFIG")
    return IgnoreConfig.model_validate_json(_config) if _config else None


def set_ignore_config(config: str | IgnoreConfig):
    if isinstance(config, str):
        config = IgnoreConfig.model_validate_json(config)
    os.environ["IGNORE_CONFIG"] = config.model_dump_json()


def _filter_route(route: APIRoute, ignored_route: dict[Method, bool], ignore_missing: bool) -> APIRoute | None:
    # If no config is provided and default is to ignore missing, return the route
    if ignored_route is None and not ignore_missing:
        return route

    has_methods = False
    enabled_methods = {method for method, has_access in ignored_route.items() if has_access}
    for method in list(route.methods):
        # If the config is not specified, we remove the method if ignore_missing is True
        if method not in ignored_route:
            if ignore_missing:
                route.methods -= {method}
                continue
            else:
                has_methods = True
                continue
        elif method not in enabled_methods:
            route.methods -= {method}
        else:
            has_methods = True

    if not has_methods:
        return None
    return route


def filter_routes(routes: list[APIRoute], ignore_config: IgnoreConfig, prefix: str = "") -> list[APIRoute]:
    _routes = []
    for route in routes:
        if not isinstance(route, APIRoute):
            _routes.append(route)
            continue

        _ignored_route = ignore_config.endpoints.get(f"{prefix}{route.path}")
        _route = _filter_route(route, _ignored_route or {}, ignore_missing=ignore_config.ignore_missing)
        if _route is not None:
            _routes.append(_route)
    return _routes


def _get_return_type(fn):
    return_type = get_type_hints(fn)["return"]
    _args = get_args(return_type)
    is_optional = _NoneType in _args
    _model = [x for x in _args if x is not _NoneType][-1]
    return _model if not is_optional else _model | None


def add_endpoint(
    path: str,
    router: APIRouter,
    endpoint: Endpoint,
    *,
    dependencies: Dependencies = None,
):
    _ignore_config = get_ignore_config()
    for _method, _meta in endpoint.methods.items():
        _fn = _meta["fn"]
        _status_code = _meta["status_code"]
        _dependencies = _meta["dependencies"]
        _path = _meta["path"]
        _response_model = _meta["response_model"]
        if not _response_model:
            try:
                _response_model = _get_return_type(_fn)
            except KeyError:
                raise ValueError(f"Could not find a return type for {_method} {path}")

        full_path = path if not _path else f"{path}/{_path}"

        if not getdoc(_fn):
            warnings.warn(f"Docstring is missing for {_method} {path}")

        # Todo: add warning name is not None
        router.add_api_route(
            full_path,
            _fn,
            methods=[_method],
            name=getdoc(_fn),
            response_model=_response_model,
            status_code=_status_code,
            dependencies=[*(_dependencies or []), *(dependencies or [])],
            openapi_extra=_meta.get("openapi_extra"),
        )


@dataclass(init=False)
class JSONSerialResponse(JSONResponse):
    json_serial: ClassVar[Callable[[Any], str]] = field(default=json_serial)

    def render(self, content: Any) -> bytes:
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
            default=self.json_serial,
        ).encode("utf-8")


def model_to_list(string: str) -> str:
    return f"Array[{string}]"


class CamelCaseModel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, coerce_numbers_to_str=True, populate_by_name=True)


def check_status(resp, status: int = starlette.status.HTTP_200_OK):
    assert resp.status_code == status, json.dumps(resp.json(), indent=4)


def generate_list_name_model(model: Type[B], status: int | None = None) -> dict:
    _status = "200" if status is None else str(status)
    if get_origin(model) and get_origin(model) is list:
        _title = f"Array[{get_args(model)[0].__name__}]"
    else:
        _title = model.__name__ if hasattr(model, "__name__") else None

    # Todo verify response content type
    return {"responses": {_status: {"content": {"application/json": {"schema": {"title": _title}}}}}}
