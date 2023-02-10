import json
from dataclasses import field, dataclass
from inspect import getdoc
from typing import (
    TypeVar, Callable, Any,
    Literal, Sequence,
    AsyncIterator, Coroutine,
    get_type_hints, get_args, TypedDict,
    TypeAlias, Type, ClassVar
)
from .utils import json_serial, to_camel_case

try:
    from fastapi import params, APIRouter
    from fastapi.responses import JSONResponse
    from pydantic import BaseModel
    import starlette.status
except ImportError:
    raise ImportError('Please install fastapi, pydantic or tracktolib with "api" to use this module')

D = TypeVar('D')


# noqa: N802
def Depends(
        dependency: Callable[...,
        Coroutine[Any, Any, D] |
        Coroutine[Any, Any, D | None] |
        AsyncIterator[D]
        | D] | None = None,
        *,
        use_cache: bool = True
) -> D:
    """TODO: add support for __call__ (maybe see https://github.com/python/typing/discussions/1106 ?)"""
    return params.Depends(dependency, use_cache=use_cache)  # pyright: ignore [reportGeneralTypeIssues]


B = TypeVar('B', bound=BaseModel | None | Sequence[BaseModel])

Response = dict | list[dict] | B

Method = Literal['GET', 'POST', 'DELETE', 'PATCH', 'PUT']

EnpointFn = Callable[..., Response]

Dependencies: TypeAlias = Sequence[params.Depends] | None
StatusCode: TypeAlias = int | None


class MethodMeta(TypedDict):
    fn: EnpointFn
    status_code: StatusCode
    dependencies: Dependencies
    path: str | None
    response_model: Type[BaseModel | None | Sequence[BaseModel]] | None


@dataclass
class Endpoint:
    _methods: dict[Method, MethodMeta] = field(init=False,
                                               default_factory=dict)

    @property
    def methods(self):
        return self._methods

    def get(self, status_code: StatusCode = None,
            dependencies: Dependencies = None,
            path: str | None = None,
            model: Type[B] | None = None):
        return _get_method_wrapper(cls=self, method='GET',
                                   status_code=status_code,
                                   dependencies=dependencies,
                                   path=path,
                                   model=model)

    def post(self, *, status_code: StatusCode = None,
             dependencies: Dependencies = None,
             path: str | None = None,
             model: Type[B] | None = None):
        return _get_method_wrapper(cls=self, method='POST',
                                   status_code=status_code,
                                   dependencies=dependencies,
                                   path=path,
                                   model=model)

    def put(self, status_code: StatusCode = None,
            dependencies: Dependencies = None,
            path: str | None = None,
            model: Type[B] | None = None):
        return _get_method_wrapper(cls=self, method='PUT',
                                   status_code=status_code,
                                   dependencies=dependencies,
                                   path=path,
                                   model=model)

    def delete(self, status_code: StatusCode = None,
               dependencies: Dependencies = None,
               path: str | None = None,
               model: Type[B] | None = None):
        return _get_method_wrapper(cls=self, method='DELETE',
                                   status_code=status_code,
                                   dependencies=dependencies,
                                   path=path,
                                   model=model)

    def patch(self, status_code: StatusCode = None,
              dependencies: Dependencies = None,
              path: str | None = None,
              model: Type[B] | None = None):
        return _get_method_wrapper(cls=self, method='PATCH',
                                   status_code=status_code,
                                   dependencies=dependencies,
                                   path=path,
                                   model=model)


def _get_method_wrapper(cls: Endpoint, method: Method,
                        *,
                        status_code: StatusCode = None,
                        dependencies: Dependencies = None,
                        path: str | None = None,
                        model: Type[B] | None = None):
    def _set_method_wrapper(func: EnpointFn):
        _meta: MethodMeta = {
            'fn': func,
            'status_code': status_code,
            'dependencies': dependencies,
            'path': path,
            'response_model': model
        }
        cls._methods[method] = _meta

    return _set_method_wrapper


_NoneType = type(None)


def _get_return_type(fn):
    return_type = get_type_hints(fn)['return']
    _args = get_args(return_type)
    is_optional = _NoneType in _args
    _model = [x for x in _args if x is not _NoneType][-1]
    return _model if not is_optional else _model | None


def add_endpoint(path: str,
                 router: APIRouter,
                 endpoint: Endpoint,
                 *,
                 dependencies: Dependencies = None
                 ):
    for _method, _meta in endpoint.methods.items():
        _fn = _meta['fn']
        _status_code = _meta['status_code']
        _dependencies = _meta['dependencies']
        _path = _meta['path']
        _response_model = _meta['response_model']
        if not _response_model:
            try:
                _response_model = _get_return_type(_fn)
            except KeyError:
                raise ValueError(f'Could not find a return type for {_method} {path}')

        full_path = path if not _path else f'{path}/{_path}'
        router.add_api_route(full_path,
                             _fn, methods=[_method],
                             name=getdoc(_fn),
                             response_model=_response_model,
                             status_code=_status_code,
                             dependencies=[*(_dependencies or []), *(dependencies or [])])


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
            default=self.json_serial
        ).encode("utf-8")


class CamelCaseModel(BaseModel):
    class Config:
        alias_generator = to_camel_case
        allow_population_by_field_name = True


def check_status(resp, status: int = starlette.status.HTTP_200_OK):
    assert resp.status_code == status, json.dumps(resp.json(), indent=4)
