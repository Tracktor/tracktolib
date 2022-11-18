from dataclasses import field, dataclass
from inspect import getdoc
from typing import (
    TypeVar, Callable, Any,
    Literal, Sequence,
    AsyncIterator, Coroutine,
    get_type_hints, get_args, TypedDict)
import json
from .utils import json_serial

try:
    from fastapi import params, APIRouter
except ImportError:
    raise ImportError('Please install tracktolib with "api" to use this module')

T = TypeVar('T')


# noqa: N802
def Depends(
        dependency: Callable[...,
                             Coroutine[Any, Any, T] |
                             Coroutine[Any, Any, T | None] |
                             AsyncIterator[T]
                             | T] | None = None,
        *,
        use_cache: bool = True
) -> T:
    """TODO: add support for __call__ (maybe see https://github.com/python/typing/discussions/1106 ?)"""
    return params.Depends(dependency, use_cache=use_cache)  # pyright: ignore [reportGeneralTypeIssues]


R = TypeVar('R')

ReturnType = None | dict | list[dict] | R

Method = Literal['GET', 'POST', 'DELETE', 'PATCH', 'PUT']

EnpointFn = Callable[..., ReturnType[Any]]


class MethodMeta(TypedDict):
    fn: EnpointFn
    status_code: int | None


@dataclass
class Endpoint:
    _methods: dict[Method, MethodMeta] = field(init=False,
                                               default_factory=dict)

    @property
    def methods(self):
        return self._methods

    def get(self, status_code: int | None = None):
        return _get_method_wrapper(cls=self, method='GET',
                                   status_code=status_code)

    def post(self, *, status_code: int | None = None):
        return _get_method_wrapper(cls=self, method='POST',
                                   status_code=status_code)

    def put(self, status_code: int | None = None):
        return _get_method_wrapper(cls=self, method='PUT',
                                   status_code=status_code)

    def delete(self, status_code: int | None = None):
        return _get_method_wrapper(cls=self, method='DELETE',
                                   status_code=status_code)

    def patch(self, status_code: int | None = None):
        return _get_method_wrapper(cls=self, method='PATCH',
                                   status_code=status_code)


def _get_method_wrapper(cls: Endpoint, method: Method,
                        *,
                        status_code: int | None = None):
    def _set_method_wrapper(func: EnpointFn):
        _meta: MethodMeta = {
            'fn': func,
            'status_code': status_code
        }
        cls._methods[method] = _meta

    return _set_method_wrapper


def _get_return_type(fn):
    return_type = get_type_hints(fn)['return']
    return get_args(return_type)[-1]


def add_endpoint(path: str,
                 router: APIRouter,
                 endpoint: Endpoint,
                 *,
                 dependencies: Sequence[params.Depends] | None = None
                 ):
    for _method, _meta in endpoint.methods.items():
        _fn = _meta['fn']
        _status_code = _meta['status_code']
        try:
            response_model = _get_return_type(_fn)
        except KeyError:
            raise ValueError(f'Could not find a return type for {_method} {path}')
        router.add_api_route(path, _fn, methods=[_method],
                             name=getdoc(_fn),
                             # TODO: fix this so | None is not needed
                             response_model=response_model | None,
                             status_code=_status_code,
                             dependencies=dependencies)


from fastapi.responses import JSONResponse


class JSONSerialResponse(JSONResponse):

    def render(self, content: Any) -> bytes:
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
            default=json_serial
        ).encode("utf-8")
