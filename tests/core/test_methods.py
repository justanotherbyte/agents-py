import functools

import pytest

from agents.core.methods import get_bound_method, method_name


class Host:
    def remind(self, payload: object, schedule: object) -> None: ...

    @classmethod
    def build(cls) -> None: ...

    @staticmethod
    def helper() -> None: ...

    def __mangled(self) -> None: ...


class Other:
    def remind(self) -> None: ...


def test_get_bound_method_returns_method_bound_to_obj() -> None:
    host = Host()
    method = get_bound_method(host, "remind")
    assert method == host.remind


def test_get_bound_method_missing_raises_attribute_error() -> None:
    with pytest.raises(AttributeError):
        get_bound_method(Host(), "missing")


@pytest.mark.parametrize("name", ["build", "helper"])
def test_get_bound_method_rejects_class_and_static_methods(name: str) -> None:
    with pytest.raises(TypeError):
        get_bound_method(Host(), name)


def test_get_bound_method_rejects_callable_attribute() -> None:
    host = Host()
    host.fn = lambda: None  # ty: ignore[unresolved-attribute]
    with pytest.raises(TypeError):
        get_bound_method(host, "fn")


def test_method_name_from_string_and_bound_method() -> None:
    host = Host()
    assert method_name(host, "remind") == "remind"
    assert method_name(host, host.remind) == "remind"


def test_method_name_rejects_another_objects_method() -> None:
    with pytest.raises(ValueError, match="not a method of Host"):
        method_name(Host(), Other().remind)


def test_method_name_rejects_unnamed_callable() -> None:
    host = Host()
    with pytest.raises(ValueError, match="not a method"):
        method_name(host, functools.partial(host.remind, None))


def test_method_name_mangled_reference_fails_at_call_site() -> None:
    host = Host()
    with pytest.raises(AttributeError):
        method_name(host, host._Host__mangled)  # ty: ignore[unresolved-attribute]
