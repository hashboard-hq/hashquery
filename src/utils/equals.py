from .serializable import Serializable


def deep_equal(a: Serializable, b: Serializable) -> bool:
    a_wire = a._to_wire_format()
    b_wire = b._to_wire_format()
    return a_wire == b_wire
