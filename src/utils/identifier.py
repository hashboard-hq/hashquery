import re


def to_python_identifier(val: str) -> str:
    # From https://stackoverflow.com/a/3305731/23327251.
    return re.sub("\W|^(?=\d)", "_", val)


def is_double_underscore_name(name: str):
    return re.fullmatch(r"__.+__\d*", name)
