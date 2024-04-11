import importlib.metadata


def get_hashquery_version() -> str:
    try:
        return importlib.metadata.version("hashquery")
    except:
        return "unknown"


HASHQUERY_VERSION = get_hashquery_version()
