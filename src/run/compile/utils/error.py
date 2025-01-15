class UserCompilationError(Exception):
    """
    An error which occurred during compilation.

    These error instances are considered "user-friendly". They can be understood
    by users unfamiliar with the Hashquery compilation process, and they provide
    enough context that a user can fix the problem themselves.
    """
