# import string
# from pathlib import Path
#
# from pyka.log import Log
#
# # Topic names become filenames, so only characters that are safe in a
# # path are allowed. "." and ".." pass the character check but are path
# # traversal, so they are rejected by name (Kafka does the same).
# _ALLOWED_NAME_CHARS = frozenset(string.ascii_letters + string.digits + "._-")
# _MAX_NAME_LENGTH = 200
#
#
# def _validate_name(name: str) -> None:
#     """Raise ``ValueError`` unless ``name`` is usable as a log filename."""
#     if not name:
#         raise ValueError("topic name must not be empty")
#     if len(name) > _MAX_NAME_LENGTH:
#         raise ValueError(f"topic name longer than {_MAX_NAME_LENGTH}: {name!r}")
#     if name in (".", ".."):
#         raise ValueError(f"reserved topic name: {name!r}")
#     if not set(name) <= _ALLOWED_NAME_CHARS:
#         raise ValueError(f"invalid topic name: {name!r}")
#
#
# class Topic:
#     def __init__(self, directory: Path) -> None:
#         self._directory = directory
#         self._directory.mkdir(parents=True, exist_ok=True)
#         self._logs: dict[str, Log] = {}
#
#     def log(self, name: str) -> Log:
#         """Return the log for ``name``, creating and caching it on first use.
#
#         :param name: topic name; must be a valid log filename
#         :raises ValueError: if the name is not a legal topic name
#         """
#         _validate_name(name)
#         if name not in self._logs:
#             self._logs[name] = Log(self._directory / f"{name}.log")
#         return self._logs[name]
#
#     def exists(self, name: str) -> bool:
#         return (self._directory/f"{name}.log").exists()
#
#     def names(self) -> list[str]:
#         return sorted( p.stem for p in self._directory.glob("*.log"))
#
#     def close(self) -> None:
#         for log in self._logs.values():
#             log.close()
#         self._logs.clear()
#
#
#
