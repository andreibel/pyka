from pathlib import Path
from pyka.log import Log
import pytest


def test_append_then_read_back(tmp_path: Path) -> None:
    log = Log(tmp_path / "test.log")
    log.append(b"first")
    log.append(b"second")
    log.append(b"third")
    log.close()
    assert list(Log(tmp_path / "test.log")) == [b"first", b"second", b"third"]


@pytest.mark.xfail(reason="A3: append does not flush yet", strict=True)
def test_records_are_visible_before_close(tmp_path: Path) -> None:
    log = Log(tmp_path / "test.log")
    log.append(b"first")
    assert list(Log(tmp_path / "test.log")) == [b"first"]

def test_read_from_offset(tmp_path: Path) -> None:
    log = Log(tmp_path / "test.log")
    log.append(b"first")
    second = log.append(b"second")
    log.append(b"third")
    log.close()
    assert list(Log(tmp_path / "test.log").read_from(second)) == [b"second", b"third"]