"""Two ints that must never be confused.

Both are plain ``int`` at runtime — ``NewType`` costs nothing and exists only
so a type checker rejects passing one where the other belongs. Segment.append
returns a Position while Segment.read_from takes an Offset, and Index holds
both, which is where mixing them would otherwise be silent and wrong.
"""

from typing import NewType

Offset = NewType("Offset", int)
"""A logical record number: 0, 1, 2 ... Never a byte count."""

Position = NewType("Position", int)
"""A byte position within a .log file."""