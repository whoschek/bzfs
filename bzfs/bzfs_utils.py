# This file is intended to hold utility functions for bzfs.

from typing import List

def cut(field: int = -1, separator: str = "\t", lines: List[str] = None) -> List[str]:
    """Retains only column number 'field' in a list of TSV/CSV lines; Analog to Unix 'cut' CLI command."""
    assert isinstance(lines, list)
    assert len(separator) == 1
    if field == 1:
        return [line[0 : line.index(separator)] for line in lines]
    elif field == 2:
        return [line[line.index(separator) + 1 :] for line in lines]
    else:
        raise ValueError("Unsupported parameter value")

def _utils_aux_test() -> str:
    return "utils_ok"
