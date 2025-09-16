# Copyright 2024 Wolfgang Hoschek AT mac DOT com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Unit tests for intern() helpers."""

from __future__ import (
    annotations,
)
import unittest
from typing import (
    Callable,
    ClassVar,
    List,
    Protocol,
)

from bzfs_main.utils import (
    Interner,
    SortedInterner,
    T,
)


#############################################################################
def suite() -> unittest.TestSuite:
    test_cases = [
        TestSortedInterner,
        TestDictInterner,
    ]
    return unittest.TestSuite(unittest.TestLoader().loadTestsFromTestCase(test_case) for test_case in test_cases)


#############################################################################
class InternerProtocol(Protocol[T]):
    def interned(self, item: T) -> T: ...
    def __contains__(self, item: T) -> bool: ...


InternerFactory = Callable[[List[str]], InternerProtocol[str]]


#############################################################################
class MyInternerTest(unittest.TestCase):
    # Subclasses must override these two class vars
    INTERNER_FACTORY: ClassVar[InternerFactory]
    duplicate_prefers_first: ClassVar[bool]

    def setUp(self) -> None:
        # Build runtime strings to avoid CPython literal interning
        self.ant: str = "".join(["a", "nt"])
        self.dog: str = "".join(["d", "og"])
        self.emu: str = "".join(["e", "mu"])
        self.base_list: list[str] = [self.ant, self.dog, self.emu]
        self.interner: InternerProtocol[str] = self.INTERNER_FACTORY(self.base_list)  # type: ignore

    def test_contains_present(self) -> None:
        self.assertTrue("".join(["d", "og"]) in self.interner)
        self.assertTrue(self.dog in self.interner)

    def test_contains_absent(self) -> None:
        self.assertFalse("".join(["c", "at"]) in self.interner)

    def test_intern_existing_returns_canonical_object(self) -> None:
        new_dog = "".join(["do", "g"])
        self.assertIsNot(new_dog, self.dog)
        canonical = self.interner.interned(new_dog)
        # Interned version must be *one* of the original duplicates
        self.assertIs(canonical, self.interner.interned(self.dog))

    def test_intern_nonexisting_returns_same_object(self) -> None:
        cat = "".join(["c", "at"])
        self.assertIs(self.interner.interned(cat), cat)
        self.assertFalse(cat in self.interner)

    def test_empty_interner(self) -> None:
        empty = self.INTERNER_FACTORY([])  # type: ignore
        fresh = "".join(["n", "ew"])
        self.assertIs(empty.interned(fresh), fresh)
        self.assertFalse(fresh in empty)

    def test_duplicates_list_canonical(self) -> None:
        one1 = "".join(["o", "ne"])
        one2 = "".join(["o", "ne"])  # distinct object, same value
        self.assertIsNot(one1, one2)

        dup_list = [self.ant, one1, one2, self.emu]
        interner_dup = self.INTERNER_FACTORY(dup_list)  # type: ignore

        query = "".join(["o", "ne"])
        result = interner_dup.interned(query)

        expected = one1 if self.duplicate_prefers_first else one2
        self.assertIs(result, expected)


#############################################################################
class TestSortedInterner(MyInternerTest):
    INTERNER_FACTORY: ClassVar[InternerFactory] = SortedInterner
    duplicate_prefers_first: ClassVar[bool] = True  # bisect_left picks first dup


#############################################################################
class TestDictInterner(MyInternerTest):
    INTERNER_FACTORY: ClassVar[InternerFactory] = Interner
    duplicate_prefers_first: ClassVar[bool] = False  # dict keeps last dup
