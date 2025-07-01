# class CheckRange is copied from https://gist.github.com/dmitriykovalev/2ab1aa33a8099ef2d514925d84aa89e7/30961300d3f8192f775709c06ff9a5b777475adf
# Written by Dmitriy Kovalev
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
# Allows you to validate open, closed, and half-open intervals on int as well as float arguments.
# Each endpoint can be either a number or positive or negative infinity:
# [a, b] --> min=a, max=b
# [a, b) --> min=a, sup=b
# (a, b] --> inf=a, max=b
# (a, b) --> inf=a, sup=b
# [a, +infinity) --> min=a
# (a, +infinity) --> inf=a
# (-infinity, b] --> max=b
# (-infinity, b) --> sup=b

from __future__ import annotations
import argparse
import operator
from typing import Any


# fmt: off
class CheckRange(argparse.Action):
    ops = {'inf': operator.gt,  # noqa: RUF012
           'min': operator.ge,
           'sup': operator.lt,
           'max': operator.le}

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if 'min' in kwargs and 'inf' in kwargs:
            raise ValueError('either min or inf, but not both')
        if 'max' in kwargs and 'sup' in kwargs:
            raise ValueError('either max or sup, but not both')

        for name in self.ops:
            if name in kwargs:
                setattr(self, name, kwargs.pop(name))

        super().__init__(*args, **kwargs)

    def interval(self) -> str:
        if hasattr(self, 'min'):
            lo = f'[{self.min}'
        elif hasattr(self, 'inf'):
            lo = f'({self.inf}'
        else:
            lo = '(-infinity'

        if hasattr(self, 'max'):
            up = f'{self.max}]'
        elif hasattr(self, 'sup'):
            up = f'{self.sup})'
        else:
            up = '+infinity)'

        return f'valid range: {lo}, {up}'

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: str | None = None,
    ) -> None:
        for name, op in self.ops.items():
            if hasattr(self, name) and not op(values, getattr(self, name)):
                raise argparse.ArgumentError(self, self.interval())
        setattr(namespace, self.dest, values)
# fmt: on
