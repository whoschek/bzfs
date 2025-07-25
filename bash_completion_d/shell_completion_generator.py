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
"""Emits a bash completion script such that typing bzfs <TAB> or bzfs_jobrunner <TAB> will auto-complete all flags.

Usage: python3 -m bash_completion_d.shell_completion_generator > /etc/bash_completion.d/bzfs-shell-completion
or     python3 -m bash_completion_d.shell_completion_generator > ~/.bash_completion.d/bzfs-shell-completion
"""

from __future__ import annotations
import argparse
import importlib
from pathlib import Path

programs = ("bzfs", "bzfs_jobrunner")


def version_line() -> str:
    """Returns the program version string."""
    from bzfs_main import bzfs

    for act in bzfs.argument_parser()._actions:
        if isinstance(act, argparse._VersionAction):
            return str(act.version)
    raise RuntimeError("Version not found in bzfs argument parser.")


def harvest(module: str) -> tuple[str, set[str], dict[str, str]]:
    """Returns (safe_name, flag_set, value_tokens_map) for a program based on its argument_parser() specs."""
    parser = importlib.import_module("bzfs_main." + module).argument_parser()
    safe = module.replace("-", "_")
    flags: set[str] = set()
    vals: dict[str, str] = {}
    for act in parser._actions:
        if act.help is argparse.SUPPRESS:
            continue
        flags.update(act.option_strings)
        if act.nargs != 0:  # option consumes a value
            tokens: list[str] = []
            if act.choices:
                seq = act.choices.keys() if isinstance(act.choices, dict) else act.choices
                tokens.extend(map(str, seq))
            const_val = getattr(act, "const", None)
            if const_val is not None:
                tokens.append(str(const_val))
            if tokens:
                tok_str = " ".join(sorted(set(tokens)))
                for opt in act.option_strings:
                    vals[opt] = tok_str
    return safe, flags, vals


blocks = []
for program in programs:
    safe, flag_set, val_map = harvest(program)
    blocks.append(f"__opts_{safe}='{' '.join(sorted(flag_set))}'")
    if val_map:
        cases = "\n".join(f"    {flag}) echo {vals} ;;" for flag, vals in sorted(val_map.items()))
        blocks.append(f'__choices_{safe}() {{\n  case "$1" in\n{cases}\n  esac\n}}')
    else:
        blocks.append(f"__choices_{safe}() {{ :; }}")

    # define completion function
    blocks.append(
        f"""_{program}() {{
    local cur prev lst
    COMPREPLY=()
    cur="${{COMP_WORDS[COMP_CWORD]}}"
    prev="${{COMP_WORDS[COMP_CWORD-1]}}"
    lst=$(__choices_{safe} "$prev")
    [[ -z $lst ]] && lst="$__opts_{safe}"
    COMPREPLY=( $(compgen -W "$lst" -- "$cur") )
    return 0
}}"""
    )
    blocks.append(f"complete -o default -o nospace -F _{program} {program}")  # register completion function

body = "\n\n".join(blocks)
print(
    f"""#!/usr/bin/env bash
# bash completion for {version_line()}: typing bzfs <TAB> or bzfs_jobrunner <TAB> will auto-complete all flags.
# Usage: source bzfs-shell-completion
# This script was auto-generated by {Path(__file__).name}

{body}
"""
)
