# markdown_from_argparse man page

<!-- BEGIN-MANPAGE-USAGE -->
```
usage: python3 -m bzfs_main.util.markdown_from_argparse
       [-h]
       --readme PATH
       --module STRING
       [--function STRING]
       [--heading-level INT]
```
<!-- END-MANPAGE-USAGE -->

<!-- BEGIN-MANPAGE-DESCRIPTION -->
Automatically generate or regenerate a markdown README.md manpage from argparse parser definitions. This avoids manually
editing the same doc in two places, namely in the argparse.ArgumentParser help configuration (help=, description=, etc), and
also in a manually edited manpage within README.md.

Has zero dependencies beyond the Python standard library.

Example README.md file:


```markdown
# Example CLI Tool

<!-- BEGIN.MANPAGE.USAGE -->
<!-- END.MANPAGE.USAGE -->

<!-- BEGIN.MANPAGE.DESCRIPTION -->
<!-- END.MANPAGE.DESCRIPTION -->

# Options

<!-- BEGIN.MANPAGE.DETAILS -->
<!-- END.MANPAGE.DETAILS -->
```


Manually replace all occurrences of '.MANPAGE.' with '-MANPAGE-' in the example file above. Then run this to generate the
manpage blurbs and replace the sections between the BEGIN-MANPAGE-* and END-MANPAGE-* marker pairs within the given markdown
file with those blurbs:


```
python3 -m bzfs_main.util.markdown_from_argparse \
  --readme=README_markdown_from_argparse.md \
  --module=bzfs_main.util.markdown_from_argparse \
  --function=_argument_parser
```


Existing file content outside of the marker pair sections is retained as-is. This enables reordering of sections, adding
custom document headers/footers/notes, and other forms of customization.

The [BEGIN|END]-MANPAGE-DESCRIPTION marker pair is optional.

Subparser sections are rendered recursively. Subparsers can be nested arbitrarily.

Generated CLI option entries include explicit HTML `id` anchors and inline permalinks so users can refer to them via copy and
paste. Subparser headings also include explicit HTML `id` anchors.

The renderer expects argparse parser `description`, `epilog`, and `help=` text in the form of blank-line-separated blocks,
where the first block of each `help=` text is prose.

Supported Markdown input contract:

- Blank lines separate blocks. In action help, the first block becomes the generated option bullet text; later blocks are
  indented under it.
- Ordinary prose blocks are rewrapped.
- Single-line Markdown headings (`#` through `######`) are preserved.
- Markdown pipe tables are preserved when the first row contains `|` and the second row is a separator row.
- Homogeneous Markdown list blocks are preserved when the first line starts an ordered or unordered list and following
  nonblank lines are list items or indented continuations.
- Homogeneous multiline blockquote blocks are preserved when every nonblank line starts with `>`.
- Triple-backtick fenced code blocks are preserved, including blank lines inside the fence. Opening and closing fences
  must be balanced.
- Blocks indented with four spaces or one tab are fenced as code.
- Blocks with repeated aligned columns are fenced as code.

Ambiguous layouts are treated as prose and may be rewrapped. Use explicit triple-backtick fences for shell sessions,
configuration, YAML, JSON, mixed prose and examples, or any layout that must survive unchanged.
<!-- END-MANPAGE-DESCRIPTION -->

# Options

<!-- BEGIN-MANPAGE-DETAILS -->
<span id="-h" class="man-option-title">**-h**, **--help**</span> <a href="#-h" title="Permalink to -h" aria-label="Permalink to -h" class="man-option-permalink">&#x1F517;</a>

- show this help message and exit

<!-- -->

<span id="--readme" class="man-option-title">**--readme** *PATH* _(required)_</span> <a href="#--readme" title="Permalink to --readme" aria-label="Permalink to --readme" class="man-option-permalink">&#x1F517;</a>

- Path of README markdown file to update. If the file does not exist a template will be generated. Example: path/to/README.md

<!-- -->

<span id="--module" class="man-option-title">**--module** *STRING* _(required)_</span> <a href="#--module" title="Permalink to --module" aria-label="Permalink to --module" class="man-option-permalink">&#x1F517;</a>

- Python module containing the parser factory. Example: 'bzfs_main.bzfs'

<!-- -->

<span id="--function" class="man-option-title">**--function** *STRING*</span> <a href="#--function" title="Permalink to --function" aria-label="Permalink to --function" class="man-option-permalink">&#x1F517;</a>

- Name of the no-argument parser factory function within the Python module. The function must return an instance of
  argparse.ArgumentParser. Default is 'argument_parser'.

<!-- -->

<span id="--heading-level" class="man-option-title">**--heading-level** *INT*</span> <a href="#--heading-level" title="Permalink to --heading-level" aria-label="Permalink to --heading-level" class="man-option-permalink">&#x1F517;</a>

- Markdown heading level for generated group/subparser sections. Must be >= 1. Default is '1'.

<!-- -->

# Examples


```shell
python3 -m bzfs_main.util.markdown_from_argparse --readme=README.md --module=bzfs_main.bzfs
```

<!-- END-MANPAGE-DETAILS -->
