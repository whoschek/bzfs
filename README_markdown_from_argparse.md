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
Automatically generate or regenerate a markdown README.md manpage from argparse parser
definitions. This avoids manually editing the same doc in two places, namely in the
argparse.ArgumentParser help configuration (help=, description=, etc), and also in a manually
edited manpage within README.md.

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


Manually replace all occurrences of '.MANPAGE.' with '-MANPAGE-' in the example file above. Then
run this to generate the manpage blurbs and replace the sections between the BEGIN-MANPAGE-* and
END-MANPAGE-* marker pairs within the given markdown file with those blurbs:


```
python3 -m bzfs_main.util.markdown_from_argparse \
  --readme=README_markdown_from_argparse.md \
  --module=bzfs_main.util.markdown_from_argparse \
  --function=_argument_parser
```


Existing file content outside of the marker pair sections is retained as-is. This enables
reordering of sections, adding custom document headers/footers/notes, and other forms of
customization.

The [BEGIN|END]-MANPAGE-DESCRIPTION marker pair is optional.
<!-- END-MANPAGE-DESCRIPTION -->

# Options

<!-- BEGIN-MANPAGE-DETAILS -->
<div id="--readme"></div>

**--readme** *PATH* _(required)_

*  Path of README markdown file to update. If the file does not exist a template will be
    generated. Example: path/to/README.md

<!-- -->

<div id="--module"></div>

**--module** *STRING* _(required)_

*  Python module containing the parser factory. Example: 'bzfs_main.bzfs'

<!-- -->

<div id="--function"></div>

**--function** *STRING*

*  Name of the no-argument parser factory function within the Python module. The function must
    return an instance of argparse.ArgumentParser. Default is 'argument_parser'.

<!-- -->

<div id="--heading-level"></div>

**--heading-level** *INT*

*  Markdown heading level for generated subparser sections. Must be >= 1. Default is '1'.

# Examples


```shell
python3 -m bzfs_main.util.markdown_from_argparse --readme=README.md --module=bzfs_main.bzfs
```

<!-- END-MANPAGE-DETAILS -->
