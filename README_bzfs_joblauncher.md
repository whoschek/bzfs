<!--
 Copyright 2024 Wolfgang Hoschek AT mac DOT com

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.-->
# YAML Job Launcher with bzfs_joblauncher
- [Introduction](#Introduction)
- [Man Page](#Man-Page)
# Introduction
<!-- DO NOT EDIT (This section was auto-generated from ArgumentParser help text as the source of "truth", via update_readme.sh) -->
<!-- BEGIN DESCRIPTION SECTION -->
Reads a YAML jobconfig and invokes bzfs_jobrunner with equivalent arguments.
Unknown CLI arguments are forwarded to bzfs_jobrunner.

<!-- END DESCRIPTION SECTION -->
# Man Page
<!-- BEGIN HELP OVERVIEW SECTION -->
```
usage: bzfs_joblauncher [-h] [--version] config
```
<!-- END HELP OVERVIEW SECTION -->
<!-- BEGIN HELP DETAIL SECTION -->
<div id="bzfs_joblauncher"></div>

**bzfs_joblauncher** [-h] [--version] config

# DESCRIPTION

Reads a YAML jobconfig and invokes bzfs_jobrunner with equivalent arguments. Unknown CLI arguments
are forwarded to bzfs_jobrunner.

<div id="config"></div>

**config**

*  Path to YAML configuration file.



<div id="--version"></div>

**--version**

*  show program's version number and exit

## Install

`bzfs_joblauncher` requires the optional `PyYAML` package.
Install via:

```
pip install 'bzfs[bzfs_joblauncher]'
```
