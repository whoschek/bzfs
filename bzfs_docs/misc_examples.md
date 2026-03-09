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

# Miscellaneous bzfs Examples

- Replicate all daily snapshots created during the last 7 days, and at the same time ensure that the latest 7 daily
  snapshots (per dataset) are replicated regardless of creation time:

```
$ bzfs tank1/foo/bar tank2/boo/bar --recursive --include-snapshot-regex '.*_daily' \
--include-snapshot-times-and-ranks '7 days ago..anytime' 'latest 7'
```

- Delete all tmp datasets within tank2/boo/bar:

```
$ bzfs dummy tank2/boo/bar --dryrun --recursive --skip-replication \
--delete-dst-datasets --include-dataset-regex '(.*/)?tmp.*' --exclude-dataset-regex '!.*'
```

- Delete all daily snapshots older than 7 days, but ensure that the latest 7 daily snapshots (per dataset) are retained
  regardless of creation time:

```
$ bzfs dummy tank2/boo/bar --dryrun --recursive --skip-replication \
--delete-dst-snapshots --include-snapshot-regex '.*_daily' \
--include-snapshot-times-and-ranks notime 'all except latest 7' \
--include-snapshot-times-and-ranks 'anytime..7 days ago'
```

- Delete all daily snapshots older than 7 days, but ensure that the latest 7 daily snapshots (per dataset) are retained
  regardless of creation time. Additionally, only delete a snapshot if no corresponding snapshot or bookmark exists in
  the source dataset:

```
$ bzfs tank1/foo/bar tank2/boo/bar --dryrun --recursive --skip-replication \
--delete-dst-snapshots --include-snapshot-regex '.*_daily' \
--include-snapshot-times-and-ranks notime 'all except latest 7' \
--include-snapshot-times-and-ranks '7 days ago..anytime'
```

- Delete all daily snapshots older than 7 days, but ensure that the latest 7 daily snapshots (per dataset) are retained
  regardless of creation time. Additionally, only delete a snapshot if no corresponding snapshot exists in the source
  dataset (same as above except append 'no-crosscheck'):

```
$ bzfs tank1/foo/bar tank2/boo/bar --dryrun --recursive --skip-replication \
--delete-dst-snapshots --include-snapshot-regex '.*_daily' \
--include-snapshot-times-and-ranks notime 'all except latest 7' \
--include-snapshot-times-and-ranks 'anytime..7 days ago' \
--delete-dst-snapshots-no-crosscheck
```

- Delete all daily bookmarks older than 90 days, but retain the latest 200 daily bookmarks (per dataset) regardless of
  creation time:

```
$ bzfs dummy tank1/foo/bar --dryrun --recursive --skip-replication \
--delete-dst-snapshots=bookmarks --include-snapshot-regex '.*_daily' \
--include-snapshot-times-and-ranks notime 'all except latest 200' \
--include-snapshot-times-and-ranks 'anytime..90 days ago'
```

- Retain all secondly snapshots that were created less than 40 seconds ago, and ensure that the latest 40 secondly
  snapshots (per dataset) are retained regardless of creation time. Same for 40 minutely snapshots, 36 hourly snapshots,
  31 daily snapshots, 12 weekly snapshots, 18 monthly snapshots, and 5 yearly snapshots:

```
$ bzfs dummy tank2/boo/bar --dryrun --recursive --skip-replication \
--delete-dst-snapshots --delete-dst-snapshots-except --include-snapshot-regex '.*_secondly' \
--include-snapshot-times-and-ranks '40 seconds ago..anytime' 'latest 40' \
--new-snapshot-filter-group --include-snapshot-regex '.*_minutely' \
--include-snapshot-times-and-ranks '40 minutes ago..anytime' 'latest 40' \
--new-snapshot-filter-group --include-snapshot-regex '.*_hourly' \
--include-snapshot-times-and-ranks '36 hours ago..anytime' 'latest 36' \
--new-snapshot-filter-group --include-snapshot-regex '.*_daily' \
--include-snapshot-times-and-ranks '31 days ago..anytime' 'latest 31' \
--new-snapshot-filter-group --include-snapshot-regex '.*_weekly' \
--include-snapshot-times-and-ranks '12 weeks ago..anytime' 'latest 12' \
--new-snapshot-filter-group --include-snapshot-regex '.*_monthly' \
--include-snapshot-times-and-ranks '18 months ago..anytime' 'latest 18' \
--new-snapshot-filter-group --include-snapshot-regex '.*_yearly' \
--include-snapshot-times-and-ranks '5 years ago..anytime' 'latest 5'
```

- Example with further options:

```
$ bzfs tank1/foo/bar root@host2.example.com:tank2/boo/bar --recursive \
--exclude-snapshot-regex '.*_(secondly|minutely)' --exclude-snapshot-regex 'test_.*' \
--include-snapshot-times-and-ranks '7 days ago..anytime' 'latest 7' --exclude-dataset \
/tank1/foo/bar/temporary --exclude-dataset /tank1/foo/bar/baz/trash --exclude-dataset-regex \
'(.*/)?private' --exclude-dataset-regex \
'(.*/)?[Tt][Ee]?[Mm][Pp][-_]?[0-9]*'
```
