#!/usr/bin/env python3
#
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

"""WARNING: For now, `bzfs_cron` is work-in-progress, and as such may still change in incompatible ways."""

import argparse
import ast
import re
import socket
import subprocess

prog_name = "bzfs_cron"


def argument_parser() -> argparse.ArgumentParser:
    # fmt: off
    parser = argparse.ArgumentParser(
        prog=prog_name,
        description=f"""
WARNING: For now, `bzfs_cron` is work-in-progress, and as such may still change in incompatible ways.

This program is a convenience wrapper around [bzfs](README.md) that automates periodic activities such as creating snapshots,
replicating and pruning, on multiple source hosts and multiple destination hosts, using a single shared 
[deployment specification file](bzfs_tests/bzfs_cron_example.py).

Typically, a cron job on the source host runs `{prog_name}` periodically to create new snapshots (via --create-src-snapshots) 
and prune outdated snapshots and bookmarks on the source (via --prune-src-snapshots and --prune-src-bookmarks), whereas 
another cron job on the destination host runs `{prog_name}` periodically to prune outdated destination snapshots (via 
--prune-dst-snapshots). Yet another cron job runs `{prog_name}` periodically to replicate the recently created snapshots from 
the source to the destination (via --replicate). The frequency of these periodic activities can vary by activity, and is 
typically every second, minute, hour, day, week, month and/or year (or multiples thereof).

This tool is just a convenience wrapper around the `bzfs` CLI.
""", formatter_class=argparse.RawTextHelpFormatter)

    # commands:
    parser.add_argument("--create-src-snapshots", action="store_true",
        help="Take snapshots on src. This command should be called by a program (or cron job) running on the src host.\n\n")
    parser.add_argument("--replicate", action="store_true",
        help="Replicate recent snapshots from src to dst, either in pull mode (recommended) or push mode. For pull mode, "
             "this command should be called by a program (or cron job) running on the dst host; for push mode on the src "
             "host.\n\n")
    parser.add_argument("--prune-src-snapshots", action="store_true",
        help="Prune snapshots on src. This command should be called by a program (or cron job) running on the src host.\n\n")
    parser.add_argument("--prune-src-bookmarks", action="store_true",
        help="Prune bookmarks on src. This command should be called by a program (or cron job) running on the src host.\n\n")
    parser.add_argument("--prune-dst-snapshots", action="store_true",
        help="Prune snapshots on dst. This command should be called by a program (or cron job) running on the dst host.\n\n")

    # options:
    parser.add_argument("--src-host", default="-", metavar="STRING",
        help="Network hostname of src. Used if replicating in pull mode.\n\n")
    dst_hosts_example = {"onsite": "nas", "us-west-1": "bak-us-west-1.example.com",
                         "eu-west-1": "bak-eu-west-1.example.com", "offsite": "archive.example.com"}
    parser.add_argument("--dst-hosts", default="{}", metavar="DICT_STRING",
        help="Dictionary that maps logical replication target names (the infix portion of a snapshot name) to actual "
             f"destination network hostnames. Example: `{format_dict(dst_hosts_example)}`. With this, given a snapshot "
             "name, we can find the destination network hostname to which the snapshot shall be replicated. Also, given a "
             "snapshot name and its own hostname, a destination host can determine if it shall 'pull' replicate the given "
             "snapshot from the --src-host, or if the snapshot is intended for another target host, in which case it skips "
             f"the snapshot. A destination host running {prog_name} will 'pull' snapshots for all targets that map to its "
             "own hostname.\n\n")
    dst_root_datasets_example = {
        "nas": "tank2/bak",
        "bak-us-west-1.example.com": "backups/bak001",
        "bak-eu-west-1.example.com": "backups/bak999",
        "archive.example.com": "archives/zoo",
    }
    parser.add_argument("--dst-root-datasets", default="{}", metavar="DICT_STRING",
        help="Dictionary that maps each destination hostname to a root dataset located on that destination host. "
             "Typically, this is the backup ZFS pool or a ZFS dataset path within that pool. The root dataset name is a "
             "prefix that will be prepended to each dataset that is replicated to that destination host. "
             f"Example: `{format_dict(dst_root_datasets_example)}`\n\n")
    src_snapshot_periods_example = {
        "prod": {
            "onsite": {"secondly": 45, "minutely": 45, "hourly": 48, "daily": 31, "weekly": 26, "monthly": 18, "yearly": 5},
            "us-west-1": {"secondly": 0, "minutely": 0, "hourly": 48, "daily": 31, "weekly": 26, "monthly": 18,
                          "yearly": 5},
            "eu-west-1": {"secondly": 0, "minutely": 0, "hourly": 48, "daily": 31, "weekly": 26, "monthly": 18,
                          "yearly": 5}},
        "test": {
            "offsite": {"12hourly": 42, "weekly": 12},
        },
    }
    parser.add_argument("--src-snapshot-periods", default="{}", metavar="DICT_STRING",
        help="Retention periods for snapshots to be used if pruning src, and when creating new snapshots on src. "
             "Snapshots that do not match a retention period will be deleted. A zero within a retention period indicates "
             "that no snapshots shall be retained (or even be created) for the given period.\n\n"
             f"Example: `{format_dict(src_snapshot_periods_example)}`. This example will, for the organization 'prod' and "
             "the intended logical target 'onsite', create and then retain secondly snapshots that were created less "
             "than 45 seconds ago, yet retain the latest 45 secondly snapshots regardless of creation time. Analog for "
             "the latest 45 minutely snapshots, 48 hourly snapshots, etc. "
             "It will also create and retain snapshots for the targets 'us-west-1' and 'eu-west-1' within the 'prod' "
             "organization. "
             "In addition, it will create and retain snapshots every 12 hours and every week for the 'test' organization, "
             "and name them as being intended for the 'offsite' replication target. "
             "The example creates snapshots with names like "
             "`prod_onsite_<timestamp>_secondly`, `prod_onsite_<timestamp>_minutely`, "
             "`prod_us-west-1_<timestamp>_hourly`, `prod_us-west-1_<timestamp>_daily`, "
             "`prod_eu-west-1_<timestamp>_hourly`, `prod_eu-west-1_<timestamp>_daily`, "
             "`test_offsite_<timestamp>_12hourly`, `test_offsite_<timestamp>_weekly`, and so on.\n\n")
    parser.add_argument("--src-bookmark-periods", default="{}", metavar="DICT_STRING",
        help="Retention periods for bookmarks to be used if pruning src. Has same format as --src-snapshot-periods.\n\n")
    parser.add_argument("--dst-snapshot-periods", default="{}", metavar="DICT_STRING",
        help="Retention periods for snapshots to be used if pruning dst. Has same format as --src-snapshot-periods.\n\n")
    parser.add_argument("--src-user", default="", metavar="STRING",
        help="SSH username on --src-host. Used if replicating in pull mode.\n\n")
    parser.add_argument("--dst-user", default="", metavar="STRING",
        help="SSH username on dst. Used if replicating in push mode.\n\n")
    parser.add_argument("--daemon-replication-frequency", default="minutely", metavar="STRING",
        help="Specifies how often the bzfs daemon shall replicate from src to dst if --daemon-lifetime is nonzero.\n\n")
    parser.add_argument("--daemon-prune-src-frequency", default="minutely", metavar="STRING",
        help="Specifies how often the bzfs daemon shall prune src if --daemon-lifetime is nonzero.\n\n")
    parser.add_argument("--daemon-prune-dst-frequency", default="minutely", metavar="STRING",
        help="Specifies how often the bzfs daemon shall prune dst if --daemon-lifetime is nonzero.\n\n")
    parser.add_argument("root_dataset_pairs", nargs="+", action=DatasetPairsAction, metavar="SRC_DATASET DST_DATASET",
        help="Source and destination dataset pairs (excluding usernames and excluding hostnames, which will all be "
             "auto-appended later).\n\n")
    return parser
    # fmt: on


def main():
    print("WARNING: For now, `bzfs_cron` is work-in-progress, and as such may still change in incompatible ways.")
    parser = argument_parser()
    args, unknown_args = parser.parse_known_args()  # forward all unknown args to `bzfs`
    src_snapshot_periods = ast.literal_eval(args.src_snapshot_periods)
    src_bookmark_periods = ast.literal_eval(args.src_bookmark_periods)
    dst_snapshot_periods = ast.literal_eval(args.dst_snapshot_periods)
    src_host = args.src_host
    dst_hosts = ast.literal_eval(args.dst_hosts)
    dst_root_datasets = ast.literal_eval(args.dst_root_datasets)
    localhostname = socket.getfqdn()
    pull_targets = [target for target, dst_hostname in dst_hosts.items() if dst_hostname == localhostname]
    sep = ","  # for straightforward log file processing

    def resolve_dst_dataset(dst_dataset: str, dst_hostname: str) -> str:
        root_dataset = dst_root_datasets.get(dst_hostname)
        assert root_dataset is not None, f"Hostname '{dst_hostname}' is missing in --dst-root-datasets: {dst_root_datasets}"
        return root_dataset + "/" + dst_dataset if root_dataset else dst_dataset

    if args.create_src_snapshots:
        opts = ["--create-src-snapshots", f"--create-src-snapshots-periods={src_snapshot_periods}", "--skip-replication"]
        opts += [f"--log-file-prefix={prog_name}{sep}create-src-snapshots{sep}"]
        opts += [f"--log-file-suffix={sep}"]
        opts += unknown_args + ["--"]
        for src, dst in args.root_dataset_pairs:
            opts += [src, "dummy"]
        run_cmd(["bzfs"] + opts)
    if args.replicate:

        def add_include_snapshot_regexes(org, target, periods, opts):
            for duration_unit, duration_amount in periods.items():
                if duration_amount > 0:
                    regex = f"{re.escape(org)}_{re.escape(ninfix(target))}.*{re.escape(nsuffix(duration_unit))}"
                    opts.append(f"--include-snapshot-regex={regex}")

        daemon_opts = [f"--daemon-frequency={args.daemon_replication_frequency}"]
        if len(pull_targets) > 0:
            print(f"Replicating targets {pull_targets} in pull mode from {src_host} to {localhostname} ...")
            opts = [f"--ssh-src-user={args.src_user}"] if args.src_user else []
            for org, target_periods in dst_snapshot_periods.items():
                for target, periods in target_periods.items():
                    if target in pull_targets:
                        add_include_snapshot_regexes(org, target, periods, opts)
            opts += [f"--log-file-prefix={prog_name}{sep}pull{sep}"]
            opts += [f"--log-file-suffix={sep}{src_host}{sep}{localhostname}{sep}"]
            opts += unknown_args + ["--"]
            for src, dst in args.root_dataset_pairs:
                opts += [f"{src_host}:{src}", resolve_dst_dataset(dst, localhostname)]
            run_cmd(["bzfs"] + daemon_opts + opts)
        else:
            assert src_host in [localhostname, "-"], "Local hostname must be --src-host or in --dst-hosts: " + localhostname
            targets = {target: "" for org, targetperiods in dst_snapshot_periods.items() for target in targetperiods.keys()}
            for target in targets.keys():
                dst_hostname = dst_hosts[target]
                print(f"Replicating target '{target}' in push mode from {localhostname} to {dst_hostname} ...")
                opts = [f"--ssh-dst-user={args.dst_user}"] if args.dst_user else []
                for org, target_periods in dst_snapshot_periods.items():
                    for target2, periods in target_periods.items():
                        if target == target2:
                            add_include_snapshot_regexes(org, target, periods, opts)
                opts += [f"--log-file-prefix={prog_name}{sep}push{sep}"]
                opts += [f"--log-file-suffix={sep}{localhostname}{sep}{dst_hostname}{sep}"]
                opts += unknown_args + ["--"]
                for src, dst in args.root_dataset_pairs:
                    opts += [src, f"{dst_hostname}:{resolve_dst_dataset(dst, dst_hostname)}"]
                run_cmd(["bzfs"] + daemon_opts + opts)

    if args.prune_src_snapshots or args.prune_src_bookmarks:
        opts = ["--skip-replication"]
        opts += [f"--daemon-frequency={args.daemon_prune_src_frequency}"]
        if args.prune_src_snapshots:
            opts += [f"--log-file-prefix={prog_name}{sep}prune-src-snapshots{sep}"]
        else:
            opts += [f"--log-file-prefix={prog_name}{sep}prune-src-bookmarks{sep}"]
        opts += [f"--log-file-suffix={sep}"]
        opts += unknown_args + ["--"]
        for src, dst in args.root_dataset_pairs:
            opts += ["dummy", src]
        if args.prune_src_snapshots:
            run_cmd(
                ["bzfs", "--delete-dst-snapshots", f"--delete-dst-snapshots-except-periods={src_snapshot_periods}"] + opts
            )
        if args.prune_src_bookmarks:
            run_cmd(
                [
                    "bzfs",
                    "--delete-dst-snapshots=bookmarks",
                    f"--delete-dst-snapshots-except-periods={src_bookmark_periods}",
                ]
                + opts
            )
    if args.prune_dst_snapshots:
        dst_snapshot_periods = {  # only retain targets that belong to the host executing bzfs_cron
            org: {target: periods for target, periods in target_periods.items() if target in pull_targets}
            for org, target_periods in dst_snapshot_periods.items()
        }
        opts = ["--delete-dst-snapshots", "--skip-replication"]
        opts += [f"--delete-dst-snapshots-except-periods={dst_snapshot_periods}"]
        opts += [f"--daemon-frequency={args.daemon_prune_dst_frequency}"]
        opts += [f"--log-file-prefix={prog_name}{sep}prune-dst-snapshots{sep}"]
        opts += [f"--log-file-suffix={sep}"]
        opts += unknown_args + ["--"]
        for src, dst in args.root_dataset_pairs:
            opts += ["dummy", resolve_dst_dataset(dst, localhostname)]
        run_cmd(["bzfs"] + opts)


def run_cmd(*params):
    subprocess.run(*params, text=True, check=True)


def nsuffix(s: str) -> str:
    return "_" + s if s else ""


def ninfix(s: str) -> str:
    return s + "_" if s else ""


def format_dict(dictionary) -> str:
    return f'"{dictionary}"'


#############################################################################
class DatasetPairsAction(argparse.Action):
    def __call__(self, parser, namespace, datasets, option_string=None):
        if len(datasets) % 2 != 0:
            parser.error(f"Each SRC_DATASET must have a corresponding DST_DATASET: {datasets}")
        root_dataset_pairs = [(datasets[i], datasets[i + 1]) for i in range(0, len(datasets), 2)]
        setattr(namespace, self.dest, root_dataset_pairs)


#############################################################################
if __name__ == "__main__":
    main()
