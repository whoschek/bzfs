---
name: bzfs-bump-dependency-versions
description: >-
  Check if an existing project dependency can and should be updated to a more recent version.
---

For each of the following components check if the version can and should be updated to a more recent version:

- Components with a version number in pyproject.toml
- `uv` via tools/install_uv.sh
- `shellcheck` via tools/install_shellcheck.py
- `prettier` via tools/prettier
- Components in .pre-commit-config.yaml except if they use language: unsupported
- Lima in .github/workflows/python-app.yml and .github/actions/test-almalinux-lima/action.yml
- FreeBSD release and vmactions/freebsd-vm in .github/workflows/python-app.yml
- zizmor in .github/workflows/python-app.yml
- pip-audit in .github/workflows/python-app.yml
- ZFS in bzfs_testbed/lima_vm.sh

Rules:

- Only consider stable releases. Ignore release candidates, prereleases, alpha and beta releases.
- Respect the cooldown period defined in `preinstall_dev.sh`. A cooldown period of N days implies that a newer version
  must be at least N days old (the release is well known at this point), AND no higher version was released in the N
  days after that (the release had no teething issues requiring an emergency patch release).
- Check if the version is compatible.
- Critical: If a relevant new version passes the filters above, evaluate its supply chain risk (including transitive
  dependency updates). Never blindly update just because you can.
- Keep proposed changes as simple as possible. No unnecessary fluff. KISS.
