# blocklists/

Bot/spam **pubkey** lists (`*.txt`, one hex pubkey per line) that the loader
unions into a single global set and filters out at load time, so the `/relays`
leaderboard counts leave out known bot and spam keys. `_load_blocklist()` in the loader walks
this directory recursively for `.txt` files.

**This directory is a data artifact, not source - it is gitignored** (~61 MB). The lists are
supplied by the operator and not distributed, so they are *not* committed here.

To populate it where the loader runs:

```sh
# on a workstation, for the test that reads the real lists: copy the lists into ./blocklists/

# in a deployment: copy a snapshot to the host directory it bind-mounts at /app/blocklists
# in the Dagster containers.
```

Running the loader with `--no-blocklist` skips filtering entirely (looser counts,
no sync dependency) — acceptable as a fallback if the artifact isn't present.

A missing or empty directory does the same with no flag: the loader loads UNFILTERED,
exits 0 and the run stays green. After any move, read the load's
`Blocklist: N pubkeys loaded` line rather than looking for a failure.
