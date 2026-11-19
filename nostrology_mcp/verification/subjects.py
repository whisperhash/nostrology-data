"""The entities the battery asks about, and the partition rule it asks against.

These are the *subjects* of the questions, not the answers: both the MCP side and
the independent side are recomputed for whichever entity is named here, so a
subject can be swapped without invalidating any expected value.

Pick subjects that are stable and interesting rather than merely top-of-leaderboard;
a subject that vanishes from the data turns its questions into empty-vs-empty
tautologies rather than failures, which the run's `--audit` pass reports.
"""

# The Zapstore curator key. Everything it publishes is a GitHub / F-Droid
# republish EXCEPT its own first-party software, which is natively published
# despite sharing the key. This pair is the native/republished partition rule and
# is domain definition, not something read out of the MCP server.
CURATOR_PUBKEY = "78ce6faa72264387284e647ba6938995735ec8c7d5c5a65737e55130f026307d"
CURATOR_OWN_D_TAGS = ["dev.zapstore.app", "dev.zapstore.alpha", "zapstore"]

# A native developer with several apps, a profile, releases, stacks and zaps -
# exercises nearly every column of every record at once.
DEV_PUBKEY = "7579076d9aff0a4cfdefa7e2045f2486c7e5d8bc63bfc6b45397233e1bbfcb19"
DEV_APP_ID = "com.greenart7c3.nostrsigner"

# The curator's own client: curator key, but native. The own-d-tag exception.
FIRST_PARTY_APP_ID = "dev.zapstore.app"

# A pure republish, for the other side of the partition.
REPUBLISHED_APP_ID = "org.torproject.torbrowser"

# A publisher with many apps and NO kind-0 profile - the NULL-name path.
NO_PROFILE_PUBKEY = "4601a3c7a91925d9a2b1683538a820704ba5b35b67b87a59152dccfecd40802e"

# Guaranteed absent - the empty-table-not-an-error path.
UNKNOWN_PUBKEY = "0" * 64

# Windows the battery uses. Calendar months keep the questions readable; the
# live-chart window is derived at run time from the scraped series instead.
MONTH_FROM, MONTH_TO = "2026-07-01", "2026-07-31"
YEAR_FROM, YEAR_TO = "2024-01-01", "2024-12-31"
APP_WINDOW_FROM, APP_WINDOW_TO = "2026-01-01", "2026-06-30"


def is_native(pubkey, d_tag):
    """The partition predicate, stated once and used by every independent check."""
    return pubkey != CURATOR_PUBKEY or d_tag in CURATOR_OWN_D_TAGS
