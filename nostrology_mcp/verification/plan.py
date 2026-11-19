"""The MCP calls the battery is built on.

One call can back many questions, so the plan is deliberately smaller than the
question list. To add a question that needs a call not listed here, add the call
(a new stable id) and reference that id from `questions.py`.
"""
from . import subjects as S

METRIC = "nostrology_query_metric"
REFERENCE = "nostrology_query_reference"


def build():
    m, r = METRIC, REFERENCE
    return [
        # -- full daily series: the four publishing metrics and the two zap metrics
        {"id": "M01", "tool": m, "args": {"metric": "new_native_app_count"}},
        {"id": "M02", "tool": m, "args": {"metric": "new_republished_app_count"}},
        {"id": "M03", "tool": m, "args": {"metric": "native_release_count"}},
        {"id": "M04", "tool": m, "args": {"metric": "republished_release_count"}},
        {"id": "M05", "tool": m, "args": {"metric": "zap_count"}},
        {"id": "M06", "tool": m, "args": {"metric": "zapped_msats"}},

        # -- leaderboards, unfiltered
        {"id": "M07", "tool": m, "args": {"metric": "app_zaps", "grain": "total"}},
        {"id": "M08", "tool": m, "args": {"metric": "app_zaps", "grain": "day"}},
        {"id": "M09", "tool": m, "args": {"metric": "app_stack_count"}},
        {"id": "M10", "tool": m, "args": {"metric": "publisher_app_count"}},

        # -- entity filters
        {"id": "M11", "tool": m, "args": {"metric": "app_zaps", "grain": "total",
                                          "publisher_pubkey": S.DEV_PUBKEY}},
        {"id": "M12", "tool": m, "args": {"metric": "app_zaps", "grain": "total",
                                          "publisher_pubkey": S.DEV_PUBKEY, "app_id": S.DEV_APP_ID}},
        {"id": "M13", "tool": m, "args": {"metric": "app_zaps", "grain": "total",
                                          "publisher_pubkey": S.CURATOR_PUBKEY,
                                          "app_id": S.FIRST_PARTY_APP_ID}},
        {"id": "M14", "tool": m, "args": {"metric": "app_stack_count",
                                          "publisher_pubkey": S.DEV_PUBKEY}},
        {"id": "M15", "tool": m, "args": {"metric": "publisher_app_count",
                                          "publisher_pubkey": S.DEV_PUBKEY}},
        {"id": "M16", "tool": m, "args": {"metric": "app_zaps", "grain": "day",
                                          "publisher_pubkey": S.DEV_PUBKEY, "app_id": S.DEV_APP_ID}},

        # -- windowed
        {"id": "M17", "tool": m, "args": {"metric": "app_zaps", "grain": "total",
                                          "from": S.MONTH_FROM, "to": S.MONTH_TO}},
        {"id": "M18", "tool": m, "args": {"metric": "zap_count",
                                          "from": S.MONTH_FROM, "to": S.MONTH_TO}},
        {"id": "M19", "tool": m, "args": {"metric": "zapped_msats",
                                          "from": S.MONTH_FROM, "to": S.MONTH_TO}},
        {"id": "M20", "tool": m, "args": {"metric": "native_release_count",
                                          "from": S.MONTH_FROM, "to": S.MONTH_TO}},
        {"id": "M21", "tool": m, "args": {"metric": "republished_release_count",
                                          "from": S.MONTH_FROM, "to": S.MONTH_TO}},
        {"id": "M22", "tool": m, "args": {"metric": "new_native_app_count",
                                          "from": S.MONTH_FROM, "to": S.MONTH_TO}},

        # -- reference records
        {"id": "M23", "tool": r, "args": {"record": "app_catalog",
                                          "publisher_pubkey": S.DEV_PUBKEY}},
        {"id": "M24", "tool": r, "args": {"record": "app_catalog",
                                          "publisher_pubkey": S.DEV_PUBKEY, "app_id": S.DEV_APP_ID}},
        {"id": "M25", "tool": r, "args": {"record": "app_catalog",
                                          "publisher_pubkey": S.CURATOR_PUBKEY}},
        {"id": "M26", "tool": r, "args": {"record": "publisher_profile"}},
        {"id": "M27", "tool": r, "args": {"record": "publisher_profile",
                                          "publisher_pubkey": S.DEV_PUBKEY}},
        {"id": "M28", "tool": r, "args": {"record": "publisher_profile",
                                          "publisher_pubkey": S.CURATOR_PUBKEY}},
        {"id": "M29", "tool": r, "args": {"record": "app_releases",
                                          "publisher_pubkey": S.DEV_PUBKEY}},
        {"id": "M30", "tool": r, "args": {"record": "app_releases",
                                          "publisher_pubkey": S.DEV_PUBKEY, "app_id": S.DEV_APP_ID}},
        {"id": "M31", "tool": r, "args": {"record": "app_releases",
                                          "publisher_pubkey": S.DEV_PUBKEY, "app_id": S.DEV_APP_ID,
                                          "from": S.APP_WINDOW_FROM, "to": S.APP_WINDOW_TO}},

        # -- contract / error handling
        {"id": "M32", "tool": r, "args": {"record": "app_catalog",
                                          "publisher_pubkey": S.UNKNOWN_PUBKEY}},
        {"id": "M33", "tool": r, "args": {"record": "app_catalog", "app_id": S.DEV_APP_ID}},
        {"id": "M34", "tool": m, "args": {"metric": "total_downloads"}},
        {"id": "M35", "tool": m, "args": {"metric": "app_stack_count",
                                          "from": S.MONTH_FROM, "to": S.MONTH_TO}},
        {"id": "M36", "tool": m, "args": {"metric": "publisher_app_count", "grain": "day"}},
        {"id": "M37", "tool": m, "args": {"metric": "zap_count",
                                          "from": S.MONTH_TO, "to": S.MONTH_FROM}},
        {"id": "M38", "tool": m, "args": {"metric": "zap_count",
                                          "from": "2099-01-01", "to": "2099-01-31"}},
        {"id": "M39", "tool": m, "args": {"metric": "app_zaps", "grain": "current"}},
        {"id": "M40", "tool": m, "args": {"metric": "zap_count", "from": S.MONTH_FROM,
                                          "to": S.MONTH_TO, "format": "markdown"}},
        {"id": "M48", "tool": m, "args": {"metric": "native_release_count", "grain": "total"}},
        {"id": "M51", "tool": m, "args": {"metric": "app_zaps", "grain": "total",
                                          "publisher_pubkey": S.UNKNOWN_PUBKEY}},

        # -- the republished side, and the no-profile publisher
        {"id": "M41", "tool": r, "args": {"record": "app_catalog",
                                          "publisher_pubkey": S.CURATOR_PUBKEY,
                                          "app_id": S.REPUBLISHED_APP_ID}},
        {"id": "M42", "tool": r, "args": {"record": "app_releases",
                                          "publisher_pubkey": S.CURATOR_PUBKEY,
                                          "app_id": S.REPUBLISHED_APP_ID}},
        {"id": "M43", "tool": m, "args": {"metric": "app_zaps", "grain": "total",
                                          "publisher_pubkey": S.CURATOR_PUBKEY}},
        {"id": "M44", "tool": m, "args": {"metric": "app_stack_count",
                                          "publisher_pubkey": S.CURATOR_PUBKEY}},
        {"id": "M45", "tool": m, "args": {"metric": "new_native_app_count",
                                          "from": S.YEAR_FROM, "to": S.YEAR_TO}},
        {"id": "M49", "tool": m, "args": {"metric": "publisher_app_count",
                                          "publisher_pubkey": S.NO_PROFILE_PUBKEY}},
        {"id": "M50", "tool": r, "args": {"record": "app_catalog",
                                          "publisher_pubkey": S.NO_PROFILE_PUBKEY}},

        # -- Routstr (2026-08-29): the second domain. The 30d window and the top
        # provider are filled in at run time from the document (run.py).
        {"id": "R01", "tool": m, "args": {"metric": "network_summary", "domain": "routstr"}},
        {"id": "R02", "tool": m, "args": {"metric": "network_activity", "domain": "routstr"}},
        {"id": "R03", "tool": m, "args": {"metric": "cumulative_revenue", "domain": "routstr"}},
        {"id": "R04", "tool": m, "args": {"metric": "provider_leaderboard", "domain": "routstr"}},
        {"id": "R05", "tool": m, "args": {"metric": "provider_activity", "domain": "routstr",
                                          "grain": "total"}},
        {"id": "R06", "tool": m, "args": {"metric": "provider_activity", "domain": "routstr",
                                          "grain": "total", "from": "__ROUTSTR_30D_FROM__",
                                          "to": "__ROUTSTR_30D_TO__"}},
        {"id": "R07", "tool": m, "args": {"metric": "model_leaderboard", "domain": "routstr"}},
        {"id": "R08", "tool": m, "args": {"metric": "model_activity", "domain": "routstr",
                                          "grain": "total"}},
        {"id": "R09", "tool": m, "args": {"metric": "model_activity", "domain": "routstr",
                                          "grain": "total", "from": "__ROUTSTR_30D_FROM__",
                                          "to": "__ROUTSTR_30D_TO__"}},
        {"id": "R10", "tool": m, "args": {"metric": "catalog_size", "domain": "routstr"}},
        {"id": "R11", "tool": m, "args": {"metric": "success_rate", "domain": "routstr"}},
        {"id": "R12", "tool": m, "args": {"metric": "provider_facets", "domain": "routstr"}},
        {"id": "R13", "tool": r, "args": {"record": "provider_profile", "domain": "routstr"}},
        {"id": "R14", "tool": r, "args": {"record": "provider_reviews", "domain": "routstr"}},
        {"id": "R15", "tool": r, "args": {"record": "catalog_versions", "domain": "routstr"}},
        {"id": "R16", "tool": r, "args": {"record": "model_catalog", "domain": "routstr"}},
        {"id": "R17", "tool": r, "args": {"record": "exclusions", "domain": "routstr"}},
        {"id": "R18", "tool": m, "args": {"metric": "provider_activity", "domain": "routstr",
                                          "grain": "day", "provider_pubkey": "__ROUTSTR_TOP_PROVIDER__"}},
        # domain resolution by name alone, and the cross-domain rejections
        {"id": "R19", "tool": m, "args": {"metric": "network_summary"}},
        {"id": "R20", "tool": m, "args": {"metric": "zap_count", "provider_pubkey": "__ROUTSTR_TOP_PROVIDER__"}},
        {"id": "R21", "tool": m, "args": {"metric": "provider_leaderboard", "domain": "routstr",
                                          "publisher_pubkey": S.DEV_PUBKEY}},
        {"id": "R22", "tool": m, "args": {"metric": "model_leaderboard", "domain": "routstr",
                                          "model": "(other)"}},

        # -- the live "rolling 30 day" leaderboard cards. The site cuts on a
        # mid-day timestamp; a calendar window can only take the boundary day
        # whole or drop it whole, so BOTH are fetched and the live value must
        # fall between them. `from` is filled in at run time (see run.py).
        {"id": "W30", "tool": m, "args": {"metric": "app_zaps", "grain": "total",
                                          "from": "__ROLLING30_FROM__", "to": "__TODAY__"}},
        {"id": "W30E", "tool": m, "args": {"metric": "app_zaps", "grain": "total",
                                           "from": "__ROLLING30_FROM_EXCL__", "to": "__TODAY__"}},
        # -- open-ended `to` (the in-progress-day wrinkle)
        {"id": "M52", "tool": m, "args": {"metric": "zap_count", "from": "__ZAP_ERA_FROM__"}},
    ]
