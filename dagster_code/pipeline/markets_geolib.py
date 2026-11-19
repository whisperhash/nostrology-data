"""Free-text / geohash -> ISO-3166 country, plus country centroids.

Deliberately conservative: a bare city name is accepted only when the whole
gazetteer maps it to exactly one country, and country names that double as
ordinary English nouns are held out of the substring scan, so a turkey
sandwich is not sold from Ankara.

PRODUCTION HOME of the marketplaces geo resolution (used by pipeline/marketplaces.py,
the marketplaces_refresh job). crosscheck/build_markets_snapshot.py, the independent
cross-check of this pipeline (crosscheck/compare_markets_snapshots.py diffs the two outputs),
imports this module rather than keeping a copy, so the agreement gate cannot catch a mistake
here: test_pipeline_markets_geolib.py holds these rules. The metric agreement between the two
methods also depends on both environments pinning the same reverse_geocoder + pycountry
versions (the gazetteer and the country-name tables are version-shaped data, not code):
crosscheck/requirements.in follows dagster_code/requirements.in.

norm() also decides what counts as a bitcoin price (BTC_UNITS, beside the generators), and
nostrology-web's src/lib/markets/currency.ts folds a `price` unit the same way: change them
together, no test reaches across the repos.
"""
import csv, os, re, unicodedata, collections
import pycountry

# The gazetteer ships inside reverse_geocoder; locate it through the installed
# package rather than a path relative to this file, so the script runs from any
# venv.
import reverse_geocoder as _rg
GAZ = os.path.join(os.path.dirname(_rg.__file__), "rg_cities1000.csv")
B32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", s.strip().lower())


def geohash_decode(gh):
    gh = (gh or "").strip().lower()
    if not gh or any(c not in B32 for c in gh):
        return None
    lat, lon, even = [-90.0, 90.0], [-180.0, 180.0], True
    for c in gh:
        idx = B32.index(c)
        for mask in (16, 8, 4, 2, 1):
            rng = lon if even else lat
            mid = (rng[0] + rng[1]) / 2
            rng[0 if idx & mask else 1] = mid
            even = not even
    return ((lat[0] + lat[1]) / 2, (lon[0] + lon[1]) / 2)


# Every digit 0-9 is a geohash base32 character, so a number that was never a geohash
# decodes without complaint: a Castellon seed seller's "12520" (a Spanish postal code,
# published as a `g` prefix ladder) decodes 2.4 km from the South Pole. Nothing downstream
# can tell that apart from a real coordinate -- the country then comes from a nearest-city
# lookup that is planar in degrees and has no distance limit, which answers Chile.
# The Antarctic Treaty's parallel is the cheap, nameable cut: the gazetteer holds exactly
# one row south of it (McMurdo Station) and no listing is sold from there.
ANTARCTIC_LAT = -60.0


def implausible_geohash(pt) -> bool:
    """True when a decoded (lat, lon) cannot be where a listing is sold from.

    Callers keep the decode (it is the record of WHY a listing is unplaced) and only
    withhold it from country attribution and from the map.
    """
    return bool(pt) and pt[0] < ANTARCTIC_LAT


ALIAS = {
    "usa": "US", "u.s.a.": "US", "u.s.": "US", "us": "US", "united states": "US",
    "united states of america": "US", "america": "US", "uk": "GB",
    "united kingdom": "GB", "great britain": "GB", "england": "GB",
    "scotland": "GB", "wales": "GB", "northern ireland": "GB",
    "deutschland": "DE", "espana": "ES", "catalunya": "ES", "catalonia": "ES",
    "brasil": "BR", "russia": "RU", "moscow": "RU", "south korea": "KR",
    "czech republic": "CZ", "czechia": "CZ", "holland": "NL",
    "netherlands": "NL", "the netherlands": "NL", "uae": "AE",
    "vietnam": "VN", "viet nam": "VN", "tanzania": "TZ", "iran": "IR",
    "syria": "SY", "laos": "LA", "turkiye": "TR", "suomi": "FI",
    "sverige": "SE", "norge": "NO", "danmark": "DK", "polska": "PL",
    "schweiz": "CH", "svizzera": "CH", "oesterreich": "AT", "italia": "IT",
    "mexico": "MX", "peru": "PE", "south africa": "ZA", "el salvador": "SV",
    "costa rica": "CR", "hong kong": "HK", "new zealand": "NZ", "nz": "NZ",
    "cook islands": "CK", "puerto rico": "PR", "ivory coast": "CI",
    "bolivia": "BO", "venezuela": "VE", "moldova": "MD", "brunei": "BN",
}
NONGEO = {
    "online", "worldwide", "remote", "global", "internet", "digital",
    "anywhere", "everywhere", "n/a", "na", "none", "-", "virtual",
    "world wide", "world", "ships everywhere", "earth", "web", "url",
    "digital download", "online only", "shipping worldwide", "any",
    "digital delivery", "nostr", "cyberspace", "the internet", "online store",
    "welt", "die welt", "ganze welt", "internacional", "international",
}
REGION = {"us & canada", "europe", "eu", "european union", "north america",
          "latin america", "asia", "africa", "south america", "caribbean",
          "middle east", "scandinavia", "benelux", "uk & eu", "usa & canada",
          "worldwide shipping", "eu only", "europa"}
RISKY = {"turkey", "china", "chad", "jersey", "georgia", "mali", "niger",
         "guinea", "grenada", "dominica", "malta", "panama", "cuba", "guam"}

BY_NAME = {}
for _c in pycountry.countries:
    for _a in ("name", "official_name", "common_name"):
        _v = getattr(_c, _a, None)
        if _v:
            BY_NAME[norm(_v)] = _c.alpha_2

US_STATES = set("""alabama alaska arizona arkansas california colorado connecticut delaware
florida hawaii idaho illinois indiana iowa kansas kentucky louisiana maine maryland
massachusetts michigan minnesota mississippi missouri montana nebraska nevada ohio oklahoma
oregon pennsylvania tennessee texas utah vermont virginia washington wisconsin wyoming
new york new jersey new mexico new hampshire north carolina south carolina north dakota
south dakota west virginia rhode island""".split("\n"))
US_STATES = set(x.strip() for line in US_STATES for x in [line] if x.strip())
US_STATES = set(w for line in """alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|
florida|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|maine|maryland|
massachusetts|michigan|minnesota|mississippi|missouri|montana|nebraska|nevada|ohio|oklahoma|
oregon|pennsylvania|tennessee|texas|utah|vermont|virginia|washington|wisconsin|wyoming|
new york|new jersey|new mexico|new hampshire|north carolina|south carolina|north dakota|
south dakota|west virginia|rhode island""".replace("\n", "").split("|") for w in [line.strip()] if w)
US_ABBR = set("""al ak az ar ca co ct de fl hi id il in ia ks ky la me md ma mi mn ms mo mt
ne nv nh nj nm ny nc nd oh ok pa ri sc sd tn tx ut vt va wa wv wi wy dc""".split())

_names = collections.defaultdict(set)
with open(GAZ, newline="") as _fh:
    for _row in csv.DictReader(_fh):
        for _f in ("name", "admin1", "admin2"):
            _v = _row[_f].strip()
            if len(_v) >= 4:
                _names[norm(_v)].add(_row["cc"])
CITY = {k: next(iter(v)) for k, v in _names.items() if len(v) == 1}
AMBIG = {k for k, v in _names.items() if len(v) > 1}
# candidate country sets for the ambiguous names, so a seller's other
# listings can break the tie (see resolve_with_seller_context below)
CANDIDATES = {k: v for k, v in _names.items() if len(v) > 1}

SCAN = sorted(
    ((k, v) for k, v in list(ALIAS.items()) + list(BY_NAME.items())
     if k not in RISKY and len(k) >= 5),
    key=lambda kv: -len(kv[0]))

# country centroid = mean of its gazetteer cities (good enough to place a bubble)
_acc = collections.defaultdict(lambda: [0.0, 0.0, 0])
_cityxy = {}
with open(GAZ, newline="") as _fh:
    for _row in csv.DictReader(_fh):
        _cc, _la, _lo = _row["cc"], float(_row["lat"]), float(_row["lon"])
        a = _acc[_cc]
        a[0] += _la; a[1] += _lo; a[2] += 1
        _n = norm(_row["name"])
        if _n not in _cityxy:
            _cityxy[_n] = (_la, _lo, _cc)
CENTROID = {cc: (a[0] / a[2], a[1] / a[2]) for cc, a in _acc.items() if a[2]}
# a few centroids the "mean of cities" trick places badly
CENTROID.update({"US": (39.5, -98.35), "RU": (57.0, 60.0), "CA": (56.1, -106.3),
                 "FR": (46.6, 2.4), "NL": (52.2, 5.3), "GB": (54.0, -2.5),
                 "NO": (64.5, 12.0), "CL": (-35.7, -71.5), "BR": (-10.3, -53.2),
                 "ID": (-2.5, 118.0), "AU": (-25.3, 133.8), "ZA": (-30.6, 22.9),
                 "PT": (39.6, -8.0), "ES": (40.2, -3.6), "IT": (42.8, 12.6),
                 "HR": (45.35, 15.55), "GR": (39.0, 22.3), "NO": (61.5, 9.0),
                 "SE": (62.2, 15.6), "FI": (63.3, 26.0), "DK": (56.0, 9.5),
                 "VN": (14.1, 108.3), "MY": (3.6, 102.0), "PH": (12.4, 122.0),
                 "IL": (31.4, 34.85)})

ISO3 = {c.alpha_3: c.alpha_2 for c in pycountry.countries}


def text_to_cc(loc):
    """-> ('NONGEO'|'REGION'|'AMBIG'|iso2|None, source)"""
    s = norm(loc)
    if not s:
        return None, None
    if s in NONGEO:
        return "NONGEO", "text"
    if s in REGION:
        return "REGION", "text"
    if s in RISKY:
        # "Georgia", "Turkey", "China" and friends are far more often a US
        # state, a bird or an adjective than the country. A bare risky name is
        # not evidence either way, so report it as AMBIGUOUS rather than as
        # unresolved -- unresolved would let the seller-context pass overwrite
        # it with that seller's modal country, which is a guess dressed up as
        # a fact. AMBIG is a terminal bucket the page reports honestly.
        return "AMBIG", "risky-name"
    if s in ALIAS:
        return ALIAS[s], "text"
    if s in BY_NAME:
        return BY_NAME[s], "text"
    parts = [p.strip() for p in re.split(r"[,·|/()>]|\s+-\s+", s) if p.strip()]
    for p in reversed(parts):
        if p in RISKY:
            continue
        if p in ALIAS:
            return ALIAS[p], "text"
        if p in BY_NAME:
            return BY_NAME[p], "text"
        if p in US_STATES or p in US_ABBR:
            return "US", "text"
    if any(p in NONGEO for p in parts):
        return "NONGEO", "text"
    if any(p in REGION for p in parts):
        return "REGION", "text"
    for name, cc in SCAN:
        if re.search(rf"(^|[^a-z]){re.escape(name)}([^a-z]|$)", s):
            return cc, "country-in-text"
    for p in parts + [s]:
        if p in CITY:
            return CITY[p], "gazetteer"
        if p in AMBIG:
            return "AMBIG", "gazetteer"
    return None, None


def text_to_xy(loc):
    """Best lat/lon for a free-text location: exact city hit, else None."""
    s = norm(loc)
    for p in [q.strip() for q in re.split(r"[,·|/()>]|\s+-\s+", s) if q.strip()] + [s]:
        if p in _cityxy and p not in AMBIG:
            la, lo, cc = _cityxy[p]
            return (la, lo, cc)
    return None


def cc_name(cc):
    c = pycountry.countries.get(alpha_2=cc)
    return c.name if c else cc


def any_country_to_cc(v):
    """Normalise whatever a `country` tag holds: ISO2, ISO3, or a name."""
    s = (v or "").strip()
    if not s:
        return None
    up = s.upper()
    if len(up) == 2 and pycountry.countries.get(alpha_2=up):
        return up
    if len(up) == 3 and up in ISO3:
        return ISO3[up]
    cc, _ = text_to_cc(s)
    return cc if cc and cc not in ("NONGEO", "REGION", "AMBIG") else None


def candidates_for(loc):
    """Country codes an ambiguous free-text location could denote."""
    s = norm(loc)
    for p in [q.strip() for q in re.split(r"[,·|/()>]|\s+-\s+", s) if q.strip()] + [s]:
        if p in CANDIDATES:
            return CANDIDATES[p]
    return set()
