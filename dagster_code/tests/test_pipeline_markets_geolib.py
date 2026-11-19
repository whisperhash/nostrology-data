"""Unit tests for pipeline/markets_geolib - the marketplaces geo resolution.

The module is data-shaped: pycountry supplies the country-name tables and reverse_geocoder
the GeoNames cities1000 gazetteer, both version-pinned in requirements.txt. These tests pin
the RESOLUTION RULES (holdouts, aliases, precedence, terminal buckets) rather than the data,
so a deliberate version bump that shifts a city set fails the agreement gate, not this file.
"""
import math

import pytest

from dagster_code.pipeline import markets_geolib as G


# --- geohash --------------------------------------------------------------------------------

def test_geohash_decode_known_point():
    lat, lon = G.geohash_decode("u4pruydqqvj")  # the canonical wiki example, ~57.649N 10.407E
    assert math.isclose(lat, 57.649, abs_tol=0.01)
    assert math.isclose(lon, 10.407, abs_tol=0.01)


def test_geohash_decode_rejects_invalid():
    assert G.geohash_decode("") is None
    assert G.geohash_decode(None) is None
    assert G.geohash_decode("ab!c") is None
    assert G.geohash_decode("aio")  is None  # a, i, o are not base32 geohash chars
    assert G.geohash_decode("  u4pruy  ") is not None  # surrounding whitespace tolerated


def test_implausible_geohash_boundary():
    assert G.ANTARCTIC_LAT == -60.0
    assert G.implausible_geohash(None) is False
    assert G.implausible_geohash((-60.0, 0.0)) is False       # the parallel itself is kept
    assert G.implausible_geohash((-60.0001, 0.0)) is True
    assert G.implausible_geohash((52.5, 13.4)) is False


def test_implausible_geohash_catches_a_postal_code_published_as_a_geohash():
    """The defect the rule exists for. Every digit 0-9 is a geohash base32 character, so a
    postal code DECODES rather than failing: "12520" is Nules, Castellon, and as a geohash
    it is 2.4 km from the South Pole. The decoder is right; the input was never a geohash."""
    pt = G.geohash_decode("12520")
    assert pt is not None
    assert pt[0] < -89.9
    assert G.implausible_geohash(pt) is True


def test_implausible_geohash_rejects_the_one_real_place_south_of_the_line():
    """The rule's cost, stated rather than discovered later: the gazetteer holds exactly one
    row south of the parallel (McMurdo Station) and this rejects it too. Nothing is sold
    from there, which is why the cut is worth its cost."""
    assert G.implausible_geohash(G.geohash_decode("pdnt0")) is True


def test_only_one_gazetteer_row_lies_south_of_the_cut():
    """The cut's justification is a fact about version-pinned DATA, not about code: the
    gazetteer holds exactly one row below the parallel. A reverse_geocoder bump that added
    an inhabited place down there would make the rule cost more than it is worth, and the
    comment in markets_geolib and the design doc would both quietly become wrong."""
    import csv
    with open(G.GAZ, newline="") as fh:
        south = [r for r in csv.DictReader(fh) if float(r["lat"]) < G.ANTARCTIC_LAT]
    assert [(r["name"], r["cc"]) for r in south] == [("McMurdo Station", "AQ")]


def test_implausible_geohash_keeps_inhabited_far_south():
    # Ushuaia, the southernmost city of any size, sits 5 degrees north of the cut.
    assert G.implausible_geohash((-54.8, -68.3)) is False


# --- text_to_cc -----------------------------------------------------------------------------

def test_text_to_cc_terminal_buckets():
    assert G.text_to_cc("Online") == ("NONGEO", "text")
    assert G.text_to_cc("worldwide") == ("NONGEO", "text")
    assert G.text_to_cc("Europe") == ("REGION", "text")
    assert G.text_to_cc("") == (None, None)
    assert G.text_to_cc(None) == (None, None)


def test_text_to_cc_risky_names_are_ambiguous_not_resolved():
    # "Georgia" is a US state far more often than the country; a bare risky name must land
    # in the terminal AMBIG bucket (NOT None - None would let seller-context overwrite it).
    for risky in ("Georgia", "Turkey", "China", "Jersey", "Chad"):
        assert G.text_to_cc(risky) == ("AMBIG", "risky-name")


def test_text_to_cc_aliases_and_names():
    assert G.text_to_cc("USA") == ("US", "text")
    assert G.text_to_cc("Deutschland") == ("DE", "text")
    assert G.text_to_cc("UK") == ("GB", "text")
    assert G.text_to_cc("Brasil") == ("BR", "text")
    assert G.text_to_cc("Portugal") == ("PT", "text")


def test_text_to_cc_comma_parts_and_us_states():
    assert G.text_to_cc("Austin, Texas") == ("US", "text")
    assert G.text_to_cc("Miami, FL")[0] == "US"
    # The last part wins when several resolve (the country usually trails the city).
    assert G.text_to_cc("Lisbon, Portugal") == ("PT", "text")


def test_text_to_cc_country_in_text_scan_skips_risky():
    assert G.text_to_cc("shipping from portugal only")[0] == "PT"
    # A risky name inside prose must NOT resolve via the substring scan.
    cc, _src = G.text_to_cc("fresh turkey sandwiches")
    assert cc != "TR"


def test_text_to_cc_gazetteer_unique_vs_ambiguous():
    cc, src = G.text_to_cc("Reykjavik")
    assert (cc, src) == ("IS", "gazetteer")
    # A city name existing in many countries is terminal-AMBIG at this stage; the
    # seller-context pass may later break the tie via candidates_for.
    cc, _ = G.text_to_cc("Springfield")
    assert cc == "AMBIG"
    assert len(G.candidates_for("Springfield")) > 1


# --- any_country_to_cc (shipping `country` tag values) --------------------------------------

def test_any_country_to_cc_iso2_iso3_and_names():
    assert G.any_country_to_cc("US") == "US"
    assert G.any_country_to_cc("usa") == "US"
    assert G.any_country_to_cc("DEU") == "DE"
    assert G.any_country_to_cc("Portugal") == "PT"
    assert G.any_country_to_cc("") is None
    assert G.any_country_to_cc(None) is None
    assert G.any_country_to_cc("Worldwide") is None   # NONGEO never becomes a country
    assert G.any_country_to_cc("Europe") is None      # REGION neither


def test_any_country_to_cc_unassigned_iso2_is_not_a_country():
    # Two uppercase letters alone are not enough - 'XX' is not assigned.
    assert G.any_country_to_cc("XX") is None


# --- centroids / names ----------------------------------------------------------------------

def test_centroid_overrides_present_and_sane():
    # The hand-corrected centroids (mean-of-cities places these badly).
    for cc, (lat_lo, lat_hi) in {"US": (30, 50), "HR": (44, 47), "VN": (10, 20), "IL": (29, 34)}.items():
        lat, lon = G.CENTROID[cc]
        assert lat_lo <= lat <= lat_hi, (cc, lat)
        assert -180 <= lon <= 180


def test_cc_name_falls_back_to_code():
    assert G.cc_name("US") == "United States"
    assert G.cc_name("ZZ") == "ZZ"


# --- norm -----------------------------------------------------------------------------------

def test_norm_folds_diacritics_case_and_spaces():
    assert G.norm("  Wrocław  ") == "wrocaw" or G.norm("  Wrocław  ") == "wroclaw"
    assert G.norm("SÃO PAULO") == "sao paulo"
    assert G.norm("A  B") == "a b"
    assert G.norm(None) == ""


def test_text_to_xy_exact_city_only():
    hit = G.text_to_xy("Reykjavik")
    assert hit is not None and hit[2] == "IS"
    assert G.text_to_xy("nowhere-that-exists-xyz") is None
    # Ambiguous names never place a point.
    assert G.text_to_xy("Springfield") is None
