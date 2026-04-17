"""Flatten SGregister enterprise envelopes into parquet-friendly rows.

Each record from ``/api/enterprises/{orgnr}`` is a single-object
envelope ``{"dibk-sgdata": {...}}`` with four children: ``enterprise``,
``status``, ``additional_terms``, ``valid_approval_areas[]``.

This module produces:

* :func:`flatten_enterprise` — flat dict with 30 scalar columns plus
  a JSON-serialised ``approval_areas_json`` and a sorted list of
  canonical ``approval_area_codes``. Suitable for one row in
  ``snapshots.parquet``.
* :func:`content_hash` — deterministic SHA-256[:16] over the
  canonical JSON of the full envelope. Used for change detection.

The "canonical approval area code" is the string
``"{function_xml}-{subject_area_xml}-{grade}"``. Sorting this list
gives a stable representation that reveals area additions, removals,
and grade changes without descending into the nested objects.
"""

import hashlib
import json


def content_hash(record):
    """Compute a truncated SHA-256 hash of an SGregister envelope.

    Parameters
    ----------
    record : dict
        Raw envelope as returned by the API: ``{"dibk-sgdata": {...}}``.

    Returns
    -------
    str
        First 16 hexadecimal characters of the SHA-256 digest of the
        canonical JSON serialisation (sorted keys, no ASCII escaping).
    """
    canonical = json.dumps(record, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _area_code(area):
    """Canonical ``"function_xml-subject_area_xml-grade"`` code.

    Missing parts render as empty strings.
    """
    return "{}-{}-{}".format(
        area.get("function_xml") or "",
        area.get("subject_area_xml") or "",
        area.get("grade") or "",
    )


def flatten_enterprise(record):
    """Flatten an SGregister envelope into a flat dict.

    Parameters
    ----------
    record : dict
        Raw envelope ``{"dibk-sgdata": {...}}`` as returned by
        ``/api/enterprises/{orgnr}``.

    Returns
    -------
    dict
        Flat dict with 30 scalar keys plus ``approval_areas_json``
        (full nested list as JSON string) and ``approval_area_codes``
        (sorted list of canonical codes).
    """
    data = record.get("dibk-sgdata") or {}
    ent = data.get("enterprise") or {}
    biz = ent.get("businessaddress") or {}
    post = ent.get("postaladdress") or {}
    status = data.get("status") or {}
    terms = data.get("additional_terms") or {}
    areas = data.get("valid_approval_areas") or []

    codes = sorted({_area_code(a) for a in areas})

    return {
        "orgnr": ent.get("organizational_number"),
        "name": ent.get("name"),
        "phone": ent.get("phone"),
        "email": ent.get("email"),
        "www": ent.get("www"),
        "biz_addr_line_1": biz.get("line_1"),
        "biz_addr_line_2": biz.get("line_2"),
        "biz_addr_line_3": biz.get("line_3"),
        "biz_addr_line_4": biz.get("line_4"),
        "biz_addr_country": biz.get("country"),
        "biz_addr_postal_code": biz.get("postal_code"),
        "biz_addr_postal_town": biz.get("postal_town"),
        "post_addr_line_1": post.get("line_1"),
        "post_addr_line_2": post.get("line_2"),
        "post_addr_line_3": post.get("line_3"),
        "post_addr_line_4": post.get("line_4"),
        "post_addr_country": post.get("country"),
        "post_addr_postal_code": post.get("postal_code"),
        "post_addr_postal_town": post.get("postal_town"),
        "approved": status.get("approved"),
        "approval_period_to": status.get("approval_period_to"),
        "approval_certificate_url": status.get("approval_certificate"),
        "liability_insurance": terms.get("liability_insurance"),
        "industrial_injury_insurance": terms.get("industrial_injury_insurance"),
        "educational_enterprise_approved": terms.get("educational_enterprise_approved"),
        "n_approval_areas": len(areas),
        "approval_areas_json": json.dumps(areas, ensure_ascii=False, sort_keys=True),
        "approval_area_codes": codes,
    }
