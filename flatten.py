"""Flatten SGregister enterprise envelopes into firm + area tables.

Two LUAS, two output tables:

* **Firm LUAS = orgnr.** One row per legal entity. Carries identity,
  contact, address, approval period, terms.
* **Area LUAS = (orgnr, function_xml, subject_area_xml).** Many rows
  per firm, one per approval scope. Carries grade (the deterioration
  signal), plus pbl/pbl_xml/function/subject_area as attributes
  (pbl is empirically constant at "PBL 2016" in April 2026 but
  tracked so any future PBL revision surfaces as a regular attribute
  change rather than silently overwriting).

Area-row dedup
--------------
Empirically ~1% of firms return exact-duplicate area rows in
``valid_approval_areas[]`` (same function_xml, same subject_area_xml,
same pbl, same grade — data-entry artefact on the DiBK side). This
module dedupes on ``(function_xml, subject_area_xml)``; if two rows
with the same LUAS disagree on any field, the first occurrence wins
and the conflict count is surfaced via ``dup_conflicts``.
"""

import hashlib
import json


def content_hash(obj):
    """Truncated SHA-256 over canonical JSON (sorted keys, unicode-safe)."""
    canonical = json.dumps(obj, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def flatten_firm(record):
    """Flatten firm-level fields from an SGregister envelope.

    Parameters
    ----------
    record : dict
        Raw envelope ``{"dibk-sgdata": {...}}`` as returned by
        ``/api/enterprises/{orgnr}``.

    Returns
    -------
    dict
        Flat dict with 26 attribute keys plus ``content_hash`` (over
        attribute keys only — area-level changes do not bump this
        hash).
    """
    data = record.get("dibk-sgdata") or {}
    ent = data.get("enterprise") or {}
    biz = ent.get("businessaddress") or {}
    post = ent.get("postaladdress") or {}
    status = data.get("status") or {}
    terms = data.get("additional_terms") or {}
    areas = data.get("valid_approval_areas") or []

    firm = {
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
    }
    firm["content_hash"] = content_hash({k: v for k, v in firm.items()})
    return firm


def flatten_areas(record):
    """Flatten area rows, deduped on (function_xml, subject_area_xml).

    Parameters
    ----------
    record : dict
        Raw envelope ``{"dibk-sgdata": {...}}``.

    Returns
    -------
    tuple
        ``(rows, dup_conflicts)`` where ``rows`` is a list of deduped
        flat area dicts and ``dup_conflicts`` is the number of
        duplicate keys where fields differed between rows collapsed
        into one. A non-zero conflict count is worth logging.
    """
    data = record.get("dibk-sgdata") or {}
    ent = data.get("enterprise") or {}
    orgnr = ent.get("organizational_number")
    areas = data.get("valid_approval_areas") or []

    seen = {}
    conflicts = 0
    for a in areas:
        key = (a.get("function_xml"), a.get("subject_area_xml"))
        row = {
            "orgnr": orgnr,
            "function_xml": a.get("function_xml"),
            "subject_area_xml": a.get("subject_area_xml"),
            "function": a.get("function"),
            "subject_area": a.get("subject_area"),
            "pbl": a.get("pbl"),
            "pbl_xml": a.get("pbl_xml"),
            "grade": a.get("grade"),
        }
        row["content_hash"] = content_hash(row)
        if key in seen:
            if seen[key]["content_hash"] != row["content_hash"]:
                conflicts += 1
            continue
        seen[key] = row
    return list(seen.values()), conflicts


def flatten(record):
    """Convenience wrapper returning firm + area rows + dup conflict count."""
    firm = flatten_firm(record)
    areas, conflicts = flatten_areas(record)
    return firm, areas, conflicts
