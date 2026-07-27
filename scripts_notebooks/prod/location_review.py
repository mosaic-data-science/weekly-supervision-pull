#!/usr/bin/env python3
"""
Location Review flag

The daily tracker places each BT on the clinic tab named by their CentralReach
provider-profile Office Location field (Provider.ProviderOfficeLocationName, carried
through the pipeline as WorkLocation). That field is maintained by hand, so when a BT
transfers clinics but the profile is not updated, they keep showing on their old
clinic's roster even though all their sessions are now delivered elsewhere.

This module does NOT change how BTs are assigned to tabs. It keeps the CentralReach
Office Location as the source of truth and instead builds a review list: for each BT it
compares their profile Office Location against the clinic where they have actually been
delivering the most sessions over a recent lookback window, and flags the ones that
disagree so business can update the stale CentralReach profiles.
"""

import logging

import pandas as pd

from transform_data import clean_clinic_name


# A BT is flagged only when the recent service signal is strong enough to be a real
# transfer rather than incidental cross-coverage at another clinic.
DEFAULT_MIN_SESSIONS = 8        # ignore BTs with very little recent direct-service activity
DEFAULT_DOMINANT_SHARE = 0.60   # their busiest clinic must be >=60% of recent sessions


def _norm(value) -> str:
    """Normalize a clinic name for comparison only (display keeps the original)."""
    if pd.isna(value):
        return ''
    cleaned = clean_clinic_name(value)
    if pd.isna(cleaned):
        return ''
    return str(cleaned).strip().casefold()


def build_location_review(
    employee_locations_df: pd.DataFrame,
    recent_service_df: pd.DataFrame,
    logger: logging.Logger = None,
    min_sessions: int = DEFAULT_MIN_SESSIONS,
    dominant_share: float = DEFAULT_DOMINANT_SHARE,
) -> pd.DataFrame:
    """
    Build the Location Review flag list.

    Args:
        employee_locations_df: Employee locations from SQL (ProviderContactId,
            ProviderFirstName, ProviderLastName, WorkLocation = profile Office Location).
        recent_service_df: Recent service locations from SQL (ProviderContactId,
            ServiceLocation, Sessions, LastServiceDate) aggregated per provider+location.
        logger: Logger instance (optional).
        min_sessions: Minimum recent direct-service sessions for a BT to be considered.
        dominant_share: Minimum share of recent sessions at the busiest clinic.

    Returns:
        pd.DataFrame: One row per flagged BT (may be empty), with the columns:
            ProviderName, ProfileOfficeLocation (CentralReach), RecentServiceLocation,
            SessionsAtRecentLocation, ShareAtRecentLocation, TotalRecentSessions,
            LastServiceDate.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    review_columns = [
        'ProviderName',
        'ProfileOfficeLocation (CentralReach)',
        'RecentServiceLocation',
        'SessionsAtRecentLocation',
        'ShareAtRecentLocation',
        'TotalRecentSessions',
        'LastServiceDate',
    ]

    if recent_service_df is None or len(recent_service_df) == 0:
        logger.info("Location review: no recent service data available, skipping flag")
        return pd.DataFrame(columns=review_columns)
    if employee_locations_df is None or len(employee_locations_df) == 0:
        logger.info("Location review: no employee locations available, skipping flag")
        return pd.DataFrame(columns=review_columns)

    svc = recent_service_df.copy()
    svc = svc[svc['ProviderContactId'].notna() & svc['ServiceLocation'].notna()]
    svc['Sessions'] = pd.to_numeric(svc['Sessions'], errors='coerce').fillna(0).astype(int)
    svc['LastServiceDate'] = pd.to_datetime(svc['LastServiceDate'], errors='coerce')

    # Total recent sessions per provider, and the busiest (dominant) clinic per provider.
    totals = svc.groupby('ProviderContactId', as_index=False)['Sessions'].sum()
    totals = totals.rename(columns={'Sessions': 'TotalRecentSessions'})

    # Keep the whole busiest row per provider (drop_duplicates keeps the first row
    # intact, unlike groupby().first() which takes the first non-null value per column
    # independently and could pull LastServiceDate from a different location's row).
    dominant = (
        svc.sort_values(['ProviderContactId', 'Sessions', 'LastServiceDate'],
                        ascending=[True, False, False])
           .drop_duplicates(subset='ProviderContactId', keep='first')
           .reset_index(drop=True)
    )
    dominant = dominant.merge(totals, on='ProviderContactId', how='left')

    # Profile Office Location per provider (one row per ProviderContactId).
    profile = employee_locations_df.copy()
    profile = profile[profile['ProviderContactId'].notna()]
    profile = profile.drop_duplicates(subset=['ProviderContactId'], keep='first')

    merged = dominant.merge(profile, on='ProviderContactId', how='inner')
    if len(merged) == 0:
        logger.info("Location review: no providers common to service data and profiles")
        return pd.DataFrame(columns=review_columns)

    merged['share'] = merged['Sessions'] / merged['TotalRecentSessions'].where(
        merged['TotalRecentSessions'] > 0, other=pd.NA
    )

    def _is_flagged(row) -> bool:
        profile_loc = row.get('WorkLocation')
        # No profile location on file: the pipeline already falls back to service
        # location for these, so there is nothing stale to flag.
        if pd.isna(profile_loc) or str(profile_loc).strip() == '':
            return False
        if row['TotalRecentSessions'] < min_sessions:
            return False
        if pd.isna(row['share']) or row['share'] < dominant_share:
            return False
        return _norm(row['ServiceLocation']) != _norm(profile_loc)

    merged['flagged'] = merged.apply(_is_flagged, axis=1)
    flagged = merged[merged['flagged']].copy()

    if len(flagged) == 0:
        logger.info("Location review: no BTs flagged (all profile locations match recent service)")
        return pd.DataFrame(columns=review_columns)

    def _full_name(row) -> str:
        first = '' if pd.isna(row.get('ProviderFirstName')) else str(row.get('ProviderFirstName')).strip()
        last = '' if pd.isna(row.get('ProviderLastName')) else str(row.get('ProviderLastName')).strip()
        return f"{first} {last}".strip()

    out = pd.DataFrame({
        'ProviderName': flagged.apply(_full_name, axis=1),
        'ProfileOfficeLocation (CentralReach)': flagged['WorkLocation'].apply(clean_clinic_name),
        'RecentServiceLocation': flagged['ServiceLocation'].apply(clean_clinic_name),
        'SessionsAtRecentLocation': flagged['Sessions'].astype(int),
        'ShareAtRecentLocation': (flagged['share'] * 100).round(0).astype(int).astype(str) + '%',
        'TotalRecentSessions': flagged['TotalRecentSessions'].astype(int),
        'LastServiceDate': flagged['LastServiceDate'].dt.strftime('%Y-%m-%d'),
    })

    # Most-transferred-looking first: highest share, then most sessions.
    out['_share_num'] = flagged['share'].values
    out = out.sort_values(
        ['_share_num', 'SessionsAtRecentLocation'], ascending=[False, False]
    ).drop(columns='_share_num').reset_index(drop=True)

    logger.info(f"Location review: flagged {len(out)} BT(s) whose profile clinic differs from recent service clinic")
    return out
