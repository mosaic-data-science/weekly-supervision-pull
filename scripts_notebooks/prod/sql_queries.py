"""
SQL Query Templates for Daily Supervision Pull

This module contains all SQL query templates used by the daily supervision pull script.
"""

# Placeholder values for f-string evaluation 
# These will be replaced with actual dates when .format() is called in pull_data.py
# The f-string evaluates these to literal '{start_date}' strings which .format() can then replace
start_date = '{start_date}'
end_date = '{end_date}'

# SQL query template for direct service data (ServiceCode = '97153')
# Excludes BCBAs from being direct providers.
#
# The Employee join is on EmployeeId = ProviderContactId (same CentralReach contact
# identifier; Employee is 1:1 on it). It previously joined on EmployeeFirstName +
# EmployeeLastName, which fanned out for the 36 duplicated staff names: because this
# is a LEFT JOIN feeding SELECT DISTINCT, one non-BCBA namesake row was enough to let
# a BCBA's own entries through the WHERE filter. Kept as a LEFT JOIN with the
# IS NULL branch below so providers absent from Employee are still treated as direct
# providers rather than silently dropped.
DIRECT_SERVICES_SQL_TEMPLATE = f"""
SELECT DISTINCT
    b.BillingEntryId,
    b.ClientContactId,
    c.ClientFullName,
    c.ClientOfficeLocationName,
    b.ProviderContactId,
    pdir.FirstName AS ProviderFirstName,
    pdir.LastName AS ProviderLastName,
    sc.ServiceCode,
    b.ServiceStartTime,
    b.ServiceEndTime,
    COALESCE(b.ServiceLocationName, '(Unknown)') AS ServiceLocationName
FROM [insights].[dw2].[BillingEntriesCurrent] AS b
INNER JOIN [insights].[insights].[ServiceCode] AS sc
    ON b.ServiceCodeId = sc.ServiceCodeId
INNER JOIN [insights].[insights].[Client] AS c
    ON b.ClientContactId = c.ClientId
LEFT JOIN [insights].[dw2].[Contacts] AS pdir
    ON pdir.ContactId = b.ProviderContactId
LEFT JOIN [insights].[insights].[Employee] AS e
    ON e.EmployeeId = b.ProviderContactId
WHERE b.ServiceEndTime >= '{start_date}'
  AND b.ServiceEndTime <  '{end_date}'
  AND sc.ServiceCode IN ('97153', 'PDS | Technicians')
  AND (e.EmploymentPosition NOT IN ('BCBA', 'Board Certified Behavior Analyst')
       OR e.EmploymentPosition IS NULL)
ORDER BY
    c.ClientFullName,
    b.ServiceStartTime;
"""

# SQL query template for supervision service data (ServiceCode IN ('97155','Non-billable: PM Admin','PDS | BCBA'))
SUPERVISION_SERVICES_SQL_TEMPLATE = f"""
SELECT
    b.BillingEntryId,
    b.ClientContactId,
    c.ClientFullName,
    c.ClientOfficeLocationName,
    b.ProviderContactId,
    psup.FirstName AS ProviderFirstName,
    psup.LastName AS ProviderLastName,
    sc.ServiceCode,
    b.ServiceStartTime,
    b.ServiceEndTime,
    COALESCE(b.ServiceLocationName, '(Unknown)') AS ServiceLocationName
FROM [insights].[dw2].[BillingEntriesCurrent] AS b
INNER JOIN [insights].[insights].[ServiceCode] AS sc
    ON b.ServiceCodeId = sc.ServiceCodeId
INNER JOIN [insights].[insights].[Client] AS c
    ON b.ClientContactId = c.ClientId
LEFT JOIN [insights].[dw2].[Contacts] AS psup
    ON psup.ContactId = b.ProviderContactId
WHERE b.ServiceEndTime >= '{start_date}'
  AND b.ServiceEndTime <  '{end_date}'
  AND sc.ServiceCode IN (
    '97155','Non-billable: PM Admin','PDS | BCBA', '0362T', '0368T', '0373T', 
    'H0032', 'H0032 Program Management Student BCBS PREMERA', 'H2019', 'H2033'
  )
ORDER BY
    c.ClientFullName,
    b.ServiceStartTime;
"""

BACB_SUPERVISION_TEMPLATE = f"""
-- PARAMETERS
DECLARE @StartDate date = '{start_date}';
DECLARE @EndDate   date = '{end_date}';

SELECT
    b.ProviderContactId,
    BACBSupervisionCodes_binary = CAST(1 AS bit),
    BACBSupervisionHours = CAST(SUM(DATEDIFF(MINUTE, b.ServiceStartTime, b.ServiceEndTime)) / 60.0 AS DECIMAL(10,2))
FROM [insights].[dw2].[BillingEntriesCurrent] b
JOIN [insights].[insights].[ServiceCode] sc
  ON sc.ServiceCodeId = b.ServiceCodeId
WHERE b.ServiceEndTime >= @StartDate
  AND b.ServiceEndTime <  @EndDate
  AND b.ProviderContactId IS NOT NULL
  AND (
        sc.ServiceCode LIKE '%BACB%Supervision%Meeting%client%'   -- (w/out client)
     OR sc.ServiceCode LIKE '%VA%Medicaid%Supervision%client%'    -- (w/o Client)
  )
GROUP BY b.ProviderContactId
ORDER BY b.ProviderContactId;
"""

# SQL query template for employee locations (maps provider contact IDs to office
# locations). Scoped to a known set of ContactIds via the {provider_ids} placeholder
# (a comma-separated list built from the direct/supervision/BACB pulls) so the join
# never runs as a full-table scan. ORDER BY is dropped since downstream pandas code
# ignores row order.
#
# Joined on Provider.ProviderId = Contacts.ContactId -- these are the same
# CentralReach contact identifier. An earlier version joined on
# FirstName + LastName, which returned MULTIPLE rows for any provider sharing a name
# with another provider record (e.g. two distinct "Emma Smith" employees, or one
# active + one deactivated record). merge_data.add_work_locations_from_sql builds a
# ProviderContactId -> WorkLocation dict, so a duplicated ID was resolved
# last-row-wins and could silently place a BT on the wrong clinic tab. Verified over
# the current provider set: the ID join matches all the same contacts (791/791) with
# zero name mismatches and no duplicate IDs.
EMPLOYEE_LOCATIONS_SQL_TEMPLATE = """
SELECT
    c.ContactId AS ProviderContactId,
    c.FirstName AS ProviderFirstName,
    c.LastName AS ProviderLastName,
    p.ProviderOfficeLocationName AS WorkLocation
FROM [insights].[dw2].[Contacts] AS c
INNER JOIN [insights].[insights].[Provider] AS p
    ON p.ProviderId = c.ContactId
WHERE c.ContactId IN ({provider_ids});
"""

# Cheap freshness check against the source tables used by EMPLOYEE_LOCATIONS_SQL_TEMPLATE.
# Returns the MAX row-modified timestamp on each table so the pipeline can skip the
# expensive employee-locations pull when nothing has changed since the cache was built.
EMPLOYEE_LOCATIONS_FRESHNESS_SQL = """
SELECT
    (SELECT MAX(RowModifiedAt)  FROM [insights].[insights].[Provider]) AS provider_row_modified_at,
    (SELECT MAX(LastLoadedDate) FROM [insights].[dw2].[Contacts])      AS contacts_last_loaded_date;
"""

# SQL query template for each BT's recent service-delivery locations. For direct
# service (97153 / PDS | Technicians) over a recent lookback window, aggregates the
# number of sessions and the most-recent session date per (provider, client office
# location). Downstream code (location_review.build_location_review) compares each
# provider's dominant service location against their CentralReach profile Office
# Location (WorkLocation) to flag likely un-updated clinic transfers.
#
# Scoped to the same {provider_ids} set as the employee-locations pull so the flag
# covers exactly the providers on the tracker and stays cheap. It is an aggregate
# (GROUP BY), so the returned payload is one row per provider+location, not per session.
RECENT_SERVICE_LOCATION_SQL_TEMPLATE = """
SELECT
    b.ProviderContactId,
    c.ClientOfficeLocationName AS ServiceLocation,
    COUNT(*)               AS Sessions,
    MAX(b.ServiceEndTime)  AS LastServiceDate
FROM [insights].[dw2].[BillingEntriesCurrent] AS b
INNER JOIN [insights].[insights].[ServiceCode] AS sc
    ON b.ServiceCodeId = sc.ServiceCodeId
INNER JOIN [insights].[insights].[Client] AS c
    ON b.ClientContactId = c.ClientId
WHERE b.ServiceEndTime >= '{lookback_start}'
  AND b.ServiceEndTime <  '{end_date}'
  AND sc.ServiceCode IN ('97153', 'PDS | Technicians')
  AND b.ProviderContactId IN ({provider_ids})
GROUP BY b.ProviderContactId, c.ClientOfficeLocationName;
"""