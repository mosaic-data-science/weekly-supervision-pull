"""
SQL Query Templates for Weekly Supervision Pull

This module contains all SQL query templates used by the weekly supervision pull script.
"""

# SQL query template for supervision hours data
SUPERVISION_HOURS_SQL_TEMPLATE = """
WITH base AS (
    SELECT
        b.BillingEntryId,  -- if available
        b.ClientContactId,
        c.ClientFullName,
        b.ProviderContactId,
        sc.ServiceCode,
        b.ServiceStartTime,
        b.ServiceEndTime,
        COALESCE(b.ServiceLocationName, '(Unknown)') AS ServiceLocationName
    FROM [insights].[dw2].[BillingEntriesCurrent] AS b
    INNER JOIN [insights].[insights].[ServiceCode] AS sc
        ON b.ServiceCodeId = sc.ServiceCodeId
    INNER JOIN [insights].[insights].[Client] AS c
        ON b.ClientContactId = c.ClientId
    WHERE b.ServiceEndTime >= '{start_date}'
      AND b.ServiceEndTime <  '{end_date}'  -- includes all of end_date
      AND sc.ServiceCode IN ('97155','97153','Non-billable: PM Admin','PDS | BCBA')
),
direct AS (
    SELECT
        ClientContactId, ClientFullName, ProviderContactId,
        ServiceStartTime, ServiceEndTime, ServiceLocationName
    FROM base
    WHERE ServiceCode = '97153'
),
supervision AS (
    SELECT
        ClientContactId, ClientFullName, ProviderContactId,
        ServiceStartTime, ServiceEndTime, ServiceLocationName
    FROM base
    WHERE ServiceCode IN ('97155','Non-billable: PM Admin','PDS | BCBA')
),

-- Overlap between each direct entry and any supervision entry for the same client
overlap_raw AS (
    SELECT
        d.ClientContactId,
        d.ClientFullName,
        d.ProviderContactId AS DirectProviderId,
        s.ProviderContactId AS SupervisorProviderId,
        d.ServiceLocationName AS DirectServiceLocationName,
        s.ServiceLocationName AS SupervisorServiceLocationName,
        CASE
            WHEN d.ServiceStartTime >= s.ServiceStartTime AND d.ServiceEndTime <= s.ServiceEndTime
                THEN DATEDIFF(MINUTE, d.ServiceStartTime, d.ServiceEndTime) / 60.0
            WHEN d.ServiceStartTime < s.ServiceStartTime AND d.ServiceEndTime > s.ServiceEndTime
                THEN DATEDIFF(MINUTE, s.ServiceStartTime, s.ServiceEndTime) / 60.0
            WHEN d.ServiceStartTime < s.ServiceStartTime
                THEN DATEDIFF(MINUTE, s.ServiceStartTime, d.ServiceEndTime) / 60.0
            ELSE DATEDIFF(MINUTE, d.ServiceStartTime, s.ServiceEndTime) / 60.0
        END AS OverlapHours
    FROM direct d
    INNER JOIN supervision s
        ON s.ClientContactId = d.ClientContactId
       AND d.ServiceStartTime < s.ServiceEndTime
       AND d.ServiceEndTime   > s.ServiceStartTime
),

-- Summed overlap by client, direct–supervisor pair, and locations
overlap AS (
    SELECT
        ClientContactId,
        ClientFullName,
        DirectProviderId,
        SupervisorProviderId,
        DirectServiceLocationName,
        SupervisorServiceLocationName,
        CAST(SUM(OverlapHours) AS DECIMAL(10,2)) AS OverlapHours
    FROM overlap_raw
    WHERE OverlapHours > 0
    GROUP BY
        ClientContactId, ClientFullName,
        DirectProviderId, SupervisorProviderId,
        DirectServiceLocationName, SupervisorServiceLocationName
),

-- Total direct hours per client, direct provider, and direct location
direct_totals AS (
    SELECT
        d.ClientContactId,
        d.ClientFullName,
        d.ProviderContactId AS DirectProviderId,
        d.ServiceLocationName AS DirectServiceLocationName,
        SUM(DATEDIFF(MINUTE, d.ServiceStartTime, d.ServiceEndTime)) / 60.0 AS DirectHours_Total
    FROM direct d
    GROUP BY d.ClientContactId, d.ClientFullName, d.ProviderContactId, d.ServiceLocationName
),

-- Total overlap hours per client, direct provider, and direct location (across all supervisors)
overlap_by_direct AS (
    SELECT
        ClientContactId,
        DirectProviderId,
        DirectServiceLocationName,
        SUM(OverlapHours) AS OverlapHours_Total
    FROM overlap
    GROUP BY ClientContactId, DirectProviderId, DirectServiceLocationName
),

-- Direct-only = total direct minus overlapped (by direct location)
direct_only AS (
    SELECT
        dt.ClientContactId,
        dt.ClientFullName,
        dt.DirectProviderId,
        dt.DirectServiceLocationName,
        CAST(dt.DirectHours_Total - COALESCE(od.OverlapHours_Total, 0.0) AS DECIMAL(10,2)) AS DirectHours_NoSupervision
    FROM direct_totals dt
    LEFT JOIN overlap_by_direct od
      ON od.ClientContactId = dt.ClientContactId
     AND od.DirectProviderId = dt.DirectProviderId
     AND od.DirectServiceLocationName = dt.DirectServiceLocationName
),

-- ===== NEW: bring back supervision that does NOT overlap any direct =====

-- Total supervision hours per (client, supervisor, supervisor location)
supervision_totals AS (
    SELECT
        s.ClientContactId,
        s.ClientFullName,
        s.ProviderContactId AS SupervisorProviderId,
        s.ServiceLocationName AS SupervisorServiceLocationName,
        SUM(DATEDIFF(MINUTE, s.ServiceStartTime, s.ServiceEndTime)) / 60.0 AS SupervisionHours_Total
    FROM supervision s
    GROUP BY s.ClientContactId, s.ClientFullName, s.ProviderContactId, s.ServiceLocationName
),

-- Overlap attributed to each supervisor/location
overlap_by_supervision AS (
    SELECT
        ClientContactId,
        SupervisorProviderId,
        SupervisorServiceLocationName,
        SUM(OverlapHours) AS OverlapHours_Total
    FROM overlap
    GROUP BY ClientContactId, SupervisorProviderId, SupervisorServiceLocationName
),

-- Non-overlapped supervision only
supervision_only AS (
    SELECT
        st.ClientContactId,
        st.ClientFullName,
        st.SupervisorProviderId,
        st.SupervisorServiceLocationName,
        CAST(st.SupervisionHours_Total - COALESCE(os.OverlapHours_Total, 0.0) AS DECIMAL(10,2)) AS SupervisionHours_NoDirect
    FROM supervision_totals st
    LEFT JOIN overlap_by_supervision os
      ON os.ClientContactId = st.ClientContactId
     AND os.SupervisorProviderId = st.SupervisorProviderId
     AND os.SupervisorServiceLocationName = st.SupervisorServiceLocationName
    WHERE (st.SupervisionHours_Total - COALESCE(os.OverlapHours_Total, 0.0)) > 0
),

-- Labels using your exact column layout
named_direct_only AS (
    SELECT
        do.ClientContactId,
        do.ClientFullName,
        do.DirectProviderId,
        pdir.FirstName AS DirectFirstName,
        pdir.LastName  AS DirectLastName,
        do.DirectServiceLocationName,
        do.DirectHours_NoSupervision AS DirectHours,
        CAST(0.0 AS DECIMAL(10,2)) AS SupervisionHours,
        NULL AS SupervisorFirstName,
        NULL AS SupervisorLastName,
        NULL AS SupervisorServiceLocationName,
        'Direct (no supervision overlap)' AS RowType
    FROM direct_only do
    LEFT JOIN [insights].[dw2].[Contacts] pdir
      ON pdir.ContactId = do.DirectProviderId
),

named_overlap AS (
    SELECT
        o.ClientContactId,
        o.ClientFullName,
        o.DirectProviderId,
        pdir.FirstName AS DirectFirstName,
        pdir.LastName  AS DirectLastName,
        o.DirectServiceLocationName,
        o.OverlapHours AS DirectHours,
        o.OverlapHours AS SupervisionHours,
        psup.FirstName AS SupervisorFirstName,
        psup.LastName  AS SupervisorLastName,
        o.SupervisorServiceLocationName,
        'Direct overlapped with supervision' AS RowType
    FROM overlap o
    LEFT JOIN [insights].[dw2].[Contacts] pdir
      ON pdir.ContactId = o.DirectProviderId
    LEFT JOIN [insights].[dw2].[Contacts] psup
      ON psup.ContactId = o.SupervisorProviderId
),

named_supervision_only AS (
    SELECT
        so.ClientContactId,
        so.ClientFullName,
        NULL AS DirectProviderId,
        NULL AS DirectFirstName,
        NULL AS DirectLastName,
        NULL AS DirectServiceLocationName,
        CAST(0.0 AS DECIMAL(10,2)) AS DirectHours,
        so.SupervisionHours_NoDirect AS SupervisionHours,
        psup.FirstName AS SupervisorFirstName,
        psup.LastName  AS SupervisorLastName,
        so.SupervisorServiceLocationName,
        'Supervision without direct overlap' AS RowType
    FROM supervision_only so
    LEFT JOIN [insights].[dw2].[Contacts] psup
      ON psup.ContactId = so.SupervisorProviderId
)

SELECT
    x.ClientContactId,
    x.ClientFullName,
    x.DirectProviderId,
    x.DirectFirstName,
    x.DirectLastName,
    x.DirectServiceLocationName,
    x.DirectHours,
    x.SupervisionHours,
    x.SupervisorFirstName,
    x.SupervisorLastName,
    x.SupervisorServiceLocationName,
    x.RowType
FROM (
    SELECT * FROM named_direct_only
    UNION ALL
    SELECT * FROM named_overlap
    UNION ALL
    SELECT * FROM named_supervision_only
) x
WHERE (x.DirectHours > 0 OR x.SupervisionHours > 0)
ORDER BY
    x.ClientFullName,
    x.DirectLastName, x.DirectFirstName,
    x.SupervisorLastName, x.SupervisorFirstName,
    x.DirectServiceLocationName,
    x.SupervisorServiceLocationName;
"""
