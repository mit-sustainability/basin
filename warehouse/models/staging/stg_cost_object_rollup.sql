WITH cleaned AS (
    SELECT
        CAST(
            NULLIF(REGEXP_REPLACE(cost_collector_id, '\D', '', 'g'), '') AS INTEGER
        ) AS cost_collector_id,
        dlc_name,
        school_area
    FROM {{ source('raw', 'cost_object_warehouse') }}
    WHERE (school_area IS NOT NULL) AND (dlc_name != 'Defunct units')
)

SELECT
    cost_collector_id AS cost_object,
    dlc_name,
    CASE
        WHEN school_area = 'VP Research' THEN 'Interdisciplinary Research Initiatives'
        WHEN school_area IN (
            'Associate Provost for the Arts',
            'Chancellor''s Area',
            'Dean for Undergraduate Education',
            'Executive Vice President''s Area',
            'Facilities Area',
            'HR Area',
            'Libraries, Tech Review, and MIT Press',
            'Office of the Provost Area',
            'Office of the Vice Chancellor''s Area',
            'Outside organizations affiliated with MIT',
            'President''s area',
            'Senior Council and Environmental Programs',
            'Treasurer''s Area',
            'VP Finance Area'
        ) THEN 'MIT Administration'
        ELSE school_area
    END AS school_area
FROM cleaned
