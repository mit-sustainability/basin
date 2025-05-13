SELECT
    "event",
    "event_date",
    COALESCE(department_name, 'external') AS department_name,
    COUNT(*) AS attendee_count
FROM {{ ref('stg_attendance_records') }}
GROUP BY "event", "event_date", "department_name"
ORDER BY "event", "event_date", "department_name"
