SELECT
    event,
    event_date,
    "role",
    COUNT(*) AS attendee_count
FROM {{ ref('stg_attendance_records')}}
GROUP BY event, event_date, "role"
ORDER BY event, event_date, "role"
