WITH stu AS (
    SELECT *
    FROM {{ ref('stg_attendance_records')}}
    WHERE student_year IS NOT NULL
)

SELECT
    "event",
    "event_date",
    student_year,
    COUNT(*) AS attendee_count
FROM stu
GROUP BY "event", "event_date", "student_year"
ORDER BY "event", "student_year"
