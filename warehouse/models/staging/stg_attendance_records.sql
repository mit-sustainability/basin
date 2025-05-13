SELECT
    ar.record_id,
    ar.status,
    ar.first_name,
    ar.last_name,
    ar.email,
    ar.event_date,
    ar."event",
    COALESCE(ed.department_name, st.department_name) AS department_name,
    st.student_year,
    CASE
        WHEN (
            ar.email ILIKE '%@mit.edu'
            OR ar.email ILIKE '%@media.mit.edu'
            OR ar.email ILIKE '%@mitimco.mit.edu'
        ) AND st.student_year IS NULL THEN 'employee'
        WHEN ar.email ILIKE '%@alum.mit.edu' THEN 'alumni'
        WHEN st.student_year IS NOT NULL THEN 'student'
        ELSE 'external'
    END AS "role",
    ar.last_update
FROM {{ source('raw','attendance_records') }} AS ar
LEFT JOIN {{ source('raw','employee_directory') }} AS ed ON ar.email = ed.email
LEFT JOIN {{ source('raw','student_directory') }} AS st ON ar.email = st.email
