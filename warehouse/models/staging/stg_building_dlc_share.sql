WITH building_groups AS (
    SELECT
        '64B' AS fclt_building_key,
        '62_64' AS grouped_building
    UNION ALL
    SELECT
        '64G',
        '62_64'
    UNION ALL
    SELECT
        '62H',
        '62_64'
    UNION ALL
    SELECT
        '62M',
        '62_64'
    UNION ALL
    SELECT
        '62W',
        '62_64'
    UNION ALL
    SELECT
        '64W',
        '62_64'
    UNION ALL
    SELECT
        '6B',
        '6_6B'
    UNION ALL
    SELECT
        '6',
        '6_6B'
    UNION ALL
    SELECT
        '7',
        '7_7A'
    UNION ALL
    SELECT
        '7A',
        '7_7A'
    UNION ALL
    SELECT
        'NW12',
        'NW12_NW12A'
    UNION ALL
    SELECT
        'NW12A',
        'NW12_NW12A'
    UNION ALL
    SELECT
        'W53',
        'W53_W53A_W53B_W53C_W53D'
    UNION ALL
    SELECT
        'W53A',
        'W53_W53A_W53B_W53C_W53D'
    UNION ALL
    SELECT
        'W53B',
        'W53_W53A_W53B_W53C_W53D'
    UNION ALL
    SELECT
        'W53C',
        'W53_W53A_W53B_W53C_W53D'
    UNION ALL
    SELECT
        'W53D',
        'W53_W53A_W53B_W53C_W53D'
    UNION ALL
    SELECT
        'W53E',
        'W53_W53A_W53B_W53C_W53D'
    UNION ALL
    SELECT
        'W53F',
        'W53_W53A_W53B_W53C_W53D'
    UNION ALL
    SELECT
        'W53G',
        'W53_W53A_W53B_W53C_W53D'
    UNION ALL
    SELECT
        '42',
        '42_43_N16_E40'
    UNION ALL
    SELECT
        '43',
        '42_43_N16_E40'
    UNION ALL
    SELECT
        'N16',
        '42_43_N16_E40'
    UNION ALL
    SELECT
        'E40',
        '42_43_N16_E40'
    UNION ALL
    SELECT
        '42T',
        '42_43_N16_E40'
    UNION ALL
    SELECT
        '42C',
        '42_43_N16_E40'
    UNION ALL
    SELECT
        'W85',
        'W85_W85A_W85B_W85C_W85D_W85E_W85F_W85G_W85H_W85J_W85K'
    UNION ALL
    SELECT
        'W85A',
        'W85_W85A_W85B_W85C_W85D_W85E_W85F_W85G_W85H_W85J_W85K'
    UNION ALL
    SELECT
        'W85B',
        'W85_W85A_W85B_W85C_W85D_W85E_W85F_W85G_W85H_W85J_W85K'
    UNION ALL
    SELECT
        'W85C',
        'W85_W85A_W85B_W85C_W85D_W85E_W85F_W85G_W85H_W85J_W85K'
    UNION ALL
    SELECT
        'W85D',
        'W85_W85A_W85B_W85C_W85D_W85E_W85F_W85G_W85H_W85J_W85K'
    UNION ALL
    SELECT
        'W85E',
        'W85_W85A_W85B_W85C_W85D_W85E_W85F_W85G_W85H_W85J_W85K'
    UNION ALL
    SELECT
        'W85F',
        'W85_W85A_W85B_W85C_W85D_W85E_W85F_W85G_W85H_W85J_W85K'
    UNION ALL
    SELECT
        'W85G',
        'W85_W85A_W85B_W85C_W85D_W85E_W85F_W85G_W85H_W85J_W85K'
    UNION ALL
    SELECT
        'W85H',
        'W85_W85A_W85B_W85C_W85D_W85E_W85F_W85G_W85H_W85J_W85K'
    UNION ALL
    SELECT
        'W85J',
        'W85_W85A_W85B_W85C_W85D_W85E_W85F_W85G_W85H_W85J_W85K'
    UNION ALL
    SELECT
        'W85K',
        'W85_W85A_W85B_W85C_W85D_W85E_W85F_W85G_W85H_W85J_W85K'
    UNION ALL
    SELECT
        '32P',
        'Parking Areas'
    UNION ALL
    SELECT
        'W18P',
        'Parking Areas'
    UNION ALL
    SELECT
        'E37P',
        'Parking Areas'
    UNION ALL
    SELECT
        'E53P',
        'Parking Areas'
    UNION ALL
    SELECT
        'E62P',
        'Parking Areas'
    UNION ALL
    SELECT
        'NW86P',
        'Parking Areas'
    UNION ALL
    SELECT
        'W18P',
        'Parking Areas'
),

bd_grouped AS (
    SELECT
        COALESCE(bg.grouped_building, bd.fclt_building_key) AS fclt_building_key,
        bd.dlc_key,
        SUM(bd.total_area) AS total_area
    FROM raw.dlc_floor_share AS bd
    LEFT JOIN building_groups AS bg ON bd.fclt_building_key = bg.fclt_building_key
    GROUP BY COALESCE(bg.grouped_building, bd.fclt_building_key), bd.dlc_key
),

total_area_per_dlc AS (
    SELECT
        fclt_building_key,
        SUM(total_area) AS total_floor_area
    FROM bd_grouped
    GROUP BY fclt_building_key
),

adjusted_share AS (
    SELECT
        bg.fclt_building_key,
        bg.dlc_key,
        bg.total_area AS dlc_assigned_area,
        ta.total_floor_area AS building_total_area,
        bg.total_area / NULLIF(ta.total_floor_area, 0) AS dlc_share
    FROM bd_grouped AS bg
    LEFT JOIN total_area_per_dlc AS ta
        ON bg.fclt_building_key = ta.fclt_building_key
)

SELECT
    fclt_building_key AS building_number,
    dlc_key,
    dlc_assigned_area,
    building_total_area,
    dlc_share
FROM adjusted_share
ORDER BY fclt_building_key
