WITH longest_process_forward AS (

SELECT *,
CONCAT(operation_type," ","-"," ",operation_type_2) operation_type_1_2,
branch_name_1 branch_name,
scan_record_duration_hour leadtime_hour,
scan_record_duration_sec leadtime_sec,

FROM `datamart_idexp.dashboard_cx_longest_process_v2-1`

WHERE DATE(shipping_time) BETWEEN DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY)) AND DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -78 DAY))
AND first_scan_time_branch IS NOT NULL

),

longest_process_return AS (
SELECT *,
CONCAT(operation_type," ","-"," ",operation_type_2) operation_type_1_2,
branch_name_1 branch_name,
scan_record_duration_hour leadtime_hour,
scan_record_duration_sec leadtime_sec,

FROM `datamart_idexp.dashboard_cx_longest_process_v2-2`

WHERE DATE(shipping_time) BETWEEN DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY)) AND DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -78 DAY))
AND first_scan_time_branch IS NOT NULL
)

SELECT * FROM longest_process_forward UNION ALL
SELECT * FROM longest_process_return
