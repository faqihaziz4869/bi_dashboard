-------------- Query Longest Process buat waybill forward ----------------


WITH waybill_data AS (

SELECT
ww.waybill_no,
DATETIME(ww.shipping_time,'Asia/Jakarta') shipping_time,
sr.option_name waybill_source,
ww.return_flag,

FROM `datawarehouse_idexp.waybill_waybill` ww
left join `grand-sweep-324604.datawarehouse_idexp.system_option` sr on ww.waybill_source  = sr.option_value and sr.type_option = 'waybillSource'

WHERE DATE(ww.shipping_time,'Asia/Jakarta') >= (DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY)))
AND ww.void_flag = '0' AND ww.deleted= '0'
-- AND ww.waybill_no IN ('IDD900607625919') --'IDE701766223150' --'IDE701766223150' --'IDV902145320146' --'IDE700706479068'

QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1
),

return_data AS (

  SELECT
rr.waybill_no,
rr.return_waybill_no,
DATETIME(rr.return_confirm_record_time,'Asia/Jakarta') return_confirm_time,
rc.option_name AS return_confirm_status,
DATETIME(rr.update_time,'Asia/Jakarta') update_time_rr,


  FROM `datawarehouse_idexp.waybill_return_bill` rr
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rc ON rc.option_value = rr.return_confirm_status AND rc.type_option = 'returnConfirmStatus'
LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.return_type` t5 ON rr.return_type_id = t5.id AND t5.deleted=0


WHERE DATE(rr.update_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))
-- AND DATE(rr.return_record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))

QUALIFY ROW_NUMBER() OVER (PARTITION BY rr.waybill_no ORDER BY rr.update_time DESC)=1
),

waybill_to_return AS (

  SELECT
  ww.waybill_no,
  ww.shipping_time,
  ww.waybill_source,
  rr.return_waybill_no,
  rr.return_confirm_status,
  rr.return_confirm_time,
  ww.return_flag,

FROM waybill_data ww
LEFT OUTER JOIN return_data rr ON ww.waybill_no = rr.waybill_no

WHERE rr.return_confirm_time IS NULL --Filter yang non return aja--
),

scan_record_main AS (

  SELECT 
  waybill_no,
  rd1.option_name operation_type,
  operation_branch_name,
  DATETIME(sr.record_time,'Asia/Jakarta') record_time,

FROM
    `datawarehouse_idexp.waybill_waybill_line` sr
  LEFT JOIN `datawarehouse_idexp.system_option` rd1 ON sr.operation_type = rd1.option_value AND rd1.type_option = 'operationType'


  WHERE DATE(sr.record_time,'Asia/Jakarta') >= (DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY)))

),

get_scan_record_detail AS (

SELECT

currenttab.waybill_no,
currenttab.operation_type,
currenttab.operation_branch_name,
currenttab.record_time,

LAG(currenttab.operation_branch_name,1) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS previous_branch_name_1,
LAG(currenttab.operation_branch_name,2) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS previous_branch_name_2,
LEAD(currenttab.operation_branch_name) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time) AS next_branch_name,
MAX(currenttab.operation_branch_name) OVER (PARTITION BY currenttab.waybill_no ORDER BY currenttab.record_time DESC) AS last_location,

FROM scan_record_main currenttab

),

get_waybill_last_loc AS (
    SELECT waybill_no,
    MAX(operation_branch_name) OVER (PARTITION BY waybill_no ORDER BY record_time DESC) AS last_location,
    MAX(record_time) OVER (PARTITION BY waybill_no ORDER BY record_time DESC) AS latest_scan_time,

    FROM scan_record_main
    QUALIFY ROW_NUMBER() OVER (PARTITION BY waybill_no ORDER BY record_time DESC)=1

),

first_scan_record AS (

  SELECT 
  sr.waybill_no,
  sr.operation_type,
  sr.operation_branch_name branch_name_1,
  sr.record_time first_scan_time_branch,
  sr.previous_branch_name_1,
  sr.previous_branch_name_2,
  sr.next_branch_name,
  sr.last_location,



FROM
    get_scan_record_detail sr
    
  WHERE operation_type NOT IN ('Pickup Failure')

  QUALIFY ROW_NUMBER() OVER (PARTITION BY waybill_no,operation_branch_name ORDER BY record_time ASC)=1
),

last_scan_record AS (

  SELECT 
  sr.waybill_no,
  sr.operation_type operation_type_2,
  sr.operation_branch_name branch_name_2,
  sr.record_time last_scan_time_branch,
  DATETIME(rr.return_confirm_record_time,'Asia/Jakarta') return_confirm_record_time,
  CASE
      WHEN sr.record_time >= DATETIME(rr.return_confirm_record_time,'Asia/Jakarta') THEN "return process"
      ELSE "forward delivery" END AS return_or_not_last_scan,
  

FROM
    get_scan_record_detail sr
    LEFT JOIN waybill_data ww ON sr.waybill_no = ww.waybill_no
    LEFT JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sr.waybill_no = rr.waybill_no
    AND DATE(rr.update_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))

WHERE DATE(sr.record_time) >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))
  AND operation_type NOT IN ('Confirm Return Bill')
  -- AND ww.return_flag = '0'

  -- AND DATETIME(sr.record_time) <= DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')

  QUALIFY ROW_NUMBER() OVER (PARTITION BY waybill_no,operation_branch_name ORDER BY record_time DESC)=1
  ),
  -- WHERE last_scan_time_branch < return_confirm_record_time,

first_scan_record_return AS (

  SELECT 
  sr.waybill_no,
  sr.operation_type,
  sr.operation_branch_name branch_name_1,
  sr.record_time first_scan_time_branch,
  sr.previous_branch_name_1,
  sr.previous_branch_name_2,
  sr.next_branch_name,
  sr.last_location,

FROM
    get_scan_record_detail sr
  LEFT JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sr.waybill_no = rr.waybill_no
    AND DATE(rr.update_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))

WHERE DATE(sr.record_time) >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))
  AND sr.record_time >= DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')

  QUALIFY ROW_NUMBER() OVER (PARTITION BY waybill_no,operation_branch_name ORDER BY record_time ASC)=1
),

last_scan_record_return AS (

  SELECT 
  sr.waybill_no,
  sr.operation_type operation_type_2,
  sr.operation_branch_name branch_name_2,
  sr.record_time last_scan_time_branch,
  CASE
      WHEN sr.record_time >= DATETIME(rr.return_confirm_record_time,'Asia/Jakarta') THEN "return process"
      ELSE "forward delivery" END AS return_or_not_last_scan,
  

FROM
    get_scan_record_detail sr
    LEFT JOIN `datawarehouse_idexp.waybill_return_bill` rr ON sr.waybill_no = rr.waybill_no
    AND DATE(rr.update_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))

WHERE DATE(sr.record_time) >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))
  AND sr.record_time >= DATETIME(rr.return_confirm_record_time,'Asia/Jakarta')

  QUALIFY ROW_NUMBER() OVER (PARTITION BY waybill_no,operation_branch_name ORDER BY record_time DESC)=1
),

get_waybill_return_scan_record AS (

SELECT *,
DATETIME_DIFF(last_scan_time_branch, first_scan_time_branch, HOUR) AS scan_record_duration_hour,
DATETIME_DIFF(last_scan_time_branch, first_scan_time_branch, SECOND) AS scan_record_duration_sec,

FROM (
  SELECT
ww.waybill_no,
ww.shipping_time,
ww.waybill_source,
a.operation_type,
a.branch_name_1,
a.first_scan_time_branch,
a.previous_branch_name_1,
a.previous_branch_name_2,
a.next_branch_name,
b.operation_type_2,
b.branch_name_2,
b.last_scan_time_branch,
c.last_location,
c.latest_scan_time,
b.return_or_not_last_scan,
CASE
    WHEN ww.return_confirm_time IS NOT NULL THEN "Return" ELSE "Non-Return"
    END AS return_or_not, -- tambah kolom


FROM waybill_to_return ww
LEFT JOIN first_scan_record a ON ww.waybill_no = a.waybill_no
LEFT JOIN last_scan_record b ON a.waybill_no = b.waybill_no AND a.branch_name_1 = b.branch_name_2
LEFT JOIN get_waybill_last_loc c ON a.waybill_no = c.waybill_no
-- LEFT JOIN get_scan_record_detail sr ON ww.waybill_no = sr.waybill_no
-- WHERE b.return_or_not_last_scan = "non-return"
-- WHERE ww.return_flag = '0'
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY waybill_no,operation_branch_name ORDER BY record_time DESC)=1
ORDER BY first_scan_time_branch DESC
-- GROUP BY a.waybill_no, a.branch_name_1
-- SELECT * FROM last_scan_record
)
),

get_waybill_return_scan_record_return AS (

SELECT *,
DATETIME_DIFF(last_scan_time_branch, first_scan_time_branch, HOUR) AS scan_record_duration_hour,
DATETIME_DIFF(last_scan_time_branch, first_scan_time_branch, SECOND) AS scan_record_duration_sec,



FROM (
  SELECT
ww.waybill_no,
ww.shipping_time,
ww.waybill_source,
a.operation_type,
a.branch_name_1,
a.first_scan_time_branch,
a.previous_branch_name_1,
a.previous_branch_name_2,
a.next_branch_name,
b.operation_type_2,
b.branch_name_2,
b.last_scan_time_branch,
c.last_location,
c.latest_scan_time,
b.return_or_not_last_scan,
CASE
    WHEN ww.return_confirm_time IS NOT NULL THEN "Return" ELSE "Non-Return"
    END AS return_or_not, -- tambah kolom


FROM waybill_to_return ww
LEFT JOIN first_scan_record_return a ON ww.waybill_no = a.waybill_no
LEFT JOIN last_scan_record_return b ON a.waybill_no = b.waybill_no AND a.branch_name_1 = b.branch_name_2
LEFT JOIN get_waybill_last_loc c ON a.waybill_no = c.waybill_no
-- WHERE b.return_or_not_last_scan = "non-return"

ORDER BY first_scan_time_branch DESC
-- GROUP BY a.waybill_no, a.branch_name_1
-- SELECT * FROM last_scan_record
)
),

gabung_all_data AS (

  SELECT * FROM get_waybill_return_scan_record UNION ALL
  SELECT * FROM get_waybill_return_scan_record_return

)

SELECT *

FROM (

  SELECT *,
  IF ((waybill_no IS NOT NULL), RANK() OVER (PARTITION BY waybill_no ORDER BY DATE_DIFF(last_scan_time_branch, first_Scan_time_branch, SECOND) DESC), NULL) AS ranking,
  DATE_DIFF(last_scan_time_branch, first_scan_time_branch, DAY) AS scan_record_duration_day, --tambah kolom
  DATE_DIFF(CURRENT_DATE('Asia/Jakarta'),latest_scan_time, DAY) AS aging_from_last_scan, --tambah kolom
  (scan_record_duration_hour/24) AS scan_record_duration_day_1, --tambah kolom
  CEIL((scan_record_duration_hour/24)) AS rounded_duration_day, --tambah kolom


-- FROM get_waybill_return_scan_record
FROM gabung_all_data
)
WHERE ranking <= 3
AND rounded_duration_day >= 14


