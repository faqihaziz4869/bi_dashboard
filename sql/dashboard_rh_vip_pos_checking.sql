
WITH pos_checking AS (
SELECT 

ww.waybill_no,
DATETIME(ww.shipping_time,'Asia/Jakarta') shipping_time,
t0.option_name AS waybill_source,
ww.vip_customer_name vip_username,
ww.pickup_branch_name,
ww.sender_province_name,
ww.sender_city_name,
ww.recipient_province_name,
ww.recipient_city_name,
t1.option_name AS waybill_status,


FROM `datawarehouse_idexp.waybill_waybill` ww
LEFT OUTER JOIN `dev_idexp.masterdata_branch_coverage_th` mb ON ww.recipient_district_id = mb.district_id
LEFT OUTER JOIN `datawarehouse_idexp.system_option` t0 ON t0.option_value = ww.waybill_source AND t0.type_option = 'waybillSource'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = ww.waybill_status AND t1.type_option = 'waybillStatus'

WHERE DATE(ww.shipping_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))
AND ww.void_flag = '0' AND ww.deleted = '0'
AND t0.option_name IN ('VIP Customer Portal')
AND t1.option_name NOT IN ('Signed','Return Received')
AND ww.pod_record_time IS NULL
AND ww.return_pod_record_time IS NULL

QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1

),

deliv_attempt as(

  SELECT
sc.waybill_no,
sc.deliv_attempt_1,
sc.deliv_attempt_2,
sc.deliv_attempt_3,

FROM (
  SELECT
        waybill_no,
        MAX(IF(id = 1, DATETIME(record_time), NULL)) AS deliv_attempt_1,
        MAX(IF(id = 2, DATETIME(record_time), NULL)) AS deliv_attempt_2,
        MAX(IF(id = 3, DATETIME(record_time), NULL)) AS deliv_attempt_3,
        FROM (
              SELECT sc.waybill_no, 
              DATETIME(sc.record_time,'Asia/Jakarta') record_time, 
                        
              RANK() OVER (PARTITION BY waybill_no ORDER BY DATETIME(sc.record_time, 'Asia/Jakarta') ASC ) AS id
              FROM `datawarehouse_idexp.waybill_waybill_line` sc
              WHERE DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))
              AND sc.operation_type IN ('09')
        ) 

        GROUP BY 1 
) sc
QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no)=1
),

pos_attempt as(

  SELECT
ps.waybill_no,
ps.pos_attempt_1,
ps.pos_reason_1,

ps.pos_attempt_2,
pos_reason_2,

pos_attempt_3,
pos_reason_3,


FROM (
  SELECT
        waybill_no,
        MAX(IF(id = 1, DATETIME(operation_time), NULL)) AS pos_attempt_1,
        MAX(IF(id = 2, DATETIME(operation_time), NULL)) AS pos_attempt_2,
        MAX(IF(id = 3, DATETIME(operation_time), NULL)) AS pos_attempt_3,

        MAX(IF(id = 1, problem_reason, NULL)) AS pos_reason_1,
        MAX(IF(id = 2, problem_reason, NULL)) AS pos_reason_2,
        MAX(IF(id = 3, problem_reason, NULL)) AS pos_reason_3,

        FROM (
              SELECT ps.waybill_no, 
              DATETIME(ps.operation_time,'Asia/Jakarta') operation_time,
              ps.problem_reason, 
                        
              RANK() OVER (PARTITION BY ps.waybill_no ORDER BY DATETIME(ps.operation_time, 'Asia/Jakarta') ASC ) AS id
            --   FROM `datawarehouse_idexp.waybill_waybill_line` sc
              FROM `datawarehouse_idexp.waybill_problem_piece` ps

              WHERE DATE(ps.operation_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))
              AND ps.problem_type NOT IN ('02')
        ) 

        GROUP BY 1 
) ps
QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.waybill_no)=1
),

count_pos as(
  SELECT
        sc.waybill_no,
        COUNT(sc.record_time) OVER (PARTITION BY sc.waybill_no) AS count_pos,

              FROM `datawarehouse_idexp.waybill_waybill_line` sc
              LEFT join `grand-sweep-324604.datawarehouse_idexp.res_problem_package` t4 on sc.problem_code = t4.code and t4.deleted = '0'
              LEFT OUTER JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = sc.problem_type AND t1.type_option = 'problemType'
              WHERE DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))

              AND sc.operation_type IN ('18')

              QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no)=1
        ),

count_deliv_attempt AS (

SELECT
sc.waybill_no,
COUNT(rd16.option_name) OVER (PARTITION BY sc.waybill_no) AS count_deliv_attempt,

FROM `datawarehouse_idexp.waybill_waybill_line` sc 
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd16 ON rd16.option_value = sc.operation_type AND rd16.type_option = 'operationType'

WHERE 
DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))
AND operation_type = "09"

QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no)=1

),

last_location as(
  SELECT
      sc.waybill_no,
      MAX(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) AS last_location, 
      MAX(rd16.option_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) AS last_activity, 
      MAX(DATETIME(sc.record_time,'Asia/Jakarta')) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) AS last_scan_time,

              FROM `datawarehouse_idexp.waybill_waybill_line` sc
              LEFT join `grand-sweep-324604.datawarehouse_idexp.res_problem_package` t4 on sc.problem_code = t4.code and t4.deleted = '0'
              LEFT OUTER JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = sc.problem_type AND t1.type_option = 'problemType'
              LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd16 ON rd16.option_value = sc.operation_type AND rd16.type_option = 'operationType'

              WHERE DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))

              QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC)=1
        )

SELECT *

FROM (

      SELECT ww.*,
      da.deliv_attempt_1,
      ps.pos_attempt_1,
      ps.pos_reason_1,
      da.deliv_attempt_2,
      ps.pos_attempt_2,
      ps.pos_reason_2,
      loc.last_location,
      cp.count_pos count_pos_attempt,
      cd.count_deliv_attempt,

FROM pos_checking ww
LEFT OUTER JOIN deliv_attempt da ON ww.waybill_no = da.waybill_no
LEFT OUTER JOIN pos_attempt ps ON ww.waybill_no = ps.waybill_no 
LEFT OUTER JOIN count_pos cp ON ww.waybill_no = cp.waybill_no
LEFT OUTER JOIN count_deliv_attempt cd ON ww.waybill_no = cd.waybill_no
LEFT OUTER JOIN last_location loc ON ww.waybill_no = loc.waybill_no
)
WHERE count_pos_attempt >= 2
