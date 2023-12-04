WITH waybill_data AS (

SELECT
  ww.waybill_no,
  CASE WHEN ww.waybill_no IS NOT NULL THEN 1 END AS waybill_alias,
  -- FORMAT_DATE("%b %Y", DATE(ww.shipping_time,'Asia/Jakarta')) AS month,
  DATETIME(ww.shipping_time,'Asia/Jakarta') shipping_time,
  sr.option_name AS waybill_source,
  ww.parent_shipping_cleint vip_username,
  ww.sender_name,
  ww.sender_cellphone,
  ww.sender_province_name,
  ww.sender_city_name,
  ww.sender_district_name,
  ww.pickup_branch_name,
  ww.recipient_district_name,
  ww.recipient_city_name,
  ww.recipient_province_name,
  et.option_name AS express_type,
  st.option_name AS service_type,
  kw.kanwil_name AS kanwil_name_regist,

    FROM `datawarehouse_idexp.waybill_waybill` ww
left join `grand-sweep-324604.datawarehouse_idexp.system_option` sr on ww.waybill_source  = sr.option_value and sr.type_option = 'waybillSource'
left join `grand-sweep-324604.datawarehouse_idexp.system_option` et on ww.express_type  = et.option_value and et.type_option = 'expressType'
left join `grand-sweep-324604.datawarehouse_idexp.system_option` st on ww.service_type  = st.option_value and st.type_option = 'serviceType'
LEFT JOIN `datamart_idexp.mapping_kanwil_area` kw ON ww.recipient_province_name = kw.province_name

WHERE DATE(ww.update_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))
AND ww.void_flag = '0' AND ww.deleted= '0'

QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1
),

return_data AS (

  SELECT
rr.waybill_no,
DATETIME(rr.return_record_time,'Asia/Jakarta') return_regist_time,
rr.return_branch_name return_register_branch,
t5.return_type AS remarks_return,
DATETIME(rr.return_confirm_record_time,'Asia/Jakarta') return_confirm_time,
rc.option_name AS return_confirm_status,
rr.return_shipping_fee,
DATETIME(rr.return_pod_record_time,'Asia/Jakarta') return_pod_record_time,
DATETIME(rr.update_time,'Asia/Jakarta') update_time_rr,

CASE WHEN rr.return_record_time IS NOT NULL THEN 1 ELSE 0 END AS return_regist_alias,
CASE WHEN rr.return_confirm_record_time IS NOT NULL THEN 1 ELSE 0 END AS return_confirm_alias,
CASE WHEN rr.return_pod_record_time IS NOT NULL THEN 1 ELSE 0 END AS return_pod_alias,


  FROM `datawarehouse_idexp.waybill_return_bill` rr
  LEFT OUTER JOIN `datawarehouse_idexp.system_option` rc ON rc.option_value = rr.return_confirm_status AND rc.type_option = 'returnConfirmStatus'
LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.return_type` t5 ON rr.return_type_id = t5.id AND t5.deleted=0
LEFT OUTER JOIN `datamart_idexp.masterdata_city_mapping_area_island_new` pu3 ON rr.recipient_city_name = pu3.city and rr.recipient_province_name = pu3.province --Return_area_register, 

WHERE DATE(rr.update_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))
AND DATE(rr.return_record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -62 DAY))

QUALIFY ROW_NUMBER() OVER (PARTITION BY rr.waybill_no ORDER BY rr.update_time DESC)=1
),

join_return AS (

SELECT 

*,
DATE_DIFF(CURRENT_DATE(),return_regist_time, DAY) AS aging_backlog_confirm,
DATE_DIFF(CURRENT_DATE(),return_confirm_time, DAY) AS aging_backlog_return,


FROM (

SELECT 

ww.waybill_no,
ww.waybill_alias,
ww.shipping_time,
ww.waybill_source,
ww.vip_username,
ww.sender_name,
ww.sender_cellphone,
ww.sender_province_name,
ww.sender_city_name,
ww.sender_district_name,
ww.pickup_branch_name,
ww.recipient_district_name,
ww.recipient_city_name,
ww.recipient_province_name,
ww.express_type,
ww.service_type,
ww.kanwil_name_regist,

rr.return_regist_time,
rr.return_register_branch,
rr.remarks_return,
rr.return_confirm_time,
rr.return_confirm_status,
rr.return_shipping_fee,
rr.return_pod_record_time,
rr.update_time_rr,

rr.return_regist_alias,
rr.return_confirm_alias,
(rr.return_regist_alias - rr.return_confirm_alias) AS backlog_confirm_alias,
rr.return_pod_alias,

CASE
    WHEN rr.return_regist_time IS NOT NULL AND rr.return_confirm_time IS NULL THEN "Backlog Confirm Return"
    WHEN rr.return_regist_time IS NOT NULL AND rr.return_confirm_time IS NOT NULL AND rr.return_pod_record_time IS NULL THEN "Backlog Return"
    WHEN rr.return_regist_time IS NOT NULL AND rr.return_confirm_time IS NOT NULL AND rr.return_pod_record_time IS NOT NULL THEN "POD Return"
    END AS backlog_return_flag,

FROM waybill_data ww
LEFT JOIN return_data rr ON ww.waybill_no = rr.waybill_no

WHERE rr.return_regist_time IS NOT NULL
AND rr.return_pod_record_time IS NULL
)
)

SELECT * FROM (

  SELECT
*,
CASE 
    WHEN backlog_return_flag IN ('Backlog Confirm Return') AND aging_backlog_confirm <3 THEN "<3 Days"
    WHEN backlog_return_flag IN ('Backlog Confirm Return') AND aging_backlog_confirm BETWEEN 3 AND 7 THEN "3-7 Days"
    WHEN backlog_return_flag IN ('Backlog Confirm Return') AND aging_backlog_confirm BETWEEN 8 AND 14 THEN "8-14 Days"
    WHEN backlog_return_flag IN ('Backlog Confirm Return') AND aging_backlog_confirm BETWEEN 15 AND 30 THEN "15-30 Days"
    WHEN backlog_return_flag IN ('Backlog Confirm Return') AND aging_backlog_confirm >30 THEN ">30 Days"
    END AS backlog_confirm_category,
CASE 
    WHEN backlog_return_flag IN ('Backlog Return') AND aging_backlog_return <3 THEN "<3 Days"
    WHEN backlog_return_flag IN ('Backlog Return') AND aging_backlog_return BETWEEN 3 AND 7 THEN "3-7 Days"
    WHEN backlog_return_flag IN ('Backlog Return') AND aging_backlog_return BETWEEN 8 AND 14 THEN "8-14 Days"
    WHEN backlog_return_flag IN ('Backlog Return') AND aging_backlog_return BETWEEN 15 AND 30 THEN "15-30 Days"
    WHEN backlog_return_flag IN ('Backlog Return') AND aging_backlog_return >30 THEN ">30 Days"
    END AS backlog_return_category,


FROM join_return
)
