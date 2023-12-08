--masih pakai dummy >> `dev_idexp.temporary_table_dummy_smu`

WITH root_smu_system AS (

SELECT
  -- DISTINCT(ww.waybill_no) waybill_no,
  sc.waybill_no,
  sc.bag_no,
  sc.vehicle_tag_no,
  DATE(sc.record_time, 'Asia/Jakarta') AS record_time,
  CONCAT(sc.bag_no,' ','-',' ',sc.vehicle_tag_no,' ','-',' ',sc.waybill_no) AS bm_vm_awb,
  CONCAT(sc.bag_no,' ','-',' ',sc.vehicle_tag_no) AS bm_vm_concat,
  -- rd1.option_name AS operation_type,
  -- pu.pulau,

  -- CAST(ww.item_actual_weight AS NUMERIC) AS item_actual_weight,

FROM `datawarehouse_idexp.waybill_waybill_line` sc
  LEFT JOIN `datawarehouse_idexp.system_option` rd1 ON sc.operation_type = rd1.option_value AND rd1.type_option = 'operationType'

WHERE DATE(sc.record_time) BETWEEN '2023-07-01' AND '2023-07-31' -->= (DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -20 DAY)))
-- AND sc.operation_type = '04'
  -- AND ww.sender_province_name = 'DKI JAKARTA'
  -- AND sc.bag_no IN (  )
  -- AND sc.vehicle_tag_no IN ()
AND bag_no IS NOT NULL AND vehicle_tag_no IS NOT NULL
-- GROUP BY 1,2,3,4,5,6,7,8,9
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC)=1
-- wl.operation_type, 
-- wl.record_time ORDER BY ww.waybill_no, wl.record_time ASC)=1
),

smu_sending AS (

  SELECT

  sc.waybill_no,
  sc.bag_no,
  sc.vehicle_tag_no,
  DATE(sc.record_time, 'Asia/Jakarta') AS record_time,
  CONCAT(sc.bag_no,' ','-',' ',sc.vehicle_tag_no,' ','-',' ',sc.waybill_no) AS bm_vm_awb,
  CONCAT(sc.bag_no,' ','-',' ',sc.vehicle_tag_no) AS bm_vm_concat,
  sc.operation_branch_name AS mh_sending,


  FROM `datawarehouse_idexp.waybill_waybill_line` sc
  LEFT JOIN `datawarehouse_idexp.system_option` rd1 ON sc.operation_type = rd1.option_value AND rd1.type_option = 'operationType'

  WHERE DATE(sc.record_time) BETWEEN '2023-06-01' AND '2023-07-31' 

  AND sc.operation_type = '04'
  AND sc.operation_branch_name LIKE '%MH%'
  AND (CONCAT(sc.bag_no,' ','-',' ',sc.operation_branch_name,' ','-',' ',sc.vehicle_tag_no)) IS NOT NULL
  AND bag_no IS NOT NULL AND vehicle_tag_no IS NOT NULL

-- QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC)=1
),

smu_arrival AS (

  SELECT

  sc.waybill_no,
  sc.bag_no,
  sc.vehicle_tag_no,
  DATE(sc.record_time, 'Asia/Jakarta') AS record_time,
  CONCAT(sc.bag_no,' ','-',' ',sc.vehicle_tag_no,' ','-',' ',sc.waybill_no) AS bm_vm_awb,
  CONCAT(sc.bag_no,' ','-',' ',sc.vehicle_tag_no) AS bm_vm_concat,
  sc.operation_branch_name AS mh_arrival,


  FROM `datawarehouse_idexp.waybill_waybill_line` sc
  LEFT JOIN `datawarehouse_idexp.system_option` rd1 ON sc.operation_type = rd1.option_value AND rd1.type_option = 'operationType'

  WHERE DATE(sc.record_time) BETWEEN '2023-07-01' AND '2023-07-31' 

  AND sc.operation_type = '05'
  AND sc.operation_branch_name LIKE '%MH%'
  AND (CONCAT(sc.bag_no,' ','-',' ',sc.operation_branch_name,' ','-',' ',sc.vehicle_tag_no)) IS NOT NULL
  AND bag_no IS NOT NULL AND vehicle_tag_no IS NOT NULL

-- QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC)=1
),

shipping_waybill AS (

  SELECT
  ww.waybill_no,
  DATE(ww.shipping_time, 'Asia/Jakarta') AS shipping_time,
  et.option_name AS express_type,
  ww.standard_shipping_fee,
  ww.handling_fee,
  ww.other_fee,
  ww.insurance_amount,
  ww.total_shipping_fee,
  (standard_shipping_fee + handling_fee) AS system_sf,
  CAST(ww.item_calculated_weight AS NUMERIC) AS system_weight,
  ww.sender_province_name,
  ww.sender_city_name,
  ww.sender_district_name,
  ww.pickup_branch_name,
  ww.recipient_province_name,
  ww.recipient_city_name,
  ww.recipient_district_name,
  ww.pod_branch_name,


  FROM `datawarehouse_idexp.waybill_waybill` ww
  LEFT JOIN `datawarehouse_idexp.system_option` et ON ww.express_type = et.option_value AND et.type_option = 'expressType'

  WHERE DATE(ww.shipping_time) BETWEEN '2023-04-01' AND '2023-08-16'
  AND ww.deleted = '0'


)

SELECT 
*
-- bag_no,
-- vehicle_tag_no,
-- SUM(system_sf) total_system_sf,
-- SUM(system_weight) total_weight,
-- SUM(item_actual_weight) item_actual_weight,
-- COUNT(bag_no) count_BM,

 FROM (

  SELECT
  sc.bm_vm_awb,
  sc.bm_vm_concat,
  sc.bag_no,
  sc.vehicle_tag_no,
  sc.waybill_no,
  ww.shipping_time,
  ww.express_type,
  sc.record_time,
  ww.standard_shipping_fee,
  ww.handling_fee,
  ww.other_fee,
  ww.insurance_amount,
  ww.total_shipping_fee,
  ww.system_weight,
  ww.system_sf,
  -- item_actual_weight,
  ww.sender_province_name,
  ww.sender_city_name,
  ww.sender_district_name,
  ww.pickup_branch_name,
  ww.recipient_province_name,
  ww.recipient_city_name,
  ww.recipient_district_name,
  ww.pod_branch_name,
  ss.mh_sending,
  sa.mh_arrival,


  FROM root_smu_system sc
  LEFT OUTER JOIN shipping_waybill ww ON sc.waybill_no = ww.waybill_no
  LEFT OUTER JOIN smu_sending ss ON sc.bm_vm_awb = ss.bm_vm_awb
  LEFT OUTER JOIN smu_arrival sa ON sc.bm_vm_awb = sa.bm_vm_awb

  -- WHERE waybill_no = 'IDS900077226141'
  -- WHERE 
  -- bag_no IN ('BA2500043954')
  -- AND 
  -- vehicle_tag_no IN ('VF0330000971')
  -- waybill_no = 'IDE702946439234'

  QUALIFY ROW_NUMBER() OVER (PARTITION BY bm_vm_awb)=1
  -- QUALIFY ROW_NUMBER() OVER (PARTITION BY vehicle_tag_no)=1
  -- QUALIFY ROW_NUMBER() OVER (PARTITION BY waybill_no)=1
  )

-- GROUP BY 1,2
WHERE shipping_time IS NOT NULL
AND bag_no <> ''

-- AND bm_vm_concat = "BM2730017840 - VF2730000233"
ORDER BY bag_no ASC, waybill_no ASC
-- ORDER BY record_time DESC, bag_no ASC
