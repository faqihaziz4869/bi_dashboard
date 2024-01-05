WITH root_data_vip AS (

  SELECT

  ww.waybill_no,
  DATETIME(ww.shipping_time,'Asia/Jakarta') shipping_date,
  CASE
      WHEN ww.waybill_no IS NOT NULL THEN 1 ELSE 0 END AS waybill_alias,
  DATETIME(ww.pod_record_time,'Asia/Jakarta') pod_date,
  CASE
      WHEN ww.pod_record_time IS NOT NULL THEN 1 ELSE 0 END AS pod_alias,

  CASE
      WHEN ww.pod_record_time IS NULL THEN 1 ELSE 0 END AS total_not_pod,

  sr.option_name AS waybill_source,
  ww.parent_shipping_cleint vip_username,
  ww.standard_shipping_fee,

  ww.recipient_province_name,
  ww.recipient_city_name,
  ww.recipient_district_name,
  st.option_name AS service_type,
  et.option_name AS express_type,

  t2.division AS D_Source,
  t2.sales_name AS Sales_Name_Source,
  t3.division AS D_Seller, 
  t3.sales_name AS Sales_Name_Seller,

  CASE
      WHEN t3.division = 'Mitra' AND t3.sales_name = 'Mitra' THEN "VIP Seller Amartha"
      WHEN t3.division IS NULL AND t3.sales_name IS NULL THEN "VIP Seller Amartha"
      ELSE "VIP Seller HO" END AS source_category,

  CASE 
      WHEN ww.delivery_branch_name IS NOT NULL THEN ww.delivery_branch_name
      WHEN ww.delivery_branch_name IS NULL THEN mb.branch_name
      END AS th_destination,
  kw.kanwil_name,
  ws.option_name AS waybill_status,
  rf.option_name AS return_flag,

  CASE
      WHEN ww.pod_record_time IS NOT NULL THEN "Delivered"
      WHEN ww.pod_record_time IS NULL AND rf.option_name = 'Retur' AND ww.return_pod_record_time IS NOT NULL THEN "Returned"
      WHEN ww.pod_record_time IS NULL AND ww.return_pod_record_time IS NOT NULL THEN "Returned"
      WHEN ww.pod_record_time IS NULL AND rf.option_name = 'Retur' AND ww.return_pod_record_time IS NULL THEN "Return Process"
      WHEN ww.pod_record_time IS NULL AND rf.option_name = 'Tidak Retur' AND ws.option_name NOT IN ('Signed','Return Received') THEN "Stuck"
      END AS last_status,


  FROM `datawarehouse_idexp.dm_waybill_waybill` ww
  -- FROM `datawarehouse_idexp.waybill_waybill` ww
  LEFT OUTER JOIN `dev_idexp.masterdata_branch_coverage_th` mb ON ww.recipient_district_id = mb.district_id
  LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.system_option` sr on ww.waybill_source  = sr.option_value and sr.type_option = 'waybillSource'
  LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.system_option` ws on ww.waybill_status  = ws.option_value and ws.type_option = 'waybillStatus'
  LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.system_option` rf on ww.return_flag  = rf.option_value and rf.type_option = 'returnFlag'
  LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.system_option` st on ww.service_type  = st.option_value and st.type_option = 'serviceType'
  LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.system_option` et on ww.express_type  = et.option_value and et.type_option = 'expressType'
  LEFT JOIN `datamart_idexp.masterdata_sales_source` t2 ON t2.source = sr.option_name
  LEFT JOIN `datamart_idexp.masterdata_sales_seller_vip` t3 ON t3.seller_name = vip_customer_name
  LEFT OUTER JOIN `datamart_idexp.mapping_kanwil_area` kw ON ww.recipient_province_name = kw.province_name

  WHERE DATE(ww.shipping_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))
  AND ww.void_flag = '0' --AND ww.deleted= '0'
  AND sr.option_name IN ('VIP Customer Portal')

QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1
),

last_pos as(
  SELECT
        ps.waybill_no,
        MAX(ps.problem_reason) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) AS last_pos_reason,
        MAX(DATETIME(ps.operation_time,'Asia/Jakarta')) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) AS last_pos_attempt,

              FROM `datawarehouse_idexp.waybill_problem_piece` ps
              LEFT OUTER JOIN `datawarehouse_idexp.system_option` prt ON ps.problem_type  = prt.option_value AND prt.type_option = 'problemType'
              WHERE DATE(ps.operation_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))

              AND ps.problem_type NOT IN ('02')

              QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC)=1
         ),

    join_waybill_pos AS (

      SELECT
      ww.*,
      ps.last_pos_reason,
      ps.last_pos_attempt,
      CASE
          WHEN ps.last_pos_attempt IS NOT NULL THEN 1 ELSE 0 END AS total_waybill_pos,
      CASE
          WHEN ps.last_pos_attempt IS NOT NULL AND pod_date IS NULL THEN 1 ELSE 0 END AS not_pod_with_pos,
      CASE
          WHEN ps.last_pos_attempt IS NULL AND pod_date IS NULL THEN 1 ELSE 0 END AS not_pod_without_pos,

      CASE
          WHEN ps.last_pos_attempt IS NOT NULL AND pod_date IS NOT NULL THEN 1 ELSE 0 END AS pod_with_pos,
      CASE
          WHEN ps.last_pos_attempt IS NULL AND pod_date IS NOT NULL THEN 1 ELSE 0 END AS pod_without_pos,
      
      CASE
          WHEN pod_date IS NULL THEN date_diff(current_date('Asia/Jakarta'), DATE(shipping_date), DAY) END AS aging_from_pickup_to_date,

      date_diff(DATE(pod_date), DATE(shipping_date), DAY) AS leadtime_pickup_to_pod,


      FROM root_data_vip ww
      LEFT OUTER JOIN last_pos ps ON ww.waybill_no = ps.waybill_no
      LEFT JOIN `datamart_idexp.masterdata_sales_source` t2 ON t2.source = ww.waybill_source
LEFT JOIN `datamart_idexp.masterdata_sales_seller_vip` t3 ON t3.seller_name = ww.vip_username
      
    )

SELECT *,

CASE
    WHEN aging_from_pickup_to_date = 0 THEN 1 ELSE 0 END AS aging_0day,
CASE
    WHEN aging_from_pickup_to_date IN (1,2) THEN 1 ELSE 0 END AS aging_1_2day,
CASE
    WHEN aging_from_pickup_to_date BETWEEN 3 AND 14 THEN 1 ELSE 0 END AS aging_3_14day,
CASE
    WHEN aging_from_pickup_to_date >=15 THEN 1 ELSE 0 END AS aging_more_15day,


FROM (
  
  SELECT 
  
  DATE(shipping_date) shipping_date,
  waybill_source,
  vip_username,
  source_category,
  waybill_status,
  return_flag,
  last_status,
  service_type,
  express_type,
  kanwil_name,
  recipient_province_name,
  recipient_city_name,
  recipient_district_name,
  th_destination,
  last_pos_reason pos_reason,
  aging_from_pickup_to_date,
  leadtime_pickup_to_pod,
  SUM(waybill_alias) total_pickup,
  SUM(pod_alias) total_pod,
  SUM(total_not_pod) total_not_pod,
  SUM(standard_shipping_fee) gross_revenue,
  SUM(total_waybill_pos) total_waybill_pos,
  SUM(not_pod_with_pos) not_pod_with_pos,
  SUM(not_pod_without_pos) not_pod_without_pos,
  SUM(pod_with_pos) pod_with_pos,
  SUM(pod_without_pos) pod_without_pos,
  COUNT(DISTINCT(DATE(shipping_date))) AS count_shipping_date,
  COUNT(last_pos_reason) AS count_pos_reason,
  
  FROM join_waybill_pos

  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
  ORDER BY shipping_date ASC
)
