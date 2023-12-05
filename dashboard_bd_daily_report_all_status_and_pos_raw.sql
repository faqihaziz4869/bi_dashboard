--------------Update New Query for Soscom Dashboard-----------------

WITH bd_order AS (

  SELECT

oo.waybill_no,
oo.order_no,
oo.ecommerce_order_no,
t0.option_name AS order_source,
oo.parent_shipping_cleint vip_username,
oo.vip_customer_name sub_account,

DATETIME(oo.input_time,'Asia/Jakarta') input_time,
oo.scheduling_target_branch_name scheduled_branch,
DATETIME(oo.start_pickup_time,'Asia/Jakarta') start_pickup_time,
DATETIME(oo.end_pickup_time,'Asia/Jakarta') end_pickup_time,

t4.register_reason_bahasa as pickup_failure_reason, 
DATETIME(oo.pickup_failure_time,'Asia/Jakarta') as pickup_failure_time,
DATETIME(oo.pickup_time,'Asia/Jakarta') pickup_time,
oo.pickup_branch_name,
t1.option_name AS order_status,

oo.sender_name,
oo.sender_cellphone, 
oo.sender_province_name,
oo.sender_city_name,
oo.sender_district_name,
t2.kanwil_name AS kanwil_asal,

oo.recipient_name,
oo.recipient_address,
oo.recipient_cellphone,
oo.recipient_province_name,
oo.recipient_city_name,
oo.recipient_district_name,
t3.kanwil_name as kanwil_tujuan,

rd6.option_name AS express_type,
rd11.option_name AS service_type,
rd3.option_name AS payment_type,

-- tambahan kolom baru estimasi biaya & berat
oo.standard_shipping_fee standard_shipping_fee_oo,
oo.total_shipping_fee total_shipping_fee_oo,
oo.insurance_amount insurance_amount_oo,
oo.handling_fee handling_fee_oo,
oo.other_fee other_fee_oo,
oo.item_weight item_weight_oo,
oo.item_actual_weight item_actual_weight_oo,
oo.item_calculated_weight item_calculated_weight_oo,        
oo.item_value item_value_oo,
oo.item_name item_name_oo,

      CASE 
          WHEN oo.pickup_time IS NOT NULL AND oo.start_pickup_time IS NOT NULL 
          THEN DATE_DIFF(DATE(oo.pickup_time,'Asia/Jakarta'), DATE(oo.start_pickup_time,'Asia/Jakarta'), day) 
          WHEN oo.pickup_time IS NOT NULL AND oo.start_pickup_time IS NULL 
          THEN DATE_DIFF(DATE(oo.pickup_time,'Asia/Jakarta'), DATE(oo.input_time, 'Asia/Jakarta'), day)
      END AS durasi_pickup,

t21.division AS sales_division_oo, --sales_division,



  FROM `datawarehouse_idexp.order_order` oo
LEFT OUTER JOIN `datawarehouse_idexp.system_option` t0 ON t0.option_value = oo.order_source AND t0.type_option = 'orderSource'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = oo.order_status AND t1.type_option = 'orderStatus'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd3 ON rd3.option_value = oo.payment_type  AND rd3.type_option = 'paymentType'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd6 ON rd6.option_value = oo.express_type  AND rd6.type_option = 'expressType'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd11 ON rd11.option_value = oo.service_type AND rd11.type_option = 'serviceType'
LEFT join `grand-sweep-324604.datawarehouse_idexp.res_problem_package` t4 on oo.pickup_failure_problem_code = t4.code and t4.deleted = '0' --tambahan
LEFT OUTER join `datamart_idexp.mapping_kanwil_area` t2 on oo.sender_province_name = t2.province_name --origin kanwil
LEFT OUTER join `datamart_idexp.mapping_kanwil_area` t3 on oo.recipient_province_name = t3.province_name --destinasi kanwil
LEFT JOIN `grand-sweep-324604.datamart_idexp.masterdata_sales_source` t21 ON t21.source = t0.option_name --waybill_source
LEFT JOIN `grand-sweep-324604.datamart_idexp.masterdata_sales_seller_vip`t31 ON t31.seller_name = oo.vip_customer_name --vip_name


WHERE DATE(oo.input_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))


QUALIFY ROW_NUMBER() OVER (PARTITION BY oo.waybill_no ORDER BY oo.input_time DESC)=1
),

bd_order_sla_pickup AS (

SELECT *,

CASE
      WHEN sla_pickup_status = 'Late' AND (pickup_failure_time < end_pickup_time) THEN 'Adjusted'
      WHEN order_source NOT IN ('Tokopedia') AND sla_pickup_status = 'Late' AND pickup_failure_reason IN (
            'Pengiriman akan menggunakan ekspedisi lain', 'Pengiriman dibatalkan','Pengirim akan mengantar paket ke cabang',
            'Pengirim tidak ada di lokasi/toko','Alamat pengirim kurang jelas', 'Pengirim sedang mempersiapkan paket',
            'Pengirim meminta pergantian jadwal','Paket belum ada/belum selesai dikemas','Pengirim tidak dapat dihubungi',
            'Pengirim merasa tidak menggunakan iDexpress','Pengirim sedang libur','Pengirim sebagai dropshipper dan menunggu supplier',
            'Paket pre order', 'Paket sedang disiapkan','Pengiriman dibatalkan sebelum di pickup') THEN 'Adjusted'
      ELSE sla_pickup_status
END AS adjusted_performance_pickup,

FROM (
  SELECT *,
      --drop off => IF durasi dari waktu_input dan pickup_time > 2 maka late
      --if pickup time Null dan current date > 2 hari dari input time maka late

-- SLA Pickup (dari order_order)
      CASE
      WHEN service_type = 'Pick up' AND pickup_time IS NOT NULL AND pickup_time <= end_pickup_time THEN 'On Time'
      WHEN service_type = 'Pick up' AND pickup_time IS  NULL AND end_pickup_time > CURRENT_DATE('Asia/Jakarta') THEN 'Late'
      WHEN service_type = 'Pick up' AND pickup_time IS NOT NULL AND pickup_time > end_pickup_time THEN 'Late'  
      WHEN service_type = 'Pick up' AND pickup_time IS NULL AND end_pickup_time <= CURRENT_DATE('Asia/Jakarta') THEN "Not Pick up Yet"
      END AS sla_pickup_status,

      CASE
      WHEN durasi_pickup < 0 THEN 'Early Pickup'
      WHEN durasi_pickup = 0 THEN 'H0'
      WHEN durasi_pickup = 1 THEN 'H+1'
      WHEN durasi_pickup = 2 THEN 'H+2' 
      WHEN durasi_pickup > 2 THEN 'H2+' 
      END AS pickup_rate,


      CASE WHEN order_status = 'Cancel Order' THEN 'Cancel Order'
      WHEN order_status = 'Picked Up' THEN 'Picked Up'
      WHEN order_status NOT IN ('Cancel Order','Picked Up' ) THEN 'Not Picked Up'
      END AS pickup_status,

FROM bd_order      

QUALIFY ROW_NUMBER() OVER (PARTITION BY waybill_no ORDER BY input_time DESC)=1
)),


bd_waybill AS (

  SELECT

ww.waybill_no,
ww.order_no,
ww.ecommerce_order_no,
t0.option_name AS waybill_source,
ww.parent_shipping_cleint vip_username,
ww.vip_customer_name sub_account,

DATETIME(ww.shipping_time, 'Asia/Jakarta') shipping_time,
ww.pickup_branch_name,
rd13.option_name AS waybill_status,
rd12.option_name AS void_status,

ww.sender_name,
ww.sender_cellphone,
ww.sender_province_name,
ww.sender_city_name,
ww.sender_district_name,
t2.kanwil_name AS kanwil_asal_ww,

ww.recipient_name,
ww.recipient_address,
ww.recipient_cellphone,
ww.recipient_province_name,
ww.recipient_city_name,
ww.recipient_district_name,
t3.kanwil_name AS kanwil_tujuan_ww,

et.option_name AS express_type,
st.option_name AS service_type,
pt.option_name AS payment_type,

CAST(ww.item_value AS NUMERIC) AS item_value_ww,
ww.item_name item_name_ww,
ww.standard_shipping_fee standard_shipping_fee_ww,
ww.total_shipping_fee total_shipping_fee_ww, 
ww.insurance_amount insurance_amount_ww,
ww.handling_fee handling_fee_ww,
ww.receivable_shipping_fee receivable_shipping_fee_ww, 
ww.other_fee other_fee_ww,
ww.cod_amount as cod_amount,
IF(ww.cod_amount>0,'COD','Non COD') AS cod_type,
ww.item_weight as item_weight_ww,
ww.item_actual_weight as item_actual_weight_ww,
ww.item_calculated_weight as item_calculated_weight_ww,

SAFE_CAST(t8.sla AS int64) AS sla, 
DATE_ADD(DATE(ww.shipping_time, 'Asia/Jakarta'), INTERVAL SAFE_CAST(t8.sla AS int64) DAY) AS sla_est_deadline,
DATETIME(ww.pod_record_time, 'Asia/Jakarta') pod_record_time,
ww.pod_branch_name,
ww.pod_photo_url,

pu2.pulau AS shipment_area,
pu5.pulau AS area_tujuan,

---------------------return--------------------------
DATETIME(rr.return_record_time, 'Asia/Jakarta') return_record_time,
t20.return_type as return_reason, 
rd15.option_name AS return_confirm_status,
DATETIME(rr.return_confirm_record_time,'Asia/Jakarta') return_confirm_record_time,
pu3.pulau AS return_area,
rd14.option_name AS return_status,
rr.return_shipping_fee, 
DATETIME(rr.return_pod_record_time, 'Asia/Jakarta') return_pod_record_time,
rr.return_pod_photo_url,

ww.pod_courier_name, --tambah kolom
ww.signer, --tambah kolom
rt.option_name AS relationship_type, --tambah kolom

        

FROM `grand-sweep-324604.datawarehouse_idexp.waybill_waybill` ww 
LEFT OUTER JOIN `datawarehouse_idexp.waybill_return_bill` rr ON ww.waybill_no = rr.waybill_no
AND DATE(rr.update_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))
LEFT OUTER JOIN `datamart_idexp.masterdata_sla_shopee` t8 ON ww.recipient_city_name = t8.destination_city AND ww.sender_city_name = t8.origin_city
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd13 ON rd13.option_value = ww.waybill_status AND rd13.type_option = 'waybillStatus'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd14 ON rd14.option_value = ww.return_flag AND rd14.type_option = 'returnFlag'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd12 ON rd12.option_value = ww.void_flag AND rd12.type_option = 'voidFlag'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` t0 ON t0.option_value = ww.waybill_source AND t0.type_option = 'waybillSource'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` st ON st.option_value = ww.service_type AND st.type_option = 'serviceType'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` et ON et.option_value = ww.express_type AND et.type_option = 'expressType'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` pt ON pt.option_value = ww.payment_type AND pt.type_option = 'paymentType'
LEFT OUTER JOIN `datamart_idexp.masterdata_city_mapping_area_island_new` pu5 ON ww.recipient_city_name = pu5.city AND ww.recipient_province_name = pu5.province --Area_Tujuan
LEFT OUTER JOIN `datamart_idexp.masterdata_city_mapping_area_island_new` pu2 ON ww.sender_city_name = pu2.city and ww.sender_province_name = pu2.province --Shipment_area
LEFT OUTER join `datamart_idexp.mapping_kanwil_area` t2 on ww.sender_province_name = t2.province_name --origin kanwil
LEFT OUTER join `datamart_idexp.mapping_kanwil_area` t3 on ww.recipient_province_name = t3.province_name --destinasi kanwil

LEFT OUTER JOIN `datamart_idexp.masterdata_city_mapping_area_island_new` pu3 ON rr.recipient_city_name = pu3.city and rr.recipient_province_name = pu3.province --Return_area, 
LEFT OUTER join `grand-sweep-324604.datawarehouse_idexp.return_type` t20 on rr.return_type_id = t20.id
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd15 ON rd15.option_value = rr.return_confirm_status AND rd15.type_option = 'returnConfirmStatus'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rt ON rt.option_value = ww.relationship_type AND rt.type_option = 'relation'



WHERE DATE(ww.shipping_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))
AND DATE(ww.update_time, 'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))

QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1

),

bd_order_to_waybill AS (

  SELECT

CASE WHEN oo.waybill_no IS NULL THEN ww.waybill_no ELSE oo.waybill_no END AS waybill_no,
CASE WHEN oo.order_no IS NULL THEN ww.order_no ELSE oo.order_no END AS order_no,
CASE WHEN oo.ecommerce_order_no IS NULL THEN ww.ecommerce_order_no ELSE oo.ecommerce_order_no END AS ecommerce_order_no,
CASE WHEN oo.order_source IS NULL THEN ww.waybill_source ELSE oo.order_source END AS order_source,
CASE WHEN oo.pickup_time IS NULL THEN oo.vip_username ELSE ww.vip_username END AS vip_username,
CASE WHEN oo.pickup_time IS NULL THEN oo.sub_account ELSE ww.sub_account END AS sub_account,
CASE 
  WHEN oo.input_time IS NULL THEN ww.shipping_time ELSE oo.input_time END AS input_time,
-- oo.start_pickup_time,
-- oo.end_pickup_time,

oo.pickup_failure_reason,
oo.pickup_failure_time,
CASE 
  WHEN oo.pickup_time IS NULL THEN ww.shipping_time ELSE oo.pickup_time END AS pickup_time,
CASE
  WHEN oo.pickup_time IS NULL THEN oo.scheduled_branch 
  WHEN oo.pickup_time IS NOT NULL THEN ww.pickup_branch_name END AS scheduling_or_pickup_branch,
oo.order_status,
CASE 
  WHEN oo.sender_name IS NULL THEN ww.sender_name ELSE oo.sender_name END AS sender_name,
CASE 
  WHEN oo.sender_cellphone IS NULL THEN ww.sender_cellphone ELSE oo.sender_cellphone END AS sender_cellphone,
CASE 
  WHEN oo.sender_province_name IS NULL THEN ww.sender_province_name ELSE oo.sender_province_name END AS sender_province_name,
CASE 
  WHEN oo.sender_city_name IS NULL THEN ww.sender_city_name ELSE oo.sender_city_name END AS sender_city_name,
CASE 
  WHEN oo.sender_district_name IS NULL THEN ww.sender_district_name ELSE oo.sender_district_name END AS sender_district_name,
CASE 
  WHEN oo.kanwil_asal IS NULL THEN ww.kanwil_asal_ww ELSE oo.kanwil_asal END AS kanwil_asal,
CASE 
  WHEN oo.recipient_name IS NULL THEN ww.recipient_name ELSE oo.recipient_name END AS recipient_name,
CASE 
  WHEN oo.recipient_address IS NULL THEN ww.recipient_address ELSE oo.recipient_address END AS recipient_address,
CASE 
  WHEN oo.recipient_cellphone IS NULL THEN ww.recipient_cellphone ELSE oo.recipient_cellphone END AS recipient_cellphone,
CASE 
  WHEN oo.recipient_province_name IS NULL THEN ww.recipient_province_name ELSE oo.recipient_province_name END AS recipient_province_name,
CASE 
  WHEN oo.recipient_city_name IS NULL THEN ww.recipient_city_name ELSE oo.recipient_city_name END AS recipient_city_name,
CASE 
  WHEN oo.recipient_district_name IS NULL THEN ww.recipient_district_name ELSE oo.recipient_district_name END AS recipient_district_name,
CASE 
  WHEN oo.kanwil_tujuan IS NULL THEN ww.kanwil_tujuan_ww ELSE oo.kanwil_tujuan END AS kanwil_tujuan,
CASE 
  WHEN oo.express_type IS NULL THEN ww.express_type ELSE oo.express_type END AS express_type,
CASE 
  WHEN oo.service_type IS NULL THEN ww.service_type ELSE oo.service_type END AS service_type,
CASE 
  WHEN oo.payment_type IS NULL THEN ww.payment_type ELSE oo.payment_type END AS payment_type,

oo.standard_shipping_fee_oo,
oo.total_shipping_fee_oo,
oo.insurance_amount_oo,
oo.handling_fee_oo,
oo.other_fee_oo,

ww.standard_shipping_fee_ww,
ww.total_shipping_fee_ww,
ww.insurance_amount_ww,
ww.handling_fee_ww,
ww.other_fee_ww,
ww.receivable_shipping_fee_ww,

oo.item_weight_oo,
oo.item_actual_weight_oo,
oo.item_calculated_weight_oo,

ww.item_weight_ww,
ww.item_actual_weight_ww,
ww.item_calculated_weight_ww,

CASE 
  WHEN oo.pickup_time IS NOT NULL THEN ww.item_name_ww ELSE oo.item_name_oo END AS item_name,
CASE 
  WHEN oo.pickup_time IS NOT NULL THEN ww.item_value_ww ELSE oo.item_value_oo END AS item_value,

ww.cod_amount,
ww.cod_type,
ww.void_status,

oo.durasi_pickup,
oo.sales_division_oo,
oo.sla_pickup_status,
oo.pickup_rate,
oo.pickup_status,
oo.adjusted_performance_pickup,

ww.waybill_status,
ww.sla,
ww.sla_est_deadline,
ww.pod_record_time,
ww.pod_branch_name,

DATE_DIFF(DATE(ww.pod_record_time), DATE(ww.shipping_time), DAY) AS lead_time_deliv, --waktu shipping s/d pod

  CASE 
      WHEN DATE_DIFF(DATE(ww.pod_record_time), DATE(ww.shipping_time), DAY) <= sla THEN 'Hit SLA'
      WHEN DATE_DIFF(DATE(ww.pod_record_time), DATE(ww.shipping_time), DAY) > sla THEN 'Not Hit'
      WHEN DATE_DIFF(DATE(ww.pod_record_time), DATE(ww.shipping_time), DAY) IS NULL THEN NULL
      END AS sla_status_deliv,
CASE
      WHEN ww.waybill_status IN ('Signed') OR ww.pod_record_time IS NOT NULL THEN 'Delivered'
      ELSE NULL END AS pod_status, 

---------------------return--------------------------
ww.return_record_time,
ww.return_reason, 
ww.return_confirm_status,
ww.return_confirm_record_time,
ww.return_area,
ww.return_status,
ww.return_shipping_fee, 
ww.return_pod_record_time,
ww.return_pod_photo_url,

ww.shipment_area,
ww.area_tujuan,
ww.pod_photo_url,

ww.pod_courier_name, --tambah kolom
ww.signer, --tambah kolom
ww.relationship_type, --tambah kolom


  FROM bd_order_sla_pickup oo
  FULL JOIN bd_waybill ww ON oo.waybill_no = ww.waybill_no
 
),

first_deliv_attempt AS (

SELECT
sc.waybill_no,
MIN(DATETIME(sc.record_time,'Asia/Jakarta')) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS first_deliv_attempt,
MIN(rd16.option_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS scan_type,

FROM `datawarehouse_idexp.waybill_waybill_line` sc 
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd16 ON rd16.option_value = sc.operation_type AND rd16.type_option = 'operationType'

WHERE 
DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))
AND operation_type = "09"

QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC)=1

),

last_deliv_attempt AS (

SELECT
sc.waybill_no,
MAX(DATETIME(sc.record_time,'Asia/Jakarta')) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) AS last_deliv_attempt,
MAX(rd16.option_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) AS scan_type,

FROM `datawarehouse_idexp.waybill_waybill_line` sc 
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd16 ON rd16.option_value = sc.operation_type AND rd16.type_option = 'operationType'

WHERE 
DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))
AND operation_type = "09"

QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC)=1

),

count_deliv_attempt AS (

SELECT
sc.waybill_no,
COUNT(rd16.option_name) OVER (PARTITION BY sc.waybill_no) AS count_deliv_attempt,

FROM `datawarehouse_idexp.waybill_waybill_line` sc 
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd16 ON rd16.option_value = sc.operation_type AND rd16.type_option = 'operationType'

WHERE 
DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))
AND operation_type = "09"

QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no)=1

),

fisrt_pos as(
  SELECT
        ps.waybill_no,
        MIN(ps.problem_reason) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time ASC) first_pos_reason,
        MIN(DATETIME(ps.operation_time,'Asia/Jakarta')) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time ASC) AS first_pos_attempt,
        MIN(prt.option_name) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time ASC) first_pos_type,
        MIN(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS first_pos_location,
        MIN(sc.photo_url) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time ASC) AS first_pos_photo_url,

              FROM `datawarehouse_idexp.waybill_problem_piece` ps
              LEFT OUTER JOIN `datawarehouse_idexp.waybill_waybill_line` sc ON ps.waybill_no = sc.waybill_no
              AND DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY)) AND sc.operation_type IN ('18') AND sc.problem_type NOT IN ('02')
              
              LEFT join `grand-sweep-324604.datawarehouse_idexp.res_problem_package` t4 on sc.problem_code = t4.code and t4.deleted = '0'
              LEFT OUTER JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = sc.problem_type AND t1.type_option = 'problemType'
              LEFT OUTER JOIN `datawarehouse_idexp.system_option` prt ON ps.problem_type  = prt.option_value AND prt.type_option = 'problemType'
              WHERE DATE(ps.operation_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))

              AND ps.problem_type NOT IN ('02')

              QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.waybill_no ORDER BY sc.record_time ASC)=1
        ),

 last_pos as(
  SELECT
        ps.waybill_no,
        MAX(ps.problem_reason) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) AS last_pos_reason,
        MAX(DATETIME(ps.operation_time,'Asia/Jakarta')) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) AS last_pos_attempt,
        MAX(prt.option_name) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) last_pos_type,
        MAX(sc.operation_branch_name) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) AS last_pos_location,
        MAX(sc.photo_url) OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC) AS last_pos_photo_url,

              FROM `datawarehouse_idexp.waybill_problem_piece` ps
              LEFT OUTER JOIN `datawarehouse_idexp.waybill_waybill_line` sc ON ps.waybill_no = sc.waybill_no
                  AND DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY)) AND sc.operation_type IN ('18') 
                  AND sc.problem_type NOT IN ('02')
              LEFT join `grand-sweep-324604.datawarehouse_idexp.res_problem_package` t4 on sc.problem_code = t4.code and t4.deleted = '0'
              LEFT OUTER JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = sc.problem_type AND t1.type_option = 'problemType'
              LEFT OUTER JOIN `datawarehouse_idexp.system_option` prt ON ps.problem_type  = prt.option_value AND prt.type_option = 'problemType'
              WHERE DATE(ps.operation_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))

              AND ps.problem_type NOT IN ('02')

              QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.waybill_no ORDER BY sc.record_time DESC)=1
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

              WHERE DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))

              QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no ORDER BY sc.record_time DESC)=1
        ),

count_pos as(
  SELECT
        sc.waybill_no,
        COUNT(t4.register_reason_bahasa) OVER (PARTITION BY sc.waybill_no) AS count_pos,

              FROM `datawarehouse_idexp.waybill_waybill_line` sc
              LEFT join `grand-sweep-324604.datawarehouse_idexp.res_problem_package` t4 on sc.problem_code = t4.code and t4.deleted = '0'
              LEFT OUTER JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = sc.problem_type AND t1.type_option = 'problemType'
              WHERE DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -93 DAY))

              AND sc.operation_type IN ('18')

              QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no)=1
        )

SELECT

waybill_no,
order_no,
ecommerce_order_no,
input_time waktu_input,
order_status,
pickup_time,
pickup_failure_reason,
sender_name nama_pengirim,
sender_cellphone no_hp_pengirim,
order_source,
vip_username,
sub_account sub_akun,
scheduling_or_pickup_branch,
sender_district_name kec_asal,
sender_city_name kota_asal,
sender_province_name provinsi_asal,
kanwil_asal,
shipment_area,
recipient_name nama_penerima,
service_type tipe_service,
express_type tipe_ekspress,
payment_type tipe_pembayaran,
item_name,
standard_shipping_fee_oo estimasi_biaya_standar_pengiriman,
total_shipping_fee_oo estimasi_total_biaya,
insurance_amount_oo estimasi_biaya_asuransi,
handling_fee_oo estimasi_biaya_penanganan,
other_fee_oo estimasi_biaya_lain,
standard_shipping_fee_ww biaya_standar_pengiriman,
total_shipping_fee_ww biaya_total_pengiriman,
receivable_shipping_fee_ww biaya_piutang_pengiriman,
insurance_amount_ww biaya_asuransi,
handling_fee_ww biaya_penanganan,
other_fee_ww biaya_lain,
void_status,
item_value,
cod_amount,
cod_type,
item_weight_oo estimasi_berat,
item_actual_weight_oo estimasi_berat_aktual,
item_calculated_weight_oo estimasi_kalkulasi_berat,
item_weight_ww berat,
item_actual_weight_ww berat_aktual,
item_calculated_weight_ww kalkulasi_berat,
recipient_address alamat_penerima,
recipient_cellphone no_hp_penerima,
waybill_status,
last_scan_time,
last_activity last_activity,
last_location last_location,
first_deliv_attempt,
last_deliv_attempt,
pod_branch_name destinasi_cabang_pod,
pod_record_time waktu_perekaman_pod,
pod_photo_url,
recipient_district_name tujuan_kec,
recipient_city_name kota_tujuan,
recipient_province_name provinsi_tujuan,
kanwil_tujuan,
area_tujuan,
return_status,
return_shipping_fee,
return_confirm_status,
return_reason,
return_pod_photo_url,
return_pod_record_time,
return_area,
first_pos_location,
first_pos_attempt,
first_pos_reason,
first_pos_photo_url,
last_pos_attempt,
last_pos_type,
last_pos_reason,
last_pos_photo_url,
CASE WHEN count_deliv_attempt IS NULL THEN 0 ELSE count_deliv_attempt END AS count_deliv_attempt,
CASE WHEN count_pos IS NULL THEN 0 ELSE count_pos END AS count_pos,
CASE WHEN void_status = 'Void' THEN 'Cancel Order' ELSE last_status END AS last_status,

pod_courier_name pod_courier_name_ww, --tambah kolom
signer signer_ww, --tambah kolom
relationship_type relationship_type_ww, --tambah kolom
durasi_pickup, --tambah kolom
pickup_rate, --tambah kolom
pickup_failure_time, --tambah kolom
sla_pickup_status, --tambah kolom
adjusted_performance_pickup,
deliv_problem_agg, --tambah kolom
last_pos_category, --tambah kolom
lead_time_deliv, -- tambah kolom
sla_est_deadline, -- tambah kolom
sla_status_deliv, -- tambah kolom
pickup_status pickup_status_oo, -- tambah kolom

FROM (
SELECT 
a.*, 
b.first_deliv_attempt,
h.last_deliv_attempt,
c.count_deliv_attempt,
    d.first_pos_reason,
    d.first_pos_attempt,
    d.first_pos_location,
    d.first_pos_photo_url,
e.last_pos_type,
e.last_pos_reason,
e.last_pos_attempt,
e.last_pos_location,
e.last_pos_photo_url,
    f.last_location,
    f.last_activity,
    f.last_scan_time,
g.count_pos,

CASE 
    WHEN e.last_pos_reason IN ('Kemasan paket rusak','Paket rusak/pecah', 'Kerusakan pada resi / informasi resi tidak jelas','Damaged parcels',
'Information on AWB is unclear/damage','Packaging is damage','Paket hilang atau tidak ditemukan', 'Parcels is lost or cannot be found','Package is lost','Paket crosslabel','Paket salah dalam proses penyortiran','Data alamat tidak sesuai dengan kode sortir') THEN e.last_pos_reason
    END AS deliv_problem_agg,


-- **** new column ****
      -----Problem delivery category lengkap
      CASE
      --bea cukai
      WHEN e.last_pos_reason LIKE '%bea cukai%' OR e.last_pos_reason LIKE '%Rejected by customs%' THEN 'Bea Cukai (Red Line)'
      --damaged
      WHEN e.last_pos_reason IN ('Kemasan paket rusak','Paket rusak/pecah', 'Kerusakan pada resi / informasi resi tidak jelas','Damaged parcels','Information on AWB is unclear/damage','Packaging is damage','Kemasan paket tidak sesuai prosedur','Pengemasan paket dengan kemasan rusak') THEN 'Damaged'
      --lost
      WHEN e.last_pos_reason IN ('Paket hilang atau tidak ditemukan', 'Parcels is lost or cannot be found','Package is lost','Paket Rusak') THEN 'Lost'
      --criscross
      WHEN e.last_pos_reason IN ('Paket crosslabel') THEN 'CrisCross'
      --missroute
      WHEN e.last_pos_reason IN ('Paket salah dalam proses penyortiran','Data alamat tidak sesuai dengan kode sortir','Paket akan dikembalikan ke cabang asal','Paket salah dikirimkan/salah penyortiran') THEN 'Missroute'
      --uncover
      WHEN e.last_pos_reason IN ('Di luar cakupan area cabang, akan dijadwalkan ke cabang lain') THEN 'Uncoverage' 
      --bad address 
      WHEN e.last_pos_reason IN ('Pelanggan ingin dikirim ke alamat berbeda','Alamat pelanggan salah/sudah pindah alamat') THEN 'Bad address'
      --Delivery attempt problem
      WHEN e.last_pos_reason IN ('Terdapat barang berbahaya (Dangerous Goods)','Berat paket tidak sesuai','Penerima ingin membuka paket sebelum membayar') THEN 'Delivery attempt problem'
      --Force Majeur
      WHEN e.last_pos_reason IN ('Cuaca buruk / bencana alam') THEN 'Force Majeur'
      --Recipient cannot be contacted
      WHEN e.last_pos_reason IN ('Nomor telepon yang tertera tidak dapat dihubungi','Nomor telepon yang tertera tidak dapat dihubungi atau alamat tidak jelas') THEN 'Recipient cannot be contacted'
      --Recipient not at home
      WHEN e.last_pos_reason IN ('Pelanggan libur akhir pekan/libur panjang','Reschedule pengiriman dengan penerima','Toko atau kantor sudah tutup','Pelanggan tidak di lokasi','Sudah Reschedule dengan Customer','Pelanggan tidak di tempat','Kantor atau toko tutup','Telepon bermasalah, tidak dapat dihubungi','Penjadwalan Ulang') THEN 'Recipient not at home'
      --Rejected by recipient
      WHEN e.last_pos_reason IN ('Penerima menolak menerima paket') THEN 'Rejected by recipient'
      --Selfpickup by recipient
      WHEN e.last_pos_reason IN ('Pelanggan berinisiatif mengambil paket di cabang','Penerima mengambil sendiri paket di DP') THEN 'Selfpickup by recipient'
      --Shipment cancellation
      WHEN e.last_pos_reason IN ('Pengiriman dibatalkan','Paket akan diproses dengan nomor resi yang baru','Pelanggan membatalkan pengiriman','Pengiriman dibatalkan sebelum di pickup') THEN 'Shipment cancellation'
      --Shipment on hold
      WHEN e.last_pos_reason IN ('Penundaan jadwal armada pengiriman','Paket makanan, disimpan hingga waktu pengiriman yang tepat','Food parcels, kept until proper delivery time','Melewati jam operasional cabang') THEN 'Shipment on hold'
      --other 
      WHEN e.last_pos_reason IN ('Paket hilang ditemukan','Pelanggan menunggu paket yang lain untuk dikirim','Indikasi kecurangan pengiriman') THEN 'Other'
      END AS last_pos_category,

CASE WHEN a.order_status = 'Cancel Order' THEN 'Cancel Order'
    WHEN a.waybill_status IS NULL OR a.void_status = 'Void' THEN 'Cancel Order'
    WHEN a.waybill_status IN ('Signed') OR a.pod_record_time IS NOT NULL THEN 'Delivered'
    WHEN a.waybill_status IN ('Return Received') OR a.return_pod_record_time IS NOT NULL THEN 'Returned'
    WHEN a.pod_record_time IS NULL AND return_confirm_record_time IS NOT NULL AND a.return_pod_record_time IS NULL AND (a.return_status = 'Retur') THEN 'Return Process' 
    WHEN a.pod_record_time IS NULL AND e.last_pos_reason IN ('Paket hilang atau tidak ditemukan', 'Parcels is lost or cannot be found','Package is lost') then 'Undelivered'
	WHEN a.pod_record_time IS NULL AND e.last_pos_reason LIKE '%bea cukai%' OR e.last_pos_reason LIKE '%Rejected by customs%' THEN 'Undelivered'
    WHEN e.last_pos_reason IN ('Kemasan paket rusak','Paket rusak/pecah', 'Kerusakan pada resi / informasi resi tidak jelas','Damaged parcels','Information on AWB is unclear/damage','Packaging is damage') THEN 'Undelivered'
    WHEN a.pod_record_time IS NULL AND a.waybill_status NOT IN ('Returning','Return Received','Pickup Failure','Signed') THEN 'Delivery Process'
    ELSE 'Delivery Process' END AS last_status,


-- FROM bd_order_sla_pickup
FROM bd_order_to_waybill a
LEFT OUTER JOIN first_deliv_attempt b ON a.waybill_no = b.waybill_no
LEFT OUTER JOIN count_deliv_attempt c ON a.waybill_no = c.waybill_no
LEFT OUTER JOIN fisrt_pos d ON a.waybill_no = d.waybill_no
LEFT OUTER JOIN last_pos e ON a.waybill_no = e.waybill_no
LEFT OUTER JOIN last_location f ON a.waybill_no = f.waybill_no
LEFT OUTER JOIN count_pos g ON a.waybill_no = g.waybill_no
LEFT OUTER JOIN last_deliv_attempt h ON a.waybill_no = h.waybill_no
)
