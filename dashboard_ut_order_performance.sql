-----------------Update Query for UT Dashboard-----------------

WITH kpi_order AS (

  SELECT

oo.waybill_no,
oo.order_no,
oo.ecommerce_order_no,
t0.option_name AS order_source,
oo.parent_shipping_cleint vip_username,
oo.vip_customer_name sub_account,

DATETIME(oo.input_time,'Asia/Jakarta') input_time,
DATETIME(oo.update_time,'Asia/Jakarta') update_time,
oo.scheduling_target_branch_name scheduled_branch,
DATETIME(oo.start_pickup_time,'Asia/Jakarta') start_pickup_time,
DATETIME(oo.end_pickup_time,'Asia/Jakarta') end_pickup_time,

t4.register_reason_bahasa as pickup_failure_reason, 
DATETIME(oo.pickup_failure_time,'Asia/Jakarta') AS pickup_failure_time,
DATETIME(oo.pickup_time,'Asia/Jakarta') pickup_time,
oo.pickup_branch_name,
t1.option_name AS order_status,

oo.sender_name,
oo.sender_province_name,
oo.sender_city_name,
oo.sender_district_name,
t2.kanwil_name AS kanwil_area_pickup,
pu2.pulau AS origin_area,
oo.recipient_province_name,
oo.recipient_city_name,
oo.recipient_district_name,
t3.kanwil_name as kanwil_tujuan,
pu5.pulau AS destination_area,
rd11.option_name AS service_type,

CASE 
      WHEN oo.pickup_time IS NOT NULL AND oo.start_pickup_time IS NOT NULL 
      THEN DATE_DIFF(DATE(oo.pickup_time,'Asia/Jakarta'), DATE(oo.start_pickup_time,'Asia/Jakarta'), day) 
      WHEN oo.pickup_time IS NOT NULL AND oo.start_pickup_time IS NULL 
      THEN DATE_DIFF(DATE(oo.pickup_time,'Asia/Jakarta'), DATE(oo.input_time, 'Asia/Jakarta'), day)
      END AS durasi_pickup,

 -----------------------Mapping cut off for Blibli -------------------------------------------
                        
IF(TIME(oo.input_time, 'Asia/Jakarta') >= "14:00:00",1,0) AS order_past_1400,

-----------------------------------Mapping cut off for Soscom -----------------------

IF(TIME(oo.input_time, 'Asia/Jakarta') >= "15:00:00",1,0) AS order_past_1500,

------------mapping sales division------------
IF(t0.option_name = 'VIP Customer Portal',t31.division, t21.division) AS division, --t3 : division_seller, t2 : division_source
IF(t0.option_name = 'VIP Customer Portal',t31.sales_name, t21.sales_name) as sales_name,

oo.recipient_name,
oo.recipient_cellphone,
oo.recipient_address,


FROM `datawarehouse_idexp.order_order` oo
LEFT OUTER JOIN `datawarehouse_idexp.system_option` t0 ON t0.option_value = oo.order_source AND t0.type_option = 'orderSource'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = oo.order_status AND t1.type_option = 'orderStatus'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd11 ON rd11.option_value = oo.service_type AND rd11.type_option = 'serviceType'
LEFT join `grand-sweep-324604.datawarehouse_idexp.res_problem_package` t4 on oo.pickup_failure_problem_code = t4.code and t4.deleted = '0' --tambahan
LEFT OUTER join `datamart_idexp.mapping_kanwil_area` t2 on oo.sender_province_name = t2.province_name --origin kanwil
LEFT OUTER join `datamart_idexp.mapping_kanwil_area` t3 on oo.recipient_province_name = t3.province_name --destinasi kanwil
LEFT JOIN `grand-sweep-324604.datamart_idexp.masterdata_sales_source` t21 ON t21.source = t0.option_name --waybill_source
LEFT JOIN `grand-sweep-324604.datamart_idexp.masterdata_sales_seller_vip`t31 ON t31.seller_name = oo.vip_customer_name --vip_name
LEFT OUTER JOIN `datamart_idexp.masterdata_city_mapping_area_island_new` pu5 ON oo.recipient_city_name = pu5.city AND oo.recipient_province_name = pu5.province --Area_Tujuan
LEFT OUTER JOIN `datamart_idexp.masterdata_city_mapping_area_island_new` pu2 ON oo.sender_city_name = pu2.city and oo.sender_province_name = pu2.province --Shipment_area


WHERE DATE(oo.input_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -186 DAY))


QUALIFY ROW_NUMBER() OVER (PARTITION BY oo.waybill_no ORDER BY oo.update_time DESC)=1


),

order_to_pickup AS (

  SELECT *,

 -------------------Adjusted SLA Pickup -------------------------------
CASE
      WHEN order_source IN ('Tokopedia') AND pickup_performance_everpro = 'Late' AND DATETIME(pickup_failure_time) <= DATETIME(end_pickup_time) AND pickup_failure_reason IN ('Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Pengirim akan mengantar paket ke cabang','Pengirim tidak ada di lokasi/toko','Alamat pengirim kurang jelas','Pengirim sedang mempersiapkan paket','Pengirim meminta pergantian jadwal','Paket belum ada/belum selesai dikemas','Pengirim tidak dapat dihubungi','Pengirim merasa tidak menggunakan iDexpress','Pengirim sedang libur','Pengirim sebagai dropshipper dan menunggu supplier','Paket pre order','Paket sedang disiapkan') THEN 'Adjusted'
      WHEN order_source NOT IN ('Tokopedia') AND pickup_performance_everpro = 'Late' AND pickup_failure_reason IN ('Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Pengirim akan mengantar paket ke cabang','Pengirim tidak ada di lokasi/toko','Alamat pengirim kurang jelas','Pengirim sedang mempersiapkan paket','Pengirim meminta pergantian jadwal','Paket belum ada/belum selesai dikemas','Pengirim tidak dapat dihubungi','Pengirim merasa tidak menggunakan iDexpress','Pengirim sedang libur','Pengirim sebagai dropshipper dan menunggu supplier','Paket pre order','Paket sedang disiapkan','Pengiriman dibatalkan sebelum di pickup') THEN 'Adjusted'
      ELSE pickup_performance_everpro
      END AS adjusted_performance_everpro,


  FROM (

    SELECT

waybill_no,
order_no,
ecommerce_order_no,
order_source,
vip_username,
sub_account,
input_time,
update_time,
scheduled_branch,
start_pickup_time,
end_pickup_time,
pickup_failure_reason,
pickup_failure_time,
pickup_time,
pickup_branch_name,
order_status,
sender_name,
sender_province_name,
sender_city_name,
sender_district_name,
kanwil_area_pickup,
origin_area,
recipient_province_name,
recipient_city_name,
recipient_district_name,
kanwil_tujuan,
destination_area,
service_type,
durasi_pickup,
order_past_1400,
order_past_1500,
division,
sales_name,
pickup_performance,
pickup_category_everpro,

CASE 
      WHEN sla_pickup_shopee IS NOT NULL AND sla_pickup_blibli_by_input_time IS NULL AND sla_pickup_soscom IS NULL AND sla_pickup_tokped IS NULL THEN sla_pickup_shopee
      WHEN sla_pickup_shopee IS NULL AND sla_pickup_blibli_by_input_time IS NOT NULL AND sla_pickup_soscom IS NULL AND sla_pickup_tokped IS NULL THEN sla_pickup_blibli_by_input_time
      WHEN sla_pickup_shopee IS NULL AND sla_pickup_blibli_by_input_time IS NULL AND sla_pickup_soscom IS NOT NULL AND sla_pickup_tokped IS NULL THEN sla_pickup_soscom
      WHEN sla_pickup_shopee IS NULL AND sla_pickup_blibli_by_input_time IS NULL AND sla_pickup_soscom IS NULL AND sla_pickup_tokped IS NOT NULL THEN sla_pickup_tokped
      END AS pickup_performance_everpro,

sla_pickup_shopee,
sla_pickup_tokped,
sla_pickup_blibli,
sla_pickup_blibli_by_input_time,
sla_pickup_soscom,
pickup_rate,
pickup_status,
fm_performance,

CASE 
      WHEN pickup_failure_reason IN ('Cuaca buruk / bencana alam') THEN 'External' 
      WHEN pickup_failure_reason IN ('Paket akan diproses dengan nomor resi yang baru','Di luar cakupan area cabang, akan dijadwalkan ke cabang lain','Melewati jam operasional cabang','Late Schedule to Branch','Late Schedule to Courier','Kurir tidak available','Late scan pickup') THEN 'IDE'
      WHEN pickup_failure_reason IN ('Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Pengirim akan mengantar paket ke cabang','Pengirim tidak ada di lokasi/toko','Alamat pengirim kurang jelas','Pengirim sedang mempersiapkan paket','Pengirim meminta pergantian jadwal','Paket belum ada/belum selesai dikemas','Pengirim tidak dapat dihubungi','Pengirim merasa tidak menggunakan iDexpress','Pengirim sedang libur','Pengirim sebagai dropshipper dan menunggu supplier','Paket pre order','Paket sedang disiapkan','Pengiriman dibatalkan sebelum di pickup') THEN 'Pengirim'
      WHEN pickup_failure_reason IS NULL THEN 'IDE'
      END AS late_pickup_factor,


CASE
      WHEN pickup_failure_reason IN ('Cuaca buruk / bencana alam') THEN 'Uncontrollable'
      WHEN pickup_failure_reason IN ('Paket akan diproses dengan nomor resi yang baru','Di luar cakupan area cabang, akan dijadwalkan ke cabang lain','Melewati jam operasional cabang','Late Schedule to Branch','Late Schedule to Courier','Kurir tidak available','Late scan pickup') THEN 'Controllable'
      WHEN pickup_failure_reason IN ('Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Pengirim akan mengantar paket ke cabang','Pengirim tidak ada di lokasi/toko','Alamat pengirim kurang jelas','Pengirim sedang mempersiapkan paket','Pengirim meminta pergantian jadwal','Paket belum ada/belum selesai dikemas','Pengirim tidak dapat dihubungi','Pengirim merasa tidak menggunakan iDexpress','Pengirim sedang libur','Pengirim sebagai dropshipper dan menunggu supplier','Paket pre order','Paket sedang disiapkan','Pengiriman dibatalkan sebelum di pickup') THEN 'Uncontrollable'
      WHEN pickup_failure_reason IS NULL THEN 'Controllable'
      END AS late_pickup_category,

recipient_name, --tambah kolom
recipient_cellphone, --tambah kolom
recipient_address, --tambah kolom


FROM (

SELECT *,
      --drop off => IF durasi dari waktu_input dan pickup_time > 2 maka late
      --if pickup time Null dan current date > 2 hari dari input time maka late

  -- SLA Pickup (dari order_order)
CASE
      WHEN service_type = 'Pick up' AND pickup_time IS NOT NULL AND pickup_time <= end_pickup_time THEN 'On Time'
      WHEN service_type = 'Pick up' AND pickup_time IS NULL AND CURRENT_DATE('Asia/Jakarta') > end_pickup_time THEN 'Late'  
      WHEN service_type = 'Pick up' AND pickup_time IS NOT NULL AND pickup_time > end_pickup_time THEN 'Late'  
      WHEN service_type = 'Pick up' AND pickup_time IS NULL AND CURRENT_DATE('Asia/Jakarta') <= end_pickup_time THEN "Not Pick up Yet"
      END AS pickup_performance,

  -------------------pickup category--------------------------
CASE 
      WHEN order_source IN ('Blibli') AND order_past_1400 = 1 THEN "NextDay Pickup"
      WHEN order_source IN ('Blibli') AND order_past_1400 = 0 THEN "SameDay Pickup"
      WHEN order_source IN ('Shopee platform','Tokopedia') THEN NULL
      WHEN order_source NOT IN ('Shopee platform','Blibli','Tokopedia') AND order_past_1500 = 1 THEN "NextDay Pickup"
      WHEN order_source NOT IN ('Shopee platform','Blibli','Tokopedia') AND order_past_1500 = 0 THEN "SameDay Pickup"
      END AS pickup_category_everpro,

  -------------------------------------SLA Pickup ------------------------------
                        
       ----------------- Pickup Shopee --------------------
CASE
      WHEN order_source IN ('Shopee platform') AND DATE(pickup_time) IS NOT NULL AND order_status NOT IN ('Cancel Order') AND DATE(pickup_time) <= DATE_ADD(DATE(start_pickup_time), INTERVAL 1 DAY) THEN "On Time"
      WHEN order_source IN ('Shopee platform') AND pickup_time IS NOT NULL AND order_status NOT IN ('Cancel Order') AND DATE(pickup_time) > DATE_ADD(DATE(start_pickup_time), INTERVAL 1 DAY) THEN "Late"
      WHEN order_source IN ('Shopee platform') AND pickup_time IS NULL AND order_status NOT IN ('Cancel Order') AND CURRENT_DATE('Asia/Jakarta') <= DATE_ADD(DATE(start_pickup_time), INTERVAL 1 DAY) THEN "Not Pick Up Yet"
      WHEN order_source IN ('Shopee platform') AND pickup_time IS NULL AND order_status NOT IN ('Cancel Order') AND CURRENT_DATE('Asia/Jakarta') > DATE_ADD(DATE(start_pickup_time), INTERVAL 1 DAY) THEN "Late"
      END AS sla_pickup_shopee,

      ------------------------Pickup Tokped-------------------------------
CASE
      WHEN order_source IN ('Tokopedia') AND pickup_time IS NOT NULL AND order_status NOT IN ('Cancel Order') AND DATETIME(pickup_time) <= DATETIME(end_pickup_time) THEN "On Time"
      WHEN order_source IN ('Tokopedia') AND pickup_time IS NOT NULL AND order_status NOT IN ('Cancel Order') AND DATETIME(pickup_time) > DATETIME(end_pickup_time) THEN "Late"
      WHEN order_source IN ('Tokopedia') AND pickup_time IS NULL AND order_status NOT IN ('Cancel Order') AND CURRENT_DATE('Asia/Jakarta') <= DATETIME(start_pickup_time) THEN "Not Pick Up Yet"
      WHEN order_source IN ('Tokopedia') AND pickup_time IS NULL AND order_status NOT IN ('Cancel Order') AND CURRENT_DATE('Asia/Jakarta') > DATETIME(start_pickup_time) THEN "Late"
      END AS sla_pickup_tokped,

---------------Pickup Blibli by start_pickup_time-----------------------
              -----nextday pickup------
CASE 
      WHEN order_source IN ('Blibli') AND order_past_1400 = 1 AND pickup_time IS NOT NULL AND DATE(pickup_time) <= DATE_ADD(DATE(start_pickup_time), INTERVAL 1 DAY) THEN "On Time"
      WHEN order_source IN ('Blibli') AND order_past_1400 = 1 AND pickup_time IS NULL AND Order_Status IN ('Cancel Order') THEN "Cancel Order"
      WHEN order_source IN ('Blibli') AND order_past_1400 = 1 AND pickup_time IS NOT NULL AND DATE(pickup_time) > DATE_ADD(DATE(start_pickup_time), INTERVAL 1 DAY) THEN "Late"
      WHEN order_source IN ('Blibli') AND order_past_1400 = 1 AND pickup_time IS NULL AND CURRENT_DATE('Asia/Jakarta') > DATE_ADD(DATE(start_pickup_time), INTERVAL 1 DAY) THEN "Late"
      WHEN order_source IN ('Blibli') AND order_past_1400 = 1 AND pickup_time IS NULL AND CURRENT_DATE('Asia/Jakarta') <= DATE_ADD(DATE(start_pickup_time), INTERVAL 1 DAY) THEN "Not Picked Up"

              -----sameday pickup------
      WHEN order_source IN ('Blibli') AND order_past_1400 = 0 AND pickup_time IS NOT NULL AND DATE(pickup_time) <= DATE(start_pickup_time) THEN "On Time"
      WHEN order_source IN ('Blibli') AND order_past_1400 = 0 AND pickup_time IS NULL AND Order_Status IN ('Cancel Order') THEN "Cancel Order"
      WHEN order_source IN ('Blibli') AND order_past_1400 = 0 AND pickup_time IS NOT NULL AND DATE(pickup_time) > DATE(start_pickup_time) THEN "Late"
      WHEN order_source IN ('Blibli') AND order_past_1400 = 0 AND pickup_time IS NULL AND CURRENT_DATE('Asia/Jakarta') > DATE(start_pickup_time) THEN "Late"
      WHEN order_source IN ('Blibli') AND order_past_1400 = 0 AND pickup_time IS NULL AND CURRENT_DATE('Asia/Jakarta') <= DATE(start_pickup_time) THEN "Not Picked Up"    
      END AS sla_pickup_blibli,

---------------Pickup Blibli by input_time-----------------------
              -----nextday pickup------
CASE 
      WHEN order_source IN ('Blibli') AND order_past_1400 = 1 AND pickup_time IS NOT NULL AND DATE(pickup_time) <= DATE_ADD(DATE(input_time), INTERVAL 1 DAY) THEN "On Time"
      WHEN order_source IN ('Blibli') AND order_past_1400 = 1 AND pickup_time IS NULL AND Order_Status IN ('Cancel Order') THEN "Cancel Order"
      WHEN order_source IN ('Blibli') AND order_past_1400 = 1 AND pickup_time IS NOT NULL AND DATE(pickup_time) > DATE_ADD(DATE(input_time), INTERVAL 1 DAY) THEN "Late"
      WHEN order_source IN ('Blibli') AND order_past_1400 = 1 AND pickup_time IS NULL AND CURRENT_DATE('Asia/Jakarta') > DATE_ADD(DATE(input_time), INTERVAL 1 DAY) THEN "Late"
      WHEN order_source IN ('Blibli') AND order_past_1400 = 1 AND pickup_time IS NULL AND CURRENT_DATE('Asia/Jakarta') <= DATE_ADD(DATE(input_time), INTERVAL 1 DAY) THEN "Not Picked Up"
              
              -----sameday pickup------
      WHEN order_source IN ('Blibli') AND order_past_1400 = 0 AND pickup_time IS NOT NULL AND DATE(pickup_time) <= DATE(input_time) THEN "On Time"
      WHEN order_source IN ('Blibli') AND order_past_1400 = 0 AND pickup_time IS NULL AND Order_Status IN ('Cancel Order') THEN "Cancel Order"
      WHEN order_source IN ('Blibli') AND order_past_1400 = 0 AND pickup_time IS NOT NULL AND DATE(pickup_time) > DATE(input_time) THEN "Late"
      WHEN order_source IN ('Blibli') AND order_past_1400 = 0 AND pickup_time IS NULL AND CURRENT_DATE('Asia/Jakarta') > DATE(input_time) THEN "Late"
      WHEN order_source IN ('Blibli') AND order_past_1400 = 0 AND pickup_time IS NULL AND CURRENT_DATE('Asia/Jakarta') <= DATE(input_time) THEN "Not Picked Up"    
      END AS sla_pickup_blibli_by_input_time,

---------------Pickup Performance (Except Shopee, Blibli, Tokped)-----------------------
              -----nextday pickup------ 
CASE
      WHEN order_source NOT IN ('Shopee platform','Blibli','Tokopedia') AND order_past_1500 = 1 AND pickup_time IS NOT NULL AND DATE(pickup_time) <= DATE_ADD(DATE(input_time), INTERVAL 1 DAY) THEN "On Time"
      WHEN order_source NOT IN ('Shopee platform','Blibli','Tokopedia') AND order_past_1500 = 1 AND pickup_time IS NULL AND order_status IN ('Cancel Order') THEN "Cancel Order"
      WHEN order_source NOT IN ('Shopee platform','Blibli','Tokopedia') AND order_past_1500 = 1 AND pickup_time IS NOT NULL AND DATE(pickup_time) > DATE_ADD(DATE(input_time), INTERVAL 1 DAY) THEN "Late"
      WHEN order_source NOT IN ('Shopee platform','Blibli','Tokopedia') AND order_past_1500 = 1 AND pickup_time IS NULL AND CURRENT_DATE('Asia/Jakarta') > DATE_ADD(DATE(input_time), INTERVAL 1 DAY) THEN "Late"
      WHEN order_source NOT IN ('Shopee platform','Blibli','Tokopedia') AND order_past_1500 = 1 AND pickup_time IS NULL AND CURRENT_DATE('Asia/Jakarta') <= DATE_ADD(DATE(input_time), INTERVAL 1 DAY) THEN "Not Picked Up"
              
              -----sameday pickup------
      WHEN order_source NOT IN ('Shopee platform','Blibli','Tokopedia') AND order_past_1500 = 0 AND pickup_time IS NOT NULL AND DATE(pickup_time) <= DATE(input_time) THEN "On Time"
      WHEN order_source NOT IN ('Shopee platform','Blibli','Tokopedia') AND order_past_1500 = 0 AND pickup_time IS NULL AND order_status IN ('Cancel Order') THEN "Cancel Order"
      WHEN order_source NOT IN ('Shopee platform','Blibli','Tokopedia') AND order_past_1500 = 0 AND pickup_time IS NOT NULL AND DATE(pickup_time) > DATE(input_time) THEN "Late"
      WHEN order_source NOT IN ('Shopee platform','Blibli','Tokopedia') AND order_past_1500 = 0 AND pickup_time IS NULL AND CURRENT_DATE('Asia/Jakarta') > DATE(input_time) THEN "Late"
      WHEN order_source NOT IN ('Shopee platform','Blibli','Tokopedia') AND order_past_1500 = 0 AND pickup_time IS NULL AND CURRENT_DATE('Asia/Jakarta') <= DATE(input_time) THEN "Not Picked Up"
      END AS sla_pickup_soscom,
                          
---------------------------------------------------------------------------------------------------------------------
CASE
      WHEN durasi_pickup < 0 THEN 'Early Pickup'
      WHEN durasi_pickup = 0 THEN 'H+0'
      WHEN durasi_pickup = 1 THEN 'H+1'
      WHEN durasi_pickup = 2 THEN 'H+2' 
      WHEN durasi_pickup > 2 THEN 'H2+'
      END AS pickup_rate,
                    
CASE 
      WHEN order_status = 'Cancel Order' THEN 'Cancel Order'
      WHEN order_status = 'Picked Up' THEN 'Picked Up'
      WHEN Order_Status NOT IN ('Cancel Order','Picked Up' ) THEN 'Not Picked Up'
      END AS pickup_status,
                    
CASE 
      WHEN order_status IN ("Picked Up") THEN "Picked Up"
      ELSE "Failed Pickup" 
      END AS fm_performance,

-- recipient_name,
-- recipient_cellphone,
-- recipient_address,

    FROM kpi_order

)
)),

table_pricing AS (
  SELECT 
    sender_location_id,
    recipient_location_id,
    express_type,
    shipping_client_id,
    discount_rate,
    min_sla,
    max_sla

 FROM `grand-sweep-324604.datawarehouse_idexp.standard_shipping_fee` 
 WHERE DATE(end_expire_time, 'Asia/Jakarta') > CURRENT_DATE('Asia/Jakarta')
      AND deleted = '0'

QUALIFY ROW_NUMBER() OVER (PARTITION BY search_code ORDER BY created_at DESC)=1
),

kpi_waybill AS (

SELECT *,

 CASE 
      WHEN lead_time_deliv < 1 THEN "0 Hari"
      WHEN lead_time_deliv = 1 THEN "1 Hari"
      WHEN lead_time_deliv <= 3 THEN "2-3 Hari"
      WHEN lead_time_deliv <= 5 THEN "4-5 Hari"
      WHEN lead_time_deliv <= 7 THEN "6-7 Hari"
      WHEN lead_time_deliv > 7 THEN "7 Hari+"
      END AS lead_time_category,

CASE
      WHEN lead_time_deliv <= SLA_Delivery THEN 'Hit SLA'
      WHEN lead_time_deliv > SLA_Delivery THEN 'Not Hit'
      WHEN lead_time_deliv IS NULL THEN NULL
      END AS sla_status,


FROM (

    SELECT

ww.waybill_no,
ww.order_no,
ww.ecommerce_order_no,
t0.option_name AS order_source,
ww.parent_shipping_cleint vip_username,
ww.vip_customer_name sub_account,

DATETIME(ww.shipping_time,'Asia/Jakarta') shipping_time,
DATETIME(ww.update_time,'Asia/Jakarta') update_time,
ww.pickup_branch_name,

ww.sender_name,
ww.sender_province_name,
ww.sender_city_name,
ww.sender_district_name,
kw1.kanwil_name AS kanwil_area_pickup,
pu2.pulau AS origin_area,

ww.recipient_province_name,
ww.recipient_city_name,
ww.recipient_district_name,
kw2.kanwil_name as kanwil_area_deliv,
pu5.pulau AS destination_area,

ww.cod_amount, 
rd13.option_name AS waybill_status,
rd12.option_name AS void_status,

CASE 
      WHEN ww.cod_amount > 0 THEN 'COD' 
      WHEN ww.cod_amount = 0 THEN 'Non COD' 
      END AS cod_type,

----------------------------------SLA delivery performance ------------------------------------------------------------
CASE 
      WHEN t0.option_name IN ('Blibli','pt fashion marketplace indonesia','pt fashion eservices indonesia') THEN tp.max_sla
      ELSE SAFE_CAST(t8.sla AS int64) 
      END AS sla_delivery, 
CASE 
      WHEN t0.option_name IN ('Blibli','pt fashion marketplace indonesia','pt fashion eservices indonesia') THEN DATE(DATE_ADD(ww.shipping_time, INTERVAL (tp.max_sla) DAY))
      ELSE DATE_ADD(DATE(ww.shipping_time, 'Asia/Jakarta'), INTERVAL SAFE_CAST(t8.sla AS int64) DAY) 
      END AS sla_est_deadline,

DATETIME(ww.pod_record_time,'Asia/Jakarta') pod_record_time,
ww.pod_branch_name,
DATE_DIFF(DATE(ww.pod_record_time,'Asia/Jakarta'), DATE(ww.shipping_time,'Asia/Jakarta'), DAY) AS lead_time_deliv,

-------------------------SLA ALL------------------------------
CASE 
      WHEN t0.option_name NOT IN ('Blibli','pt fashion marketplace indonesia','pt fashion eservices indonesia') AND SAFE_CAST(t8.sla as int64) >= 999 THEN "No SLA (OoC)"
      WHEN t0.option_name NOT IN ('Blibli','pt fashion marketplace indonesia','pt fashion eservices indonesia') AND SAFE_CAST(t8.sla as int64) IS NULL THEN "Hit SLA"
      WHEN t0.option_name NOT IN ('Blibli','pt fashion marketplace indonesia','pt fashion eservices indonesia') AND DATE(ww.pod_record_time,'Asia/Jakarta') > DATE(DATE_ADD(ww.shipping_time, INTERVAL (SAFE_CAST(t8.sla as int64)) day)) THEN "Not Hit"
      WHEN t0.option_name NOT IN ('Blibli','pt fashion marketplace indonesia','pt fashion eservices indonesia') AND DATE(ww.pod_record_time,'Asia/Jakarta') <= DATE(DATE_ADD(ww.shipping_time, INTERVAL (SAFE_CAST(t8.sla as int64)) day)) THEN 'Hit SLA'
      WHEN t0.option_name NOT IN ('Blibli','pt fashion marketplace indonesia','pt fashion eservices indonesia') AND DATE(ww.pod_record_time, 'Asia/Jakarta') IS NULL AND CURRENT_DATE('Asia/Jakarta') <= DATE(DATE_ADD(ww.shipping_time, INTERVAL (SAFE_CAST(t8.sla as int64)) day)) THEN NULL
      WHEN t0.option_name NOT IN ('Blibli','pt fashion marketplace indonesia','pt fashion eservices indonesia') AND DATE(ww.pod_record_time, 'Asia/Jakarta') IS NULL AND CURRENT_DATE('Asia/Jakarta') > DATE(DATE_ADD(ww.shipping_time, INTERVAL (SAFE_CAST(t8.sla as int64)) day)) THEN "Not Hit"

----------------------------SLA Blibli & Zalora --------------------------------------------   
      WHEN t0.option_name IN ('Blibli','pt fashion marketplace indonesia','pt fashion eservices indonesia') AND tp.max_sla >= 999 THEN "No SLA (OoC)"
      WHEN t0.option_name IN ('Blibli','pt fashion marketplace indonesia','pt fashion eservices indonesia') AND DATE(ww.pod_record_time,'Asia/Jakarta') > DATE(DATE_ADD(ww.shipping_time, INTERVAL (tp.max_sla*1) day)) THEN "Not Hit"
      WHEN t0.option_name IN ('Blibli','pt fashion marketplace indonesia','pt fashion eservices indonesia') AND DATE(ww.pod_record_time,'Asia/Jakarta') <= DATE(DATE_ADD(ww.shipping_time, INTERVAL (tp.max_sla*1) day)) THEN "Hit SLA"
      WHEN t0.option_name IN ('Blibli','pt fashion marketplace indonesia','pt fashion eservices indonesia') AND DATE(ww.pod_record_time, 'Asia/Jakarta') IS NULL AND CURRENT_DATE('Asia/Jakarta') <= DATE(DATE_ADD(ww.shipping_time, INTERVAL (tp.max_sla*1) day)) THEN NULL
      WHEN t0.option_name IN ('Blibli','pt fashion marketplace indonesia','pt fashion eservices indonesia') AND DATE(ww.pod_record_time, 'Asia/Jakarta') IS NULL AND CURRENT_DATE('Asia/Jakarta') > DATE(DATE_ADD(ww.shipping_time, INTERVAL (tp.max_sla*1) day)) THEN "Not Hit"
      END AS sla_performance,

------------------------------RETURN--------------------------------------------------------
   t6.return_type as remark_return,--return reason
   rr.return_branch_name, 
   rr.return_pod_photo_url,
   DATETIME(rr.return_record_time,'Asia/Jakarta') return_record_time,
   pur3.pulau as return_area,  
   rd14.option_name AS return_status,
   rd15.option_name AS return_confirm_status,
   DATETIME(rr.return_confirm_record_time,'Asia/Jakarta') as return_confirm_record_time,
   DATETIME(rr.return_pod_record_time, 'Asia/Jakarta') as return_pod_record_time, 



FROM `grand-sweep-324604.datawarehouse_idexp.waybill_waybill` ww 
LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.waybill_return_bill` rr on rr.waybill_no = ww.waybill_no and rr.deleted = '0'
AND DATE(rr.update_time, 'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -186 DAY))

LEFT JOIN table_pricing tp -- via cte karena perlu remove duplikat, jadi ada pricing yg dobel tapi ambil yg di create terbaru
              ON tp.shipping_client_id = ww.vip_customer_id 
              AND ww.sender_city_id = tp.sender_location_id 
              AND ww.recipient_district_id  = tp.recipient_location_id 
              AND ww.express_type = tp.express_type

LEFT OUTER join `grand-sweep-324604.datawarehouse_idexp.return_type` t6 on rr.return_type_id = t6.id
LEFT OUTER JOIN `datamart_idexp.masterdata_sla_shopee` t8 ON ww.recipient_city_name = t8.destination_city AND ww.sender_city_name = t8.origin_city
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd12 ON rd12.option_value = ww.void_flag AND rd12.type_option = 'voidFlag'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd13 ON rd13.option_value = ww.waybill_status AND rd13.type_option = 'waybillStatus'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd14 ON rd14.option_value = ww.return_flag AND rd14.type_option = 'returnFlag'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` rd15 ON rd15.option_value = rr.return_confirm_status AND rd15.type_option = 'returnConfirmStatus'
LEFT OUTER JOIN `datawarehouse_idexp.system_option` t0 ON t0.option_value = ww.waybill_source AND t0.type_option = 'waybillSource'
LEFT OUTER JOIN `datamart_idexp.sla_internal` s6 ON ww.recipient_city_name = s6.destination_city and ww.sender_city_name = s6.origin_city and ww.recipient_district_name = s6.destination
----mapping division (division_seller will update manually by request)
LEFT OUTER JOIN `grand-sweep-324604.datamart_idexp.masterdata_sales_source` t2 ON t2.source = t0.option_name --waybill_source
LEFT OUTER JOIN `grand-sweep-324604.datamart_idexp.masterdata_sales_seller_vip`t3 ON t3.seller_name = ww.vip_customer_name --vip_name
LEFT OUTER JOIN `datamart_idexp.masterdata_city_mapping_area_island_new` pu2 ON ww.sender_city_name = pu2.city AND ww.sender_province_name = pu2.province --Area_Origin
LEFT OUTER JOIN `datamart_idexp.masterdata_city_mapping_area_island_new` pu5 ON ww.recipient_city_name = pu5.city AND ww.recipient_province_name = pu5.province --Area_Tujuan
LEFT OUTER JOIN `datamart_idexp.masterdata_city_mapping_area_island_new` pur3 ON rr.recipient_city_name = pur3.city and rr.recipient_province_name = pur3.province --Return_area,
LEFT OUTER JOIN `datamart_idexp.mapping_kanwil_area` kw1 ON ww.sender_province_name = kw1.province_name --kanwil origin
LEFT OUTER JOIN `datamart_idexp.mapping_kanwil_area` kw2 ON ww.recipient_province_name = kw2.province_name --kanwil tujuan
LEFT OUTER JOIN `datamart_idexp.mitra_late_reason_delivery` ldr ON ww.waybill_no = ldr.waybill_no

WHERE
DATE(ww.shipping_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -186 DAY))
AND DATE(ww.update_time, 'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -186 DAY))

QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1
)
),

kpi_order_to_waybill AS (

  SELECT

CASE WHEN oo.waybill_no IS NULL THEN ww.waybill_no ELSE oo.waybill_no END AS waybill_no,
CASE WHEN oo.order_no IS NULL THEN ww.order_no ELSE oo.order_no END AS order_no,
CASE WHEN oo.ecommerce_order_no IS NULL THEN ww.ecommerce_order_no ELSE oo.ecommerce_order_no END AS ecommerce_order_no,
CASE WHEN oo.order_source IS NULL THEN ww.order_source ELSE oo.order_source END AS order_source,
CASE WHEN oo.pickup_time IS NULL THEN oo.vip_username ELSE ww.vip_username END AS vip_username,
CASE WHEN oo.pickup_time IS NULL THEN oo.sub_account ELSE ww.sub_account END AS sub_account,
CASE 
  WHEN oo.input_time IS NULL THEN ww.shipping_time ELSE oo.input_time END AS input_time,

oo.start_pickup_time,
oo.end_pickup_time,
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
  WHEN oo.sender_province_name IS NULL THEN ww.sender_province_name ELSE oo.sender_province_name END AS sender_province_name,
CASE 
  WHEN oo.sender_city_name IS NULL THEN ww.sender_city_name ELSE oo.sender_city_name END AS sender_city_name,
CASE 
  WHEN oo.sender_district_name IS NULL THEN ww.sender_district_name ELSE oo.sender_district_name END AS sender_district_name,
CASE 
  WHEN oo.kanwil_area_pickup IS NULL THEN ww.kanwil_area_pickup ELSE oo.kanwil_area_pickup END AS kanwil_area_pickup,
CASE 
  WHEN oo.origin_area IS NULL THEN ww.origin_area ELSE oo.origin_area END AS origin_area,
CASE 
  WHEN oo.recipient_province_name IS NULL THEN ww.recipient_province_name ELSE oo.recipient_province_name END AS recipient_province_name,
CASE 
  WHEN oo.recipient_city_name IS NULL THEN ww.recipient_city_name ELSE oo.recipient_city_name END AS recipient_city_name,
CASE 
  WHEN oo.recipient_district_name IS NULL THEN ww.recipient_district_name ELSE oo.recipient_district_name END AS recipient_district_name,
CASE 
  WHEN oo.kanwil_tujuan IS NULL THEN ww.kanwil_area_deliv ELSE oo.kanwil_tujuan END AS kanwil_area_deliv,
CASE 
  WHEN oo.destination_area IS NULL THEN ww.destination_area ELSE oo.destination_area END AS destination_area,
oo.service_type,
oo.durasi_pickup,
oo.order_past_1400,
oo.order_past_1500,
oo.division,
oo.sales_name,
oo.pickup_performance,
oo.pickup_category_everpro,
pickup_performance_everpro,
oo.sla_pickup_shopee,
oo.sla_pickup_tokped,
oo.sla_pickup_blibli,
oo.sla_pickup_blibli_by_input_time,
oo.sla_pickup_soscom,
oo.pickup_rate,
pickup_status,
oo.fm_performance,
oo.late_pickup_factor,
oo.late_pickup_category,
oo.adjusted_performance_everpro,

ww.cod_amount,
ww.waybill_status,
ww.void_status,
ww.cod_type,
ww.sla_delivery,
ww.sla_est_deadline,
ww.sla_performance,
ww.pod_record_time,
ww.pod_branch_name,
ww.lead_time_deliv,
ww.lead_time_category,
ww.sla_status,

------------------------- Return ----------------------
ww.remark_return,
ww.return_branch_name,
ww.return_pod_photo_url,
ww.return_record_time,
ww.return_area,
ww.return_status,
ww.return_confirm_status,
ww.return_confirm_record_time,
ww.return_pod_record_time,

oo.update_time update_time_oo,
ww.update_time update_time_ww,

oo.recipient_name, --tambah kolom
oo.recipient_cellphone, --tambah kolom
oo.recipient_address, --tambah kolom


  FROM order_to_pickup oo
  FULL JOIN kpi_waybill ww ON oo.waybill_no = ww.waybill_no
),

kpi_pos AS (

      SELECT 
ps.waybill_no,
ps.recording_time_pos,

CASE 
      WHEN ps.problem_reason IN ('Paket dikirim via ekspedisi lain','Pengirim mengantarkan paket ke Drop Point','Pengirim tidak di tempat','Paket sedang disiapkan','Pengirim akan mengantar paket ke cabang','Pengirim tidak ada di lokasi/toko','Alamat pengirim kurang jelas','Pengirim sedang mempersiapkan paket','Pengirim meminta pergantian jadwal','Paket belum ada/belum selesai dikemas','Pengirim tidak dapat dihubungi','Pengirim merasa tidak menggunakan iDexpress','Pengirim sedang libur','Pengirim sebagai dropshipper dan menunggu supplier','Paket pre order') THEN NULL
      WHEN ps.problem_reason IS NULL THEN ldr.late_delivery_reason  
      WHEN ps.problem_reason IS NOT NULL THEN ps.problem_reason
      END AS late_reason,
ps.problem_type,

FROM (
  SELECT
        ps.waybill_no,
        MAX(ps.problem_reason) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) AS problem_reason,
        MAX(DATETIME(ps.operation_time,'Asia/Jakarta')) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) AS recording_time_pos,
        MAX(prt.option_name) OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC) problem_type,

              FROM `datawarehouse_idexp.waybill_problem_piece` ps
              LEFT OUTER JOIN `datawarehouse_idexp.system_option` prt ON ps.problem_type  = prt.option_value AND prt.type_option = 'problemType'
              WHERE DATE(ps.operation_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -186 DAY))

              AND ps.problem_type NOT IN ('02')

              QUALIFY ROW_NUMBER() OVER (PARTITION BY ps.waybill_no ORDER BY ps.operation_time DESC)=1
) ps
LEFT OUTER JOIN `datamart_idexp.mitra_late_reason_delivery` ldr ON ps.waybill_no = ldr.waybill_no
),

kpi_pos_and_category AS (

      SELECT *,

-----Problem delivery category lengkap
CASE
      WHEN late_reason LIKE '%bea cukai%' OR late_reason LIKE '%Rejected by customs%' THEN 'Bea Cukai (Red Line)' --bea cukai
      WHEN late_reason IN ('Kemasan paket rusak','Paket rusak/pecah', 'Kerusakan pada resi / informasi resi tidak jelas','Damaged parcels','Information on AWB is unclear/damage','Packaging is damage','Kemasan paket tidak sesuai prosedur','Pengemasan paket dengan kemasan rusak') THEN 'Damaged' --damaged
      WHEN late_reason IN ('Paket hilang atau tidak ditemukan', 'Parcels is lost or cannot be found','Package is lost','Paket Rusak') THEN 'Lost' --lost
      WHEN late_reason IN ('Paket crosslabel') THEN 'CrisCross' --criscross
      WHEN late_reason IN ('Paket salah dalam proses penyortiran','Data alamat tidak sesuai dengan kode sortir','Paket akan dikembalikan ke cabang asal','Paket salah dikirimkan/salah penyortiran') THEN 'Missroute'  --missroute
      WHEN late_reason IN ('Di luar cakupan area cabang, akan dijadwalkan ke cabang lain') THEN 'Uncoverage' --uncover 
      WHEN late_reason IN ('Pelanggan ingin dikirim ke alamat berbeda','Alamat pelanggan salah/sudah pindah alamat') THEN 'Bad address' --bad address 
      WHEN late_reason IN ('Terdapat barang berbahaya (Dangerous Goods)','Berat paket tidak sesuai','Penerima ingin membuka paket sebelum membayar') THEN 'Delivery attempt problem' --Delivery attempt problem
      WHEN late_reason IN ('Cuaca buruk / bencana alam') THEN 'Force Majeur' --Force Majeur
      WHEN late_reason IN ('Nomor telepon yang tertera tidak dapat dihubungi','Nomor telepon yang tertera tidak dapat dihubungi atau alamat tidak jelas') THEN 'Recipient cannot be contacted' --Recipient cannot be contacted
      WHEN late_reason IN ('Pelanggan libur akhir pekan/libur panjang','Reschedule pengiriman dengan penerima','Toko atau kantor sudah tutup','Pelanggan tidak di lokasi','Sudah Reschedule dengan Customer','Pelanggan tidak di tempat','Kantor atau toko tutup','Telepon bermasalah, tidak dapat dihubungi','Penjadwalan Ulang') THEN 'Recipient not at home' --Recipient not at home
      WHEN late_reason IN ('Penerima menolak menerima paket') THEN 'Rejected by recipient' --Rejected by recipient
      WHEN late_reason IN ('Pelanggan berinisiatif mengambil paket di cabang','Penerima mengambil sendiri paket di DP') THEN 'Selfpickup by recipient' --Selfpickup by recipient
      WHEN late_reason IN ('Pengiriman dibatalkan','Paket akan diproses dengan nomor resi yang baru','Pelanggan membatalkan pengiriman','Pengiriman dibatalkan sebelum di pickup') THEN 'Shipment cancellation' --Shipment cancellation
      WHEN late_reason IN ('Penundaan jadwal armada pengiriman','Paket makanan, disimpan hingga waktu pengiriman yang tepat','Food parcels, kept until proper delivery time','Melewati jam operasional cabang') THEN 'Shipment on hold' --Shipment on hold
      WHEN late_reason IN ('Paket hilang ditemukan','Pelanggan menunggu paket yang lain untuk dikirim','Indikasi kecurangan pengiriman') THEN 'Other' --other 
      END AS problem_delivery_category_2,

-------problem delivery category based on oms 
CASE 
      WHEN problem_type NOT IN ('Percobaan pengambilan terkendala') THEN late_reason
      END AS problem_delivery_category_oms_2,

CASE
      WHEN late_reason IN ('Paket akan diproses dengan nomor resi yang baru','Paket salah dalam proses penyortiran','Paket rusak/pecah','Paket hilang atau tidak ditemukan','Data alamat tidak sesuai dengan kode sortir','Paket hilang ditemukan','Pengemasan paket dengan kemasan rusak','Paket crosslabel','Melewati jam operasional cabang','Kerusakan pada resi / informasi resi tidak jelas','Kemasan paket rusak','Paket akan dikembalikan ke cabang asal','Kerusakan pada label pengiriman','Paket salah sortir/ salah rute','Di luar cakupan area cabang, akan dijadwalkan ke cabang lain','Paket yang diterima dalam keadaan rusak','Late Arrival','Kurir tidak available','Late scan POD') THEN 'IDE'
      WHEN late_reason IN ('Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Toko atau kantor sudah tutup','Pelanggan tidak di lokasi','Reschedule pengiriman dengan penerima','Pelanggan membatalkan pengiriman','Pelanggan ingin dikirim ke alamat berbeda','Nomor telepon yang tertera tidak dapat dihubungi atau alamat tidak jelas','Penerima menolak menerima paket','Alamat pelanggan salah/sudah pindah alamat','Penerima ingin membuka paket sebelum membayar','Pelanggan libur akhir pekan/libur panjang','Pelanggan menunggu paket yang lain untuk dikirim','Pelanggan berinisiatif mengambil paket di cabang','Kemasan paket tidak sesuai prosedur','Pengirim membatalkan pengiriman','Penerima tidak di tempat','Penerima menjadwalkan ulang waktu pengiriman','Penerima pindah alamat','Penerima ingin mengambil paket di cabang','Alamat tidak lengkap','Penerima tidak dikenal','Nomor telepon tidak dapat dihubungi') THEN 'Penerima'
      WHEN late_reason IN ('Paket ditolak oleh bea cukai (red line)','Terdapat barang berbahaya (Dangerous Goods)','Cuaca buruk / bencana alam','Penundaan jadwal armada pengiriman','Indikasi kecurangan pengiriman','Berat paket tidak sesuai','Paket makanan, disimpan hingga waktu pengiriman yang tepat','Food parcels, kept until proper delivery time','Pengirim tidak dapat dihubungi','bencana alam','Cuaca buruk / Hujan') THEN 'External'
      WHEN late_reason IS NULL THEN 'IDE'
      END AS late_delivery_factor,

CASE
      WHEN late_reason IN ('Paket akan diproses dengan nomor resi yang baru','Paket salah dalam proses penyortiran','Paket rusak/pecah','Paket hilang atau tidak ditemukan','Data alamat tidak sesuai dengan kode sortir','Paket hilang ditemukan','Pengemasan paket dengan kemasan rusak','Paket crosslabel','Melewati jam operasional cabang','Kerusakan pada resi / informasi resi tidak jelas','Kemasan paket rusak','Paket akan dikembalikan ke cabang asal','Kerusakan pada label pengiriman','Paket salah sortir/ salah rute','Di luar cakupan area cabang, akan dijadwalkan ke cabang lain','Paket yang diterima dalam keadaan rusak','Late Arrival','Late scan POD') THEN 'Controllable' --Controllable
      WHEN late_reason IN ('Pengiriman akan menggunakan ekspedisi lain','Pengiriman dibatalkan','Toko atau kantor sudah tutup','Pelanggan tidak di lokasi','Reschedule pengiriman dengan penerima','Pelanggan membatalkan pengiriman','Pelanggan ingin dikirim ke alamat berbeda','Nomor telepon yang tertera tidak dapat dihubungi atau alamat tidak jelas','Penerima menolak menerima paket','Alamat pelanggan salah/sudah pindah alamat','Penerima ingin membuka paket sebelum membayar','Pelanggan libur akhir pekan/libur panjang','Pelanggan menunggu paket yang lain untuk dikirim','Pelanggan berinisiatif mengambil paket di cabang','Kemasan paket tidak sesuai prosedur','Pengirim membatalkan pengiriman','Penerima tidak di tempat','Penerima menjadwalkan ulang waktu pengiriman','Penerima pindah alamat','Penerima ingin mengambil paket di cabang','Alamat tidak lengkap','Penerima tidak dikenal','Nomor telepon tidak dapat dihubungi') THEN 'Uncontrollable' --Uncontrollable
      WHEN late_reason IN ('Paket ditolak oleh bea cukai (red line)','Terdapat barang berbahaya (Dangerous Goods)','Cuaca buruk / bencana alam','Penundaan jadwal armada pengiriman','Indikasi kecurangan pengiriman','Berat paket tidak sesuai','Paket makanan, disimpan hingga waktu pengiriman yang tepat','Food parcels, kept until proper delivery time','Pengirim tidak dapat dihubungi','bencana alam','Cuaca buruk / Hujan') THEN 'Uncontrollable'
      WHEN late_reason IS NULL THEN 'Controllable' --Controllable
      END AS late_delivery_category,


      FROM kpi_pos
),

count_pos as(
  SELECT
        sc.waybill_no,
        COUNT(sc.record_time) OVER (PARTITION BY sc.waybill_no) AS count_pos,

              FROM `datawarehouse_idexp.waybill_waybill_line` sc
              LEFT join `grand-sweep-324604.datawarehouse_idexp.res_problem_package` t4 on sc.problem_code = t4.code and t4.deleted = '0'
              LEFT OUTER JOIN `datawarehouse_idexp.system_option` t1 ON t1.option_value = sc.problem_type AND t1.type_option = 'problemType'
              WHERE DATE(sc.record_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -186 DAY))

              AND sc.operation_type IN ('18')

              QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.waybill_no)=1
        ),

all_order_waybill_pos AS (

      SELECT

a.waybill_no,
a.order_no,
a.ecommerce_order_no,
a.order_source,
a.vip_username,
a.sub_account,
a.input_time,
a.start_pickup_time,
a.end_pickup_time,
a.pickup_failure_reason,
a.pickup_failure_time,
a.pickup_time,
a.scheduling_or_pickup_branch,
a.order_status,
a.sender_name,
a.sender_province_name,
a.sender_city_name,
a.sender_district_name,
a.kanwil_area_pickup,
a.origin_area,
a.recipient_province_name,
a.recipient_city_name,
a.recipient_district_name,
a.kanwil_area_deliv,
a.destination_area,
a.service_type,
a.durasi_pickup,
a.order_past_1400,
a.order_past_1500,
a.division,
a.sales_name,
a.pickup_performance,
a.pickup_category_everpro,
a.pickup_performance_everpro,
a.sla_pickup_shopee,
a.sla_pickup_tokped,
a.sla_pickup_blibli,
a.sla_pickup_blibli_by_input_time,
a.sla_pickup_soscom,
a.sla_performance,
a.pickup_rate,
a.pickup_status,
a.fm_performance,
a.late_pickup_factor,
a.late_pickup_category,
a.adjusted_performance_everpro,
a.cod_amount,
a.waybill_status,
a.void_status,
a.cod_type,
a.update_time_oo,
a.update_time_ww,
a.sla_delivery,
a.sla_est_deadline,
a.pod_record_time,
a.pod_branch_name,
a.lead_time_deliv,
a.lead_time_category,
a.sla_status,
a.remark_return,
a.return_branch_name,
a.return_pod_photo_url,
a.return_record_time,
a.return_area,
a.return_status,
a.return_confirm_status,
a.return_confirm_record_time,
a.return_pod_record_time,
b.recording_time_pos,
b.late_reason,
b.problem_type,

CASE 
     WHEN b.late_reason IN ('Kemasan paket rusak','Paket rusak/pecah', 'Kerusakan pada resi / informasi resi tidak jelas','Damaged parcels','Information on AWB is unclear/damage','Packaging is damage') THEN 'Damaged'
     WHEN b.late_reason IN ('Paket hilang atau tidak ditemukan', 'Parcels is lost or cannot be found','Package is lost') THEN 'Lost'
     WHEN b.late_reason IN ('Paket crosslabel') THEN 'CrisCross'
     WHEN b.late_reason IN ('Paket salah dalam proses penyortiran','Data alamat tidak sesuai dengan kode sortir') THEN 'Missroute'
     WHEN a.recipient_province_name IN ("PAPUA BARAT","MALUKU UTARA","PAPUA","MALUKU","NTB","NTT") AND a.cod_amount > 0 THEN 'Uncoverage'
     END AS problem_delivery_category,

b.problem_delivery_category_2,
b.problem_delivery_category_oms_2,
b.late_delivery_factor,
b.late_delivery_category,
c.count_pos,
CASE 
      WHEN a.order_status = 'Cancel Order' THEN 'Cancel Order'
      WHEN a.void_status = 'Void' THEN 'Cancel Order'
      WHEN a.waybill_status IS NULL THEN 'Not Picked Up'
      WHEN a.waybill_status IN ('Signed') OR a.pod_record_time IS NOT NULL THEN 'Delivered'
      WHEN a.waybill_status IN ('Return Received') OR a.return_pod_record_time IS NOT NULL THEN 'Returned'
      WHEN a.waybill_status LIKE '%bea cukai%' OR b.late_reason LIKE '%Rejected by customs%' THEN 'Undelivered'
      WHEN b.late_reason IN ('Kemasan paket rusak','Paket rusak/pecah', 'Kerusakan pada resi / informasi resi tidak jelas','Damaged parcels','Information on AWB is unclear/damage','Packaging is damage') THEN 'Undelivered'
      WHEN b.late_reason IN ('Paket hilang atau tidak ditemukan', 'Parcels is lost or cannot be found','Package is lost') THEN 'Undelivered'
      WHEN a.waybill_status <> 'Return Received' AND ( a.waybill_no IS NOT NULL AND a.return_confirm_record_time is not null AND a.return_pod_record_time IS NULL AND a.pod_record_time IS NULL) THEN 'Return Process'
      ELSE 'Delivery Process' 
      END AS last_status,

CASE  
      WHEN a.order_status = 'Cancel Order' THEN 'Cancel Order'
      WHEN a.void_status = 'Void' THEN 'Cancel Order'
      WHEN a.waybill_status IS NULL THEN 'Not Picked Up'
      WHEN a.waybill_status IN ('Signed') OR a.pod_record_time IS NOT NULL THEN 'Delivered'
      WHEN a.waybill_status IN ('Return Received') OR a.return_pod_record_time IS NOT NULL THEN 'Returned'
      WHEN a.waybill_status <> 'Return Received' AND ( a.waybill_no IS NOT NULL AND a.return_confirm_record_time is not null AND a.return_pod_record_time IS NULL AND a.pod_record_time IS NULL) THEN 'Return Process'
      WHEN a.return_confirm_record_time IS NOT NULL and a.return_pod_record_time is null THEN 'Return Process' --temporary
      WHEN b.late_reason LIKE '%bea cukai%' OR b.late_reason LIKE '%Rejected by customs%' THEN 'Paket ditolak bea cukai (red line)'
      WHEN b.late_reason IN ('Kemasan paket rusak','Paket rusak/pecah', 'Kerusakan pada resi / informasi resi tidak jelas','Damaged parcels','Information on AWB is unclear/damage','Packaging is damage') THEN 'Damaged'
      WHEN b.late_reason IN ('Paket hilang atau tidak ditemukan', 'Parcels is lost or cannot be found','Package is lost') then 'Lost' 
      WHEN b.late_reason IN ('Paket crosslabel') THEN 'CrisCross'
      WHEN b.late_reason IN ('Paket salah dalam proses penyortiran','Data alamat tidak sesuai dengan kode sortir') THEN 'Missroute'
      ELSE 'Over SLA' 
      END AS final_status,

a.recipient_name, --tambah kolom
a.recipient_cellphone, --tambah kolom
a.recipient_address, --tambah kolom



      FROM kpi_order_to_waybill a
      LEFT OUTER JOIN kpi_pos_and_category b ON a.waybill_no = b.waybill_no
      LEFT OUTER JOIN count_pos c ON a.waybill_no = c.waybill_no
      
      WHERE a.order_source = 'VIP Customer Portal'
AND a.vip_username = 'universitasterbuka01p'
)

SELECT * FROM (

  SELECT *

  FROM all_order_waybill_pos
)
