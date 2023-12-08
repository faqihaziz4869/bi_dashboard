------- dummy masih pakai range 01-31 Jul 2023 -----------------
--buat table yang ada detail awb-nya--

WITH gsheet AS (
  SELECT
    CAST(report_date AS DATE FORMAT 'MM/DD/YYYY') report_date,
    -- EXTRACT(DATE(report_date) FROM report_date),
    origin,
    destination_by_bagging,
    destination_by_issued,
    no_of_packages,
    smu_weight,
    bm_code,
    vm_code,
    area_pulau,
    smu_booking_code,
    total_smu_fee,
    flight_no,
    airlanes,
    vendor,
    CONCAT(bm_code,' ','-',' ',vm_code) AS bm_vm_gsheet,
  FROM `dev_idexp.smu_template_2`
),

query AS (
  SELECT
    bag_no,
    vehicle_tag_no,
    bm_vm_concat,
    waybill_no,
    shipping_time,
    express_type,
    record_time,
    standard_shipping_fee,
    handling_fee,
    other_fee,
    insurance_amount,
    total_shipping_fee,
    system_sf,
    system_weight,
    sender_province_name origin_province,
    sender_city_name origin_city,
    sender_district_name origin_district,
    pickup_branch_name,
    recipient_province_name destination_province,
    recipient_city_name destination_city,
    recipient_district_name destination_district,
    pod_branch_name,
    return_flag,
    return_pod_branch_name,
    pod_or_return_pod_branch,

  FROM `dev_idexp.temporary_table_dummy_smu`
  -- GROUP BY 1,2,3
)


SELECT
  *
FROM gsheet g 
  LEFT JOIN query q ON q.bm_vm_concat = g.bm_vm_gsheet
