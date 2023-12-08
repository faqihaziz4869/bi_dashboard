------- dummy masih pakai range 01-31 Jul 2023 -----------------
--table bm tanpa join mh code--

WITH gsheet AS (
  SELECT
    -- CAST(report_date AS DATE FORMAT 'MM/DD/YYYY') report_date,
    report_date,
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
    mh_sending_join mh_sending,
    mh_arrival_join mh_arrival,
    
  -- FROM `dev_idexp.smu_template_2`
  FROM `dev_idexp.smu_template_2_join_mh_code`
),

query AS (
  SELECT
    bag_no,
    vehicle_tag_no,
    bm_vm_concat,
    -- record_time,
    SUM(standard_shipping_fee) AS standard_shipping_fee ,
    SUM(handling_fee) AS handling_fee,
    SUM(other_fee) AS other_fee,
    SUM(insurance_amount) AS insurance_amount,
    SUM(total_shipping_fee) AS total_shipping_fee,
    SUM(system_sf) AS system_sf,
    SUM(system_weight) AS system_weight
  FROM `dev_idexp.temporary_table_dummy_smu`
  GROUP BY 1,2,3
)


SELECT
  *
FROM gsheet g 
  LEFT JOIN query q ON q.bm_vm_concat = g.bm_vm_gsheet
