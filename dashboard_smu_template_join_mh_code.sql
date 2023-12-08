------- dummy masih pakai range 01-31 Jul 2023 -----------------
-- smu_template_2_join_mh_code --for table bm (table1 dashboard)

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


  FROM `dev_idexp.smu_template_2` g
),

join_mh_code AS (

  SELECT
  mh_code,
  mh_name,

  FROM `datamart_idexp.a_mh_facility_name_rnd`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY mh_code)=1
)

SELECT a.*,

CASE 
      WHEN a.origin IN ('CGK') THEN "MH JAKARTA"
      ELSE b.mh_name END AS mh_sending_join,

  CASE 
      WHEN a.destination_by_issued IN ('CGK') THEN "MH JAKARTA"
      ELSE c.mh_name END AS mh_arrival_join,

FROM gsheet a
LEFT OUTER JOIN join_mh_code b ON a.origin = b.mh_code
LEFT OUTER JOIN join_mh_code c ON a.destination_by_issued = c.mh_code
