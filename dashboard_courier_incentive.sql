-----------------Update dashboard_courier_incentive-------------------
WITH root_data_pickup AS (
SELECT 
ww.waybill_no,
sr.option_name AS waybill_source,
et.option_name AS express_type,
CASE 
    WHEN ww.waybill_no IS NOT NULL THEN 1 END AS waybill_alias,
CASE 
    WHEN ww.pickup_courier_id = 0 THEN oo.pickup_courier_id
    ELSE ww.pickup_courier_id
    END AS pickup_courier_id,
CASE 
    WHEN ww.pickup_courier_name = '' THEN oo.pickup_courier_name
    ELSE ww.pickup_courier_name
    END AS pickup_courier_name,
ru.personnel_no,
CASE 
    WHEN (ru.enabled = '1' OR ru1.enabled = '1') AND (ru.deleted = '0' OR ru1.deleted = '0') THEN "active"
    ELSE "inactive"
    END AS courier_status,
FORMAT_DATE("%b %Y", DATE(ww.shipping_time,'Asia/Jakarta')) AS month_pickup,
DATETIME(ww.shipping_time,'Asia/Jakarta') pickup_date,
CASE 
    WHEN ww.waybill_no IS NOT NULL THEN 1 ELSE 0 END AS pickup,
CASE 
    WHEN ww.shipping_time IS NOT NULL THEN "Pickup" END AS task_type,
rb.branch_no,
ww.pickup_branch_name,
ww.sender_district_name,
ww.sender_city_name,
ww.sender_province_name,
ra.province_name AS province_name,
ww.void_flag,
ru.enabled,
bl.option_name branch_level,
FROM `datawarehouse_idexp.waybill_waybill` ww
LEFT OUTER join `datawarehouse_idexp.order_order` oo on ww.waybill_no = oo.waybill_no
AND DATE(oo.input_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -250 DAY))
LEFT OUTER JOIN `datawarehouse_idexp.res_user` ru ON ru.id = oo.pickup_courier_id AND ru.deleted = '0'
LEFT OUTER JOIN `datawarehouse_idexp.res_user` ru1 ON ru1.id = ww.pickup_courier_id AND ru1.deleted = '0'
LEFT OUTER JOIN `datawarehouse_idexp.res_branch` rb ON ww.pickup_branch_id = rb.id
LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.system_option` bl on rb.branch_level  = bl.option_value and bl.type_option = 'branchLevel'
LEFT OUTER JOIN `datawarehouse_idexp.res_area` ra ON ra.city_id = rb.city_id
LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.system_option` sr on ww.waybill_source  = sr.option_value and sr.type_option = 'waybillSource'
LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.system_option` et on ww.express_type  = et.option_value and et.type_option = 'expressType'
                        --------------------------------------------------------------------------------------------------
WHERE
    DATE(ww.shipping_time,'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -76 DAY))
    AND ww.void_flag = '0'
    AND ww.deleted = '0'
    
QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1
),
pickup_courier AS (
  SELECT 
  waybill_no,
  waybill_alias,
  waybill_source,
  express_type,
  pickup_courier_id courier_id,
  personnel_no,
  pickup_courier_name courier_name,
  courier_status,
  pickup_date tgl_tugas,
  branch_no,
  pickup_branch_name branch_name,
  sender_district_name district_name,
  sender_city_name city_name,
  pu.province_name,
CASE 
    WHEN branch_level IN ('TH','PDB') AND kw1.kanwil_name = 'SUMATERA BAGIAN UTARA' THEN 'Sumbagut'
    WHEN branch_level IN ('TH','PDB') AND kw1.kanwil_name = 'SUMATERA BAGIAN SELATAN' THEN 'Sumbagsel'
    WHEN branch_level IN ('TH','PDB') AND pu.province_name = 'DKI JAKARTA' THEN 'Jakarta'
    WHEN branch_level IN ('TH','PDB') AND pu.province_name = 'BANTEN' THEN 'Tangerang'
    WHEN branch_level IN ('TH','PDB') AND kw1.kanwil_name = 'JAWA BARAT' THEN 'Jabar'
    WHEN branch_level IN ('TH','PDB') AND kw1.kanwil_name = 'JAWA TENGAH' THEN 'Jawa Tengah'
    WHEN branch_level IN ('TH','PDB') AND kw1.kanwil_name = 'JAWA TIMUR' THEN 'Jawa Timur'
    WHEN branch_level IN ('TH','PDB') AND kw1.kanwil_name = 'KALIMANTAN' THEN 'Kalimantan'
    WHEN branch_level IN ('TH','PDB') AND kw1.kanwil_name = 'SULAWESI' THEN 'Sulawesi'
    WHEN branch_level IN ('TH','PDB') AND kw1.kanwil_name = '3PL' THEN '3PL'
    WHEN branch_level IN ('MH') THEN 'MH'
    WHEN branch_level IN ('Outlet') THEN 'Outlet'
    WHEN branch_level IN ('Aggregator') THEN 'Aggregator'
    WHEN branch_level IN ('HQ') THEN 'HQ'
    WHEN branch_level IN ('Agent') THEN 'Agent'
    END AS kanwil_name_pickup,
   enabled,
   branch_level,
   task_type,
   pickup,
CASE 
    WHEN waybill_no IS NOT NULL THEN 0 END AS pod,
    
FROM root_data_pickup pu
LEFT JOIN `datamart_idexp.mapping_kanwil_area` kw1 ON pu.province_name = kw1.province_name 
),
courier_incentive_pickup AS (
SELECT
pc.waybill_no,
pc.waybill_alias,
pc.waybill_source,
pc.express_type,
pc.courier_id,
ru.personnel_no,
pc.courier_name,
CASE 
    WHEN ru.enabled = '1' THEN 'active'
    WHEN ru.enabled = '0' THEN 'inactive'
    END AS courier_status,
pc.tgl_tugas,
pc.branch_no,
pc.branch_name,
pc.district_name,
pc.city_name,
pc.province_name,
pc.kanwil_name_pickup AS kanwil_name,
CONCAT(pc.branch_no,' ','-',' ',pc.kanwil_name_pickup,' ',pc.branch_name) AS work_location,
                        
ru.enabled,
pc.branch_level,
pc.task_type,
                        
SUM(pc.pickup) AS pickup,
SUM(pc.pod) AS pod,
FROM pickup_courier pc
LEFT OUTER JOIN `datawarehouse_idexp.res_user` ru ON pc.courier_id = ru.id AND ru.deleted = '0'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
),
root_data_pod AS (
    SELECT 
ww.waybill_no,
sr.option_name AS waybill_source,
et.option_name AS express_type,
CASE 
    WHEN ww.waybill_no IS NOT NULL THEN 1 END AS waybill_alias,
ww.pod_courier_id,
ru.personnel_no,
ww.pod_courier_name,
CASE 
    WHEN ru.enabled = '1' AND ru.deleted = '0' THEN "active"
    ELSE "inactive"
    END AS courier_status,
FORMAT_DATE("%b %Y", DATE(ww.pod_record_time,'Asia/Jakarta')) AS month_pod,
DATETIME(ww.pod_record_time,'Asia/Jakarta') pod_date,
rb.branch_no,
ww.pod_branch_name,
ww.recipient_district_name,
ww.recipient_city_name,
ww.recipient_province_name,
ra.province_name,
ww.void_flag,
ru.enabled,
bl.option_name branch_level,
CASE 
    WHEN ww.pod_record_time IS NOT NULL THEN "POD" END AS task_type,
FROM `datawarehouse_idexp.dm_waybill_waybill` ww
LEFT OUTER JOIN `datawarehouse_idexp.res_user` ru ON ru.id = ww.pod_courier_id AND ru.deleted = '0'
LEFT OUTER JOIN `datawarehouse_idexp.res_branch` rb ON ww.pod_branch_id = rb.id
LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.system_option` bl on rb.branch_level  = bl.option_value and bl.type_option = 'branchLevel'
LEFT OUTER JOIN `datawarehouse_idexp.res_area` ra ON ra.city_id = rb.city_id
LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.system_option` sr on ww.waybill_source  = sr.option_value and sr.type_option = 'waybillSource'
LEFT OUTER JOIN `grand-sweep-324604.datawarehouse_idexp.system_option` et on ww.express_type  = et.option_value and et.type_option = 'expressType'
                    -----------------------------------------------------------------------------------------------------------------------------------------
WHERE 
      DATE(ww.update_time, 'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -76 DAY))
      AND DATE(ww.pod_record_time, 'Asia/Jakarta') >= DATE(DATE_ADD(CURRENT_DATE(), INTERVAL -76 DAY))
      AND ww.deleted = '0'
QUALIFY ROW_NUMBER() OVER (PARTITION BY ww.waybill_no ORDER BY ww.update_time DESC)=1
),
pod_courier AS (
  SELECT 
waybill_no,
waybill_alias,
waybill_source,
express_type,
pod_courier_id courier_id,
pod_courier_name courier_name,
personnel_no,
courier_status,
pod_date tgl_tugas,
branch_no,
pod_branch_name branch_name,
recipient_district_name district_name,
recipient_city_name city_name,
rp.province_name,
CASE 
     WHEN branch_level IN ('TH','PDB') AND kw1.kanwil_name = 'SUMATERA BAGIAN UTARA' THEN 'Sumbagut'
     WHEN branch_level IN ('TH','PDB') AND kw1.kanwil_name = 'SUMATERA BAGIAN SELATAN' THEN 'Sumbagsel'
     WHEN branch_level IN ('TH','PDB') AND rp.province_name = 'DKI JAKARTA' THEN 'Jakarta'
     WHEN branch_level IN ('TH','PDB') AND rp.province_name = 'BANTEN' THEN 'Tangerang'
     WHEN branch_level IN ('TH','PDB') AND kw1.kanwil_name = 'JAWA BARAT' THEN 'Jabar'
     WHEN branch_level IN ('TH','PDB') AND kw1.kanwil_name = 'JAWA TENGAH' THEN 'Jawa Tengah'
     WHEN branch_level IN ('TH','PDB') AND kw1.kanwil_name = 'JAWA TIMUR' THEN 'Jawa Timur'
     WHEN branch_level IN ('TH','PDB') AND kw1.kanwil_name = 'KALIMANTAN' THEN 'Kalimantan'
     WHEN branch_level IN ('TH','PDB') AND kw1.kanwil_name = 'SULAWESI' THEN 'Sulawesi'
     WHEN branch_level IN ('TH','PDB') AND kw1.kanwil_name = '3PL' THEN '3PL'
     WHEN branch_level IN ('MH') THEN 'MH'
     WHEN branch_level IN ('Outlet') THEN 'Outlet'
     WHEN branch_level IN ('Aggregator') THEN 'Aggregator'
     WHEN branch_level IN ('HQ') THEN 'HQ'
     WHEN branch_level IN ('Agent') THEN 'Agent'
     END AS kanwil_name_pod,
enabled,
branch_level,
CASE 
    WHEN waybill_alias IS NOT NULL THEN 0 END AS pickup,
CASE 
    WHEN pod_date IS NOT NULL THEN 1 ELSE 0 END AS pod,
task_type,
FROM root_data_pod rp
LEFT JOIN `datamart_idexp.mapping_kanwil_area` kw1 ON rp.province_name = kw1.province_name 
),
courier_incentive_pod AS (
  SELECT
pc.waybill_no,
pc.waybill_alias,
pc.waybill_source,
pc.express_type,
pc.courier_id,
ru.personnel_no,
pc.courier_name,
CASE
    WHEN ru.enabled = '1' THEN 'active'
    WHEN ru.enabled = '0' THEN 'inactive'
    END AS courier_status,
pc.tgl_tugas,
pc.branch_no,
pc.branch_name,
pc.district_name,
pc.city_name,
pc.province_name,
pc.kanwil_name_pod AS kanwil_name,
CONCAT(pc.branch_no,' ','-',' ',pc.kanwil_name_pod,' ',pc.branch_name) AS work_location,
ru.enabled,
pc.branch_level,
pc.task_type,
                    
SUM(pc.pickup) AS pickup,
SUM(pc.pod) AS pod,
FROM pod_courier pc
LEFT OUTER JOIN `datawarehouse_idexp.res_user` ru ON pc.courier_id = ru.id AND ru.deleted = '0'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
)
SELECT * FROM courier_incentive_pickup UNION ALL
SELECT * FROM courier_incentive_pod
