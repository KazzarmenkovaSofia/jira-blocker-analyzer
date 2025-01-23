# Create table all_blocker_info_issue_rk
  
drop table if exists all_blocker_info_issue_rk;
create table all_blocker_info_issue_rk as

SELECT *
FROM all_blocker_info, prod_v_emart.jira_issue_cur
Where all_blocker_info.issue_key=prod_v_emart.jira_issue_cur.issue_full_nm ;

# Unloading all jira tasks, statuses and blockers
  
drop table if exists all_blocker_info_final;
create table all_blocker_info_final as

SELECT issue_key
, prod_v_emart.jira_issue_chng.status_name
, valid_from_dttm , valid_to_dttm
, blocker_category
, flag_set_time 
, CASE
WHEN flag_removed_time::timestamp(0) NOT BETWEEN valid_from_dttm::timestamp(0) AND valid_to_dttm::timestamp(0)
THEN valid_to_dttm::timestamp(0)
ELSE flag_removed_time::timestamp(0)
END AS flag_removed_t
, CASE
WHEN flag_removed_time::timestamp(0) BETWEEN valid_from_dttm::timestamp(0) AND valid_to_dttm::timestamp(0)
THEN flag_removed_time::timestamp(0)-flag_set_time::timestamp(0)
ELSE valid_to_dttm::timestamp(0)-flag_set_time::timestamp(0)
END AS blc_time
FROM all_blocker_info_issue_rk
JOIN prod_v_emart.jira_issue_chng on all_blocker_info_issue_rk.issue_rk=prod_v_emart.jira_issue_chng.issue_rk
WHERE flag_set_time::timestamp(0) BETWEEN valid_from_dttm::timestamp(0) AND valid_to_dttm::timestamp(0)
ORDER BY issue_key, valid_from_dttm;

# Counting block days

SELECT status_name 
,CASE 
WHEN blocker_category IS NULL THEN 'Пустой блокер'
ELSE blocker_category
END AS blocker
,ROUND(CAST(DATE_PART('day',SUM(blc_time))+DATE_PART('hour',SUM(blc_time))/24+DATE_PART('minute',SUM(blc_time))/60/24 AS numeric), 2) as block_days
FROM all_blocker_info_final
GROUP BY status_name , blocker_category
ORDER BY status_name
