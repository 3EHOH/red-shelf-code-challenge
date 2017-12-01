set search_path=epbuilder;

delete from report_episode_detail where split_total_cost>=1000000 and filter_id=1;


/*create provider info*/

DROP TABLE IF EXISTS provider_prep;
CREATE TABLE provider_prep AS
SELECT
	p.provider_id AS provider_id,
	MAX(p.provider_name) AS provider_name,
	MAX(p.zipcode) AS provider_zipcode,
	MAX(pt.provider_type) AS provider_type,
	MAX(p.npi) AS provider_npi
FROM
 provider p
LEFT JOIN
  MMCOR_provider_type pt ON p.provider_id = pt.provider_id
GROUP BY
	p.provider_id
;



/*create pac_groups*/



drop table if exists PAC_groups;
create table PAC_groups as
select
episode_builder_5_4_002.episode.episode_id,
episode_builder_5_4_002.episode.name as episode_name,
episode_builder_5_4_002.episode.description as episode_description,
episode_builder_5_4_002.episode_to_code.code_value as code_id,
episode_builder_5_4_002.episode_to_code.code_type_id as type_id,
episode_builder_5_4_002.code.DESCRIPTION as code_name,
episode_builder_5_4_002.group.id as group_id,
episode_builder_5_4_002.group.name as group_name
from episode_builder_5_4_002.episode
join episode_builder_5_4_002.episode_to_code
on episode_builder_5_4_002.episode.episode_id=episode_builder_5_4_002.episode_to_code.episode_id
join episode_builder_5_4_002.code
on episode_builder_5_4_002.episode_to_code.code_value=episode_builder_5_4_002.code.value
and episode_builder_5_4_002.episode_to_code.code_type_id = episode_builder_5_4_002.code.type_id
left join episode_builder_5_4_002.group
on episode_builder_5_4_002.code.group_id=episode_builder_5_4_002.group.id
where episode_builder_5_4_002.episode.VERSION = '5.4.002' and episode_builder_5_4_002.episode_to_code.FUNCTION_ID='cm'


union

select
episode_builder_5_4_002.episode.episode_id,
episode_builder_5_4_002.episode.name as episode_name,
episode_builder_5_4_002.episode.description as episode_description,
episode_builder_5_4_002.episode_to_code_icd10_5_3_007.code_value as code_id,
episode_builder_5_4_002.episode_to_code_icd10_5_3_007.code_type_id as type_id,
episode_builder_5_4_002.code_icd10.DESCRIPTION as code_name,
episode_builder_5_4_002.group.id as group_id,
episode_builder_5_4_002.group.name as group_name
from episode_builder_5_4_002.episode
join episode_builder_5_4_002.episode_to_code_icd10_5_3_007
on episode_builder_5_4_002.episode.episode_id=episode_builder_5_4_002.episode_to_code_icd10_5_3_007.episode_id
join episode_builder_5_4_002.code_icd10
on episode_builder_5_4_002.episode_to_code_icd10_5_3_007.code_value=episode_builder_5_4_002.code_icd10.value
and episode_builder_5_4_002.episode_to_code_icd10_5_3_007.code_type_id = episode_builder_5_4_002.code_icd10.type_id
left join episode_builder_5_4_002.group
on episode_builder_5_4_002.code_icd10.group_id=episode_builder_5_4_002.group.id
where episode_builder_5_4_002.episode.VERSION = '5.4.002' and episode_builder_5_4_002.episode_to_code_icd10_5_3_007.FUNCTION_ID='cm';

drop table if exists PAC_groups_unique;
create table PAC_groups_unique as
select
code_id,
type_id,
1 as counter
from PAC_groups
group by code_id,
type_id;


/*create co occurance of chronic episodes_flag
create a flag for each chronic episode which has the same end year as any other open episode
*/
drop table if exists co_ocurrence_of_chronic_episodes;

CREATE TABLE co_ocurrence_of_chronic_episodes AS
SELECT
			red1.master_episode_id,
			red1.level AS level,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='ASTHMA' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_ASTHMA,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='ARRBLK' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_ARRBLK,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='HF' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_HF,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='COPD' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_COPD,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='CAD' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_CAD,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='ULCLTS' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_ULCLTS,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='BIPLR' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_BIPLR,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='GERD' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_GERD,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='HTN' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_HTN,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='GLCOMA' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_GLCOMA,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='LBP' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_LBP,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='CROHNS' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_CROHNS,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='DIAB' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_DIAB,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='DEPRSN' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_DEPRSN,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='OSTEOA' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_OSTEOA,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='RHNTS' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_RHNTS,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='DIVERT' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_DIVERT,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='DEPANX' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_DEPANX,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='PTSD' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_PTSD,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='SCHIZO' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_SCHIZO,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='SUDS' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_SUDS,
			(CASE WHEN COUNT(CASE WHEN red2.episode_type='Chronic' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_count_chronic,
			(CASE WHEN COUNT(red2.master_episode_id) >1 THEN 1 ELSE 0 END)  AS co_count_all,
			MAX(EXTRACT(YEAR FROM to_date(red1.episode_end_date, 'YYYY-MM-DD'))) as year
FROM
        report_episode_detail red1
INNER JOIN
        report_episode_detail red2 on red1.member_id = red2.member_id
WHERE
        red1.filter_id = 1 AND red2.filter_id = 1 AND  
        red1.member_id = red2.member_id AND
        red1.level = red2.level AND 
        red1.episode_name <> red2.episode_name AND
	(EXTRACT(YEAR FROM to_date(red1.episode_end_date, 'YYYY-MM-DD'))) = (EXTRACT(YEAR FROM to_date(red2.episode_end_date, 'YYYY-MM-DD')))
GROUP BY
        red1.master_episode_id,
        red1.level
;

drop table if exists co_ocurrence_of_chronic_episodes_trend_year1;
CREATE TABLE co_ocurrence_of_chronic_episodes_trend_year1 AS
SELECT
			red1.master_episode_id,
			red1.member_id,
			red1.level AS level,
			red1.episode_name,
			episode_end_date,
			MAX(EXTRACT(YEAR FROM to_date(red1.episode_end_date, 'YYYY-MM-DD')))-1 as year
FROM
        report_episode_detail red1
JOIN build_episode_reference eb ON eb.episode_id=red1.episode_id AND eb.END_OF_STUDY=1
 JOIN  master_epid_level_claim_type_year_1 melprime on melprime.master_episode_id=red1.master_episode_id and melprime.filter_id=1 and melprime.split=1 and melprime.annualized=1    and melprime.claim_type='PB'   
 GROUP BY
 red1.member_id,
 red1.episode_name,
 episode_end_date,
        red1.master_episode_id,
        red1.level;
        

  
drop table if exists co_ocurrence_of_chronic_episodes_trend_year;
CREATE TABLE co_ocurrence_of_chronic_episodes_trend_year AS         
select
			red1.master_episode_id,
			red1.level AS level,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='ASTHMA' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_ASTHMA,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='ARRBLK' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_ARRBLK,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='HF' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_HF,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='COPD' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_COPD,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='CAD' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_CAD,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='ULCLTS' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_ULCLTS,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='BIPLR' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_BIPLR,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='GERD' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_GERD,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='HTN' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_HTN,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='GLCOMA' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_GLCOMA,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='LBP' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_LBP,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='CROHNS' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_CROHNS,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='DIAB' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_DIAB,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='DEPRSN' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_DEPRSN,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='OSTEOA' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_OSTEOA,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='RHNTS' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_RHNTS,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='DIVERT' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_DIVERT,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='DEPANX' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_DEPANX,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='PTSD' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_PTSD,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='SCHIZO' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_SCHIZO,
			(CASE WHEN COUNT(CASE WHEN red2.episode_name='SUDS' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_occurence_count_SUDS,
			(CASE WHEN COUNT(CASE WHEN red2.episode_type='Chronic' THEN red2.master_episode_id END) >0 THEN 1 ELSE 0 END) AS co_count_chronic,
			(CASE WHEN COUNT(red2.master_episode_id) >1 THEN 1 ELSE 0 END)  AS co_count_all,
			MAX(red1.year) as year    
			select red2.*    
 from       co_ocurrence_of_chronic_episodes_trend_year1 red1
 join report_episode_detail red2 on red1.member_id = red2.member_id
         JOIN build_episode_reference eb2 ON eb2.episode_id=red2.episode_id   
 JOIN  master_epid_level_claim_type_year_1 secondary on secondary.master_episode_id=red2.master_episode_id and secondary.filter_id=1 and secondary.split=1 and secondary.annualized=1 and secondary.claim_type='PB'
WHERE
        red2.filter_id = 1 AND  
        red1.member_id = red2.member_id AND
        red1.level = red2.level AND 
        red1.episode_name <> red2.episode_name AND eb2.END_OF_STUDY=1 
        or 
        red2.filter_id = 1 AND  
        red1.member_id = red2.member_id AND
        red1.level = red2.level AND 
        red1.episode_name <> red2.episode_name 
        and eb2.END_OF_STUDY=0 and  (EXTRACT(YEAR FROM to_date(red2.episode_end_date, 'YYYY-MM-DD')))=(EXTRACT(YEAR FROM to_date(red1.episode_end_date, 'YYYY-MM-DD')))-1 
GROUP BY
        red1.master_episode_id,
        red1.level
;

/*create ip,op,pb,rx totals*/

drop table if exists member_vistualization_claim_type_temp;
create table member_vistualization_claim_type_temp as
select
claims_combined.master_claim_id, claims_combined.member_id, 
claims_combined.allowed_amt, 
claims_combined.assigned_count, 
claims_combined.claim_line_type_code, 
claims_combined.begin_date, 
claims_combined.end_date ,
max(case when left(assignment.master_episode_id,6) in ('EX1401','EP1403','EP1404','EX1502') then extract(year from episode.trig_begin_date) end) as trig_begin_year,
/*sum(PAC_groups_unique.counter) as PAC,*/
sum(filter_fail) as filter_fail_total,
sum(case when left(assignment.master_episode_id,6) in ('EX1401','EP1403','EP1404','EX1502') then 1 else 0 end) as maternity_flag,
count(distinct assignment.master_episode_id) as episode_count,
sum(case when r.PPR_Type_Code IS NOT NULL then 1 else 0 end) as ppr,
sum(case when p.preventable_reason IS NOT NULL then 1 else 0 end) as ppv
from claims_combined
left join assignment
on claims_combined.master_claim_id=assignment.master_claim_id
left join filtered_episodes
on assignment.master_episode_id=filtered_episodes.master_episode_id
left join claim_line
on claims_combined.master_claim_id=claim_line.master_claim_id
left join vbp_ppe_ppv p on claim_line.claim_id=left(p.claim_id,14)
left join PPR_2013_RA r on claim_line.claim_id=left(r.Claim_ID,14)
left join episode 
on assignment.master_episode_id=episode.master_episode_id
/*left join PAC_groups_unique on
code.code_value=PAC_groups_unique.code_id and code.nomen=PAC_groups_unique.type_id*/
where filtered_episodes.filter_id=1  or  filtered_episodes.filter_id is null 
group by claims_combined.master_claim_id, claims_combined.member_id, claims_combined.allowed_amt, claims_combined.assigned_count, claims_combined.claim_line_type_code, claims_combined.begin_date, claims_combined.end_date;



drop table if exists member_vistualization_claim_typea;
create table member_vistualization_claim_typea as
select
member_vistualization_claim_type_temp.member_id,
 EXTRACT(YEAR FROM begin_date) as year,
 trig_begin_year,
sum(case when claim_line_type_code = 'IP' then allowed_amt else 0 end )as ip_cost,
sum(case when claim_line_type_code = 'OP' then allowed_amt else 0 end )as op_cost,
sum(case when claim_line_type_code = 'PB' then allowed_amt else 0 end )as pb_cost,
sum(case when claim_line_type_code = 'RX' then allowed_amt else 0 end )as rx_cost,
/*sum(case when PAC >=1 then allowed_amt else 0 end) as pac_cost,*/
sum(case when assigned_count>0  and filter_fail_total<episode_count then allowed_amt else 0 end )as assigned_cost,
sum(case when claim_line_type_code = 'IP' and assigned_count>0 and filter_fail_total<episode_count  then allowed_amt else 0 end )as assigned_ip_cost,
sum(case when claim_line_type_code = 'OP' and assigned_count>0 and filter_fail_total<episode_count  then allowed_amt else 0 end )as assigned_op_cost,
sum(case when claim_line_type_code = 'PB' and assigned_count>0  and filter_fail_total<episode_count then allowed_amt else 0 end )as assigned_pb_cost,
sum(case when claim_line_type_code = 'RX' and assigned_count>0  and filter_fail_total<episode_count then allowed_amt else 0 end )as assigned_rx_cost,
sum(case when assigned_count>0  then allowed_amt else 0 end )as assigned_cost_unfiltered,
sum(case when claim_line_type_code = 'IP' and assigned_count>0  then allowed_amt else 0 end )as assigned_ip_cost_unfiltered,
sum(case when claim_line_type_code = 'OP' and assigned_count>0  then allowed_amt else 0 end )as assigned_op_cost_unfiltered,
sum(case when claim_line_type_code = 'PB' and assigned_count>0  then allowed_amt else 0 end )as assigned_pb_cost_unfiltered,
sum(case when claim_line_type_code = 'RX' and assigned_count>0  then allowed_amt else 0 end )as assigned_rx_cost_unfiltered,
sum(case when ppr>0 then allowed_amt else 0 end ) as PPR,
sum(case when ppv>0 then allowed_amt else 0 end ) as PPV
from member_vistualization_claim_type_temp
where maternity_flag = 0 
group by member_vistualization_claim_type_temp.member_id,  EXTRACT(YEAR FROM begin_date),trig_begin_year;


/*MAT */



drop table if exists maternity_list;
create table maternity_list as
select
mom_baby.ENCRYPT_RECIP_ID_MOM as member_id,
mom_baby.year
from mom_baby
join (select mom_baby.ENCRYPT_RECIP_ID_MOM, year, count(*) as count from mom_baby group by mom_baby.ENCRYPT_RECIP_ID_MOM, mom_baby.year) as mb2
on mom_baby.ENCRYPT_RECIP_ID_MOM=mb2.ENCRYPT_RECIP_ID_MOM and mb2.count=1 and mom_baby.year=mb2.year
join (select mom_baby.ENCRYPT_RECIP_ID_BABY, year, count(*) as count from mom_baby group by mom_baby.ENCRYPT_RECIP_ID_BABY, mom_baby.year) as mb3
on mom_baby.ENCRYPT_RECIP_ID_BABY=mb3.ENCRYPT_RECIP_ID_BABY and mb3.count=1 and mom_baby.year=mb3.year
join (select episode.member_id, count(*) as count from episode where episode_id='EX1502' group by member_id) as mb4
on mom_baby.ENCRYPT_RECIP_ID_BABY=mb4.member_id and mb4.count=1 


union

select
mom_baby.ENCRYPT_RECIP_ID_BABY as member_id,
mom_baby.year
from mom_baby
join (select mom_baby.ENCRYPT_RECIP_ID_MOM, year, count(*) as count from mom_baby group by mom_baby.ENCRYPT_RECIP_ID_MOM, mom_baby.year) as mb2
on mom_baby.ENCRYPT_RECIP_ID_MOM=mb2.ENCRYPT_RECIP_ID_MOM and mb2.count=1 and mom_baby.year=mb2.year
join (select mom_baby.ENCRYPT_RECIP_ID_BABY, year, count(*) as count from mom_baby group by mom_baby.ENCRYPT_RECIP_ID_BABY, mom_baby.year) as mb3
on mom_baby.ENCRYPT_RECIP_ID_BABY=mb3.ENCRYPT_RECIP_ID_BABY and mb3.count=1 and mom_baby.year=mb3.year
join (select episode.member_id, count(*) as count from episode where episode_id='EX1502' group by member_id) as mb4
on mom_baby.ENCRYPT_RECIP_ID_BABY=mb4.member_id and mb4.count=1 

;

create table drop_list as
select
member_id,
extract(year from episode_end_date) as year,
count(*) as count
from episode
left join sub_distinct
on sub_distinct.child_master_episode_id=episode.master_episode_id 
where episode_id = 'EX1401' and sub_distinct.association_level is null 
group by 
member_id,
extract(year from episode_end_date);

DELETE FROM maternity_list
WHERE  member_id  IN (SELECT member_id FROM drop_list WHERE count >1 and maternity_list.member_id=drop_list.member_id and maternity_list.year=drop_list.year);

drop table drop_list;


create table drop_list as
select
member_id,
extract(year from episode_end_date) as year,
count(*) as count
from episode
left join sub_distinct
on sub_distinct.child_master_episode_id=episode.master_episode_id 
where episode_id  in ('EP1403','EP1404')  and sub_distinct.association_level is null 
group by 
member_id,
extract(year from episode_end_date);


DELETE FROM maternity_list
WHERE  member_id  IN (SELECT member_id FROM drop_list WHERE count >1 and maternity_list.member_id=drop_list.member_id and maternity_list.year=drop_list.year);

drop table drop_list;

create table drop_list as
select
member_id,
extract(year from episode_end_date) as year,
count(*) as count
from episode
left join sub_distinct
on sub_distinct.child_master_episode_id=episode.master_episode_id 
where episode_id = 'EX1502' and sub_distinct.association_level is null 
group by 
member_id,
extract(year from episode_end_date);

DELETE FROM maternity_list
WHERE  member_id  IN (SELECT member_id FROM drop_list WHERE count >1 and maternity_list.member_id=drop_list.member_id and maternity_list.year=drop_list.year);

drop table drop_list;


drop table if exists member_vistualization_claim_typemat;
create table member_vistualization_claim_typemat as
select
member_vistualization_claim_type_temp.member_id,
 EXTRACT(YEAR FROM begin_date) as year,
 trig_begin_year,
case when maternity_list.member_id is null then  sum(case when claim_line_type_code = 'IP' then allowed_amt else 0 end ) else (sum(case when claim_line_type_code = 'IP' then allowed_amt else 0 end )-sum(case when claim_line_type_code = 'IP' and assigned_count>0 and filter_fail_total<assigned_count  then (allowed_amt/assigned_count)*(maternity_flag) else 0 end )) end as ip_cost,
case when maternity_list.member_id is null then  sum(case when claim_line_type_code = 'OP' then allowed_amt else 0 end ) else (sum(case when claim_line_type_code = 'OP' then allowed_amt else 0 end )-sum(case when claim_line_type_code = 'OP' and assigned_count>0 and filter_fail_total<assigned_count  then (allowed_amt/assigned_count)*(maternity_flag) else 0 end )) end as op_cost,
case when maternity_list.member_id is null then  sum(case when claim_line_type_code = 'PB' then allowed_amt else 0 end ) else (sum(case when claim_line_type_code = 'PB' then allowed_amt else 0 end )-sum(case when claim_line_type_code = 'PB' and assigned_count>0 and filter_fail_total<assigned_count  then (allowed_amt/assigned_count)*(maternity_flag) else 0 end )) end as pb_cost,
case when maternity_list.member_id is null then  sum(case when claim_line_type_code = 'RX' then allowed_amt else 0 end ) else (sum(case when claim_line_type_code = 'RX' then allowed_amt else 0 end )-sum(case when claim_line_type_code = 'RX' and assigned_count>0 and filter_fail_total<assigned_count  then (allowed_amt/assigned_count)*(maternity_flag) else 0 end )) end as rx_cost,
/*sum(case when PAC >=1 then allowed_amt else 0 end) as pac_cost,*/
sum(case when assigned_count>0  and filter_fail_total<maternity_flag then allowed_amt/assigned_count else 0 end )as assigned_cost,
sum(case when claim_line_type_code = 'IP' and assigned_count>0 and filter_fail_total<maternity_flag  then allowed_amt/assigned_count else 0 end )as assigned_ip_cost,
sum(case when claim_line_type_code = 'OP' and assigned_count>0 and filter_fail_total<maternity_flag  then allowed_amt/assigned_count else 0 end )as assigned_op_cost,
sum(case when claim_line_type_code = 'PB' and assigned_count>0  and filter_fail_total<maternity_flag then allowed_amt/assigned_count else 0 end )as assigned_pb_cost,
sum(case when claim_line_type_code = 'RX' and assigned_count>0  and filter_fail_total<maternity_flag then allowed_amt/assigned_count else 0 end )as assigned_rx_cost,
sum(case when assigned_count>0  then allowed_amt else 0 end )as assigned_cost_unfiltered,
sum(case when claim_line_type_code = 'IP' and assigned_count>0  then allowed_amt else 0 end )as assigned_ip_cost_unfiltered,
sum(case when claim_line_type_code = 'OP' and assigned_count>0  then allowed_amt else 0 end )as assigned_op_cost_unfiltered,
sum(case when claim_line_type_code = 'PB' and assigned_count>0  then allowed_amt else 0 end )as assigned_pb_cost_unfiltered,
sum(case when claim_line_type_code = 'RX' and assigned_count>0  then allowed_amt else 0 end )as assigned_rx_cost_unfiltered,
sum(case when ppr>0 then allowed_amt else 0 end ) as PPR,
sum(case when ppv>0 then allowed_amt else 0 end ) as PPV
from member_vistualization_claim_type_temp
left join maternity_list
on member_vistualization_claim_type_temp.member_id=maternity_list.member_id and member_vistualization_claim_type_temp.trig_begin_year=maternity_list.year
where maternity_flag <>0  and maternity_flag<>assigned_count  
group by member_vistualization_claim_type_temp.member_id,  EXTRACT(YEAR FROM begin_date), trig_begin_year, maternity_list.member_id;





/* grab hiv code level pacs*/
drop table if exists hiv_code_lvl_pacs;

create table hiv_code_lvl_pacs as
select 
member_id,
claims_combined.master_claim_id,
extract(year from begin_date) as year,
claim_line_type_code,
allowed_amt
from claims_combined
join code
on claims_combined.id=code.u_c_id
right join hiv_pacs
on hiv_pacs.dx_code=code.code_value
where assigned_count=0 or assigned_count is null and code.nomen='DX'
group by member_id, claims_combined.master_claim_id, claim_line_type_code, allowed_amt, extract(year from begin_date)

union

/*add ICD 10 code PACs*/
select 
member_id,
claims_combined.master_claim_id,
extract(year from begin_date) as year,
claim_line_type_code,
allowed_amt
from claims_combined
join code
on claims_combined.id=code.u_c_id
right join hiv_pacs_icd10
on hiv_pacs_icd10.dx_code=code.code_value
where assigned_count=0 or assigned_count is null and code.nomen='DX'
group by member_id, claims_combined.master_claim_id, claim_line_type_code, allowed_amt, extract(year from begin_date)
;

drop table if exists hiv_code_lvl_pac_totals;
create table hiv_code_lvl_pac_totals as
select 
member_id,
year,
sum(allowed_amt) as pac_cost
from hiv_code_lvl_pacs
group by member_id, year
;




drop table if exists member_vistualization_claim_type;
create table member_vistualization_claim_type as
select
enrolled_month.member_id,
enrolled_month.year,
enrolled_month.enrolled_month,
enrolled_month.year - member_sub_population2.birth_year as age,
member_sub_population2.zip_code,
member_sub_population2.county,
member_sub_population2.mcregion,
member_sub_population2.PPS,
member_sub_population2.MCO,
member_sub_population2.HH,
member_sub_population2.PCP,
member_sub_population2.exclusive,
case when member_sub_population2.sub_population in ('HIV','HARP','HARP & HIV') then (ifnull(member_vistualization_claim_typea.ip_cost,0)+ifnull(member_vistualization_claim_typemat.assigned_ip_cost_unfiltered,0)) else (ifnull(member_vistualization_claim_typea.ip_cost,0)+ifnull(member_vistualization_claim_typemat.ip_cost,0)) end as ip_cost,
case when member_sub_population2.sub_population in ('HIV','HARP','HARP & HIV') then (ifnull(member_vistualization_claim_typea.op_cost,0)+ifnull(member_vistualization_claim_typemat.assigned_op_cost_unfiltered,0)) else (ifnull(member_vistualization_claim_typea.op_cost,0)+ifnull(member_vistualization_claim_typemat.op_cost,0)) end as op_cost,
case when member_sub_population2.sub_population in ('HIV','HARP','HARP & HIV') then (ifnull(member_vistualization_claim_typea.pb_cost,0)+ifnull(member_vistualization_claim_typemat.assigned_pb_cost_unfiltered,0)) else (ifnull(member_vistualization_claim_typea.pb_cost,0)+ifnull(member_vistualization_claim_typemat.pb_cost,0)) end as pb_cost,
case when member_sub_population2.sub_population in ('HIV','HARP','HARP & HIV') then (ifnull(member_vistualization_claim_typea.rx_cost,0)+ifnull(member_vistualization_claim_typemat.assigned_rx_cost_unfiltered,0)) else (ifnull(member_vistualization_claim_typea.rx_cost,0)+ifnull(member_vistualization_claim_typemat.rx_cost,0)) end as rx_cost,

case when member_sub_population2.sub_population in ('HIV','HARP','HARP & HIV') then (ifnull(member_vistualization_claim_typea.assigned_cost,0)+ifnull(member_vistualization_claim_typemat.assigned_cost,0)) else ifnull(member_vistualization_claim_typea.assigned_cost,0) end as assigned_cost,
case when member_sub_population2.sub_population in ('HIV','HARP','HARP & HIV') then (ifnull(member_vistualization_claim_typea.assigned_ip_cost,0)+ifnull(member_vistualization_claim_typemat.assigned_ip_cost,0)) else ifnull(member_vistualization_claim_typea.assigned_ip_cost,0) end as assigned_ip_cost,
case when member_sub_population2.sub_population in ('HIV','HARP','HARP & HIV') then (ifnull(member_vistualization_claim_typea.assigned_op_cost,0)+ifnull(member_vistualization_claim_typemat.assigned_op_cost,0)) else ifnull(member_vistualization_claim_typea.assigned_op_cost,0) end as assigned_op_cost,
case when member_sub_population2.sub_population in ('HIV','HARP','HARP & HIV') then (ifnull(member_vistualization_claim_typea.assigned_pb_cost,0)+ifnull(member_vistualization_claim_typemat.assigned_pb_cost,0)) else ifnull(member_vistualization_claim_typea.assigned_pb_cost,0) end as assigned_pb_cost,
case when member_sub_population2.sub_population in ('HIV','HARP','HARP & HIV') then (ifnull(member_vistualization_claim_typea.assigned_rx_cost,0)+ifnull(member_vistualization_claim_typemat.assigned_rx_cost,0)) else ifnull(member_vistualization_claim_typea.assigned_rx_cost,0) end as assigned_rx_cost,

case when member_sub_population2.sub_population in ('HIV','HARP','HARP & HIV') then (ifnull(member_vistualization_claim_typea.assigned_cost_unfiltered,0)+ifnull(member_vistualization_claim_typemat.assigned_cost_unfiltered,0)) else ifnull(member_vistualization_claim_typea.assigned_cost_unfiltered,0) end as assigned_cost_unfiltered,
case when member_sub_population2.sub_population in ('HIV','HARP','HARP & HIV') then (ifnull(member_vistualization_claim_typea.assigned_ip_cost_unfiltered,0)+ifnull(member_vistualization_claim_typemat.assigned_ip_cost_unfiltered,0)) else ifnull(member_vistualization_claim_typea.assigned_ip_cost_unfiltered ,0)end as assigned_ip_cost_unfiltered,
case when member_sub_population2.sub_population in ('HIV','HARP','HARP & HIV') then (ifnull(member_vistualization_claim_typea.assigned_op_cost_unfiltered,0)+ifnull(member_vistualization_claim_typemat.assigned_op_cost_unfiltered,0)) else ifnull(member_vistualization_claim_typea.assigned_op_cost_unfiltered,0) end as assigned_op_cost_unfiltered,
case when member_sub_population2.sub_population in ('HIV','HARP','HARP & HIV') then (ifnull(member_vistualization_claim_typea.assigned_pb_cost_unfiltered,0)+ifnull(member_vistualization_claim_typemat.assigned_pb_cost_unfiltered,0)) else ifnull(member_vistualization_claim_typea.assigned_pb_cost_unfiltered,0) end as assigned_pb_cost_unfiltered,
case when member_sub_population2.sub_population in ('HIV','HARP','HARP & HIV') then (ifnull(member_vistualization_claim_typea.assigned_rx_cost_unfiltered,0)+ifnull(member_vistualization_claim_typemat.assigned_rx_cost_unfiltered,0)) else ifnull(member_vistualization_claim_typea.assigned_rx_cost_unfiltered,0) end as assigned_rx_cost_unfiltered,

ifnull(member_vistualization_claim_typea.PPR,0) as PPR,
ifnull(member_vistualization_claim_typea.PPV,0) as PPV,
sum(ifnull(master_epid_level.cost_c,0)) as pac_cost,
(CASE
                WHEN enrolled_month.year - member_sub_population2.birth_year < 6 THEN '< 6'
                WHEN enrolled_month.year - member_sub_population2.birth_year BETWEEN 6 and 11 THEN '6 - 11'
                WHEN enrolled_month.year - member_sub_population2.birth_year BETWEEN 12 and 17 THEN '12 - 17'
                WHEN enrolled_month.year - member_sub_population2.birth_year BETWEEN 18 and 44 THEN '18 - 44'
                WHEN enrolled_month.year - member_sub_population2.birth_year BETWEEN 45 and 64 THEN '45 - 64'
                WHEN enrolled_month.year - member_sub_population2.birth_year BETWEEN 65 and 200 THEN '>= 65'
                WHEN enrolled_month.year - member_sub_population2.birth_year > 200 THEN 'Unknown'
   END) AS cms_age_group,
   member_sub_population2.gender as gender,
   member_sub_population2.sub_population as member_population
from enrolled_month
left join member_vistualization_claim_typea
on member_vistualization_claim_typea.member_id=enrolled_month.member_id and member_vistualization_claim_typea.year=enrolled_month.year
left join  episode
on member_vistualization_claim_typea.member_id=episode.member_id and member_vistualization_claim_typea.year=EXTRACT(YEAR FROM episode.episode_end_date)
left join master_epid_level 
on master_epid_level.master_episode_id=episode.master_episode_id and level=5 and split=1 and annualized=1 and filter_id=1 and left(master_epid_level.master_episode_id,6) in 
('ES9901','EC0801','EC0802','EC1001','EC1903','EC1905','EC1906','EC1909','EC1910','EC0401','EC0402','EC0508','EC0511','EC0518','EC0521','EC0601') 
left join member_sub_population2 
on enrolled_month.member_id=member_sub_population2.member_id and member_sub_population2.year=enrolled_month.year 
left join member_vistualization_claim_typemat
on member_vistualization_claim_typemat.member_id=enrolled_month.member_id and member_vistualization_claim_typemat.year=enrolled_month.year 

group by enrolled_month.member_id,
enrolled_month.year, member_vistualization_claim_typea.member_id,
member_vistualization_claim_typea.year, member_sub_population2.year,
member_vistualization_claim_typea.ip_cost,
member_vistualization_claim_typea.op_cost,
member_vistualization_claim_typea.pb_cost,
member_vistualization_claim_typea.rx_cost,
member_vistualization_claim_typea.assigned_cost,
member_vistualization_claim_typea.assigned_ip_cost,
member_vistualization_claim_typea.assigned_op_cost,
member_vistualization_claim_typea.assigned_pb_cost,
member_vistualization_claim_typea.assigned_rx_cost,
member_vistualization_claim_typea.assigned_cost_unfiltered,
member_vistualization_claim_typea.assigned_ip_cost_unfiltered,
member_vistualization_claim_typea.assigned_op_cost_unfiltered,enrolled_month.enrolled_month,
member_vistualization_claim_typea.assigned_pb_cost_unfiltered,
member_vistualization_claim_typea.assigned_rx_cost_unfiltered, member_vistualization_claim_typea.PPR,
member_vistualization_claim_typea.PPV,member_sub_population2.gender,    member_sub_population2.sub_population, member_sub_population2.birth_year,member_sub_population2.zip_code,
member_sub_population2.county,
member_sub_population2.mcregion,
member_sub_population2.PPS,
member_sub_population2.MCO,
member_sub_population2.HH,
member_sub_population2.PCP,
member_sub_population2.exclusive,
member_vistualization_claim_typemat.ip_cost,
member_vistualization_claim_typemat.op_cost,
member_vistualization_claim_typemat.pb_cost,
member_vistualization_claim_typemat.rx_cost,
member_vistualization_claim_typemat.assigned_cost,
member_vistualization_claim_typemat.assigned_ip_cost,
member_vistualization_claim_typemat.assigned_op_cost,
member_vistualization_claim_typemat.assigned_pb_cost,
member_vistualization_claim_typemat.assigned_rx_cost,
member_vistualization_claim_typemat.assigned_cost_unfiltered,
member_vistualization_claim_typemat.assigned_ip_cost_unfiltered,
member_vistualization_claim_typemat.assigned_op_cost_unfiltered,enrolled_month.enrolled_month,
member_vistualization_claim_typemat.assigned_pb_cost_unfiltered,
member_vistualization_claim_typemat.assigned_rx_cost_unfiltered;
/*within episode claim cost totals START HERE*/

/*create ip,op,pb,rx totals*/
/*add Crg_summary queries in here*/




DROP table IF EXISTS exp_cost_qacrg3_age_gender;
CREATE table exp_cost_qacrg3_age_gender AS
SELECT
	crg.year AS r_year,
        fincrg AS r_fincrg,
        cms_age_group as r_cms_age_group,
        gender AS r_gender_code,
        sum(ip_cost+ op_cost+ pb_cost+rx_cost) ,
        sum(ip_cost+ op_cost+ pb_cost+rx_cost)/ COUNT(distinct cc.member_id) AS r_group_exp_cost,
        sum(PPR)/ COUNT(distinct cc.member_id) AS ppr_group_exp_cost,
        sum(PPV)/ COUNT(distinct cc.member_id) AS ppv_group_exp_cost

FROM
    crg
LEFT JOIN 
       member_vistualization_claim_type cc ON cc.member_id = crg.recip_id AND cc.year = crg.year
LEFT JOIN 
       enrolled_month  ON enrolled_month.member_id = crg.recip_id AND cc.year = crg.year
       left join member
on enrolled_month.member_id=member.member_id
WHERE
	cc.member_population NOT IN ('MLTC', 'DD') and enrolled_month.enrolled_month=12 and member.dual_eligible<>1
GROUP BY
        crg.year,
        crg.fincrg,
        cms_age_group,
        gender
;





/* Cost summarization for each member */
DROP TABLE IF EXISTS crg_cost_summary_by_member CASCADE;
CREATE TABLE crg_cost_summary_by_member
(
	member_id varchar(50) DEFAULT NULL,
	year varchar(20) DEFAULT NULL,
	cms_age_group varchar(255) DEFAULT NULL,
	gender varchar(20) DEFAULT NULL,
	fincrg varchar(20) DEFAULT NULL,
	qacrg3_desc varchar(140) DEFAULT NULL,
	actual_cost numeric(13,2) DEFAULT NULL,
	expected_cost numeric(13,2) DEFAULT NULL,
ppr_expected_cost numeric(13,2) DEFAULT NULL,
ppv_expected_cost numeric(13,2) DEFAULT NULL
)

; 

insert /*+ direct */ INTO crg_cost_summary_by_member
SELECT
        cc.member_id,
        cc.year,
        cc.cms_age_group,
        cc.gender,
        MAX(crg.fincrg),
	MAX(crg.qacrg3_desc),
        sum(ip_cost+ op_cost+ pb_cost+rx_cost) 
FROM
        member_vistualization_claim_type cc
LEFT JOIN
        crg ON cc.member_id = recip_id AND cc.year = crg.year
               left join member
on cc.member_id=member.member_id
where member.dual_eligible <>1
GROUP BY
        cc.member_id,
        cc.year,
        cc.cms_age_group,
        cc.gender
;


update /*+ direct */ crg_cost_summary_by_member
SET actual_cost = 0
WHERE actual_cost IS NULL
;


/* update /*+ direct */ the expected cost field*/
update /*+ direct */ crg_cost_summary_by_member
SET
	expected_cost = r_group_exp_cost,
		ppr_expected_cost = ppr_group_exp_cost,
			ppv_expected_cost = ppv_group_exp_cost
	
FROM exp_cost_qacrg3_age_gender gec
WHERE year = r_year AND gender = r_gender_code AND cms_age_group = r_cms_age_group  AND fincrg = r_fincrg
;





alter table report_episode_detail add column year numeric(4);

update /*+ direct */  report_episode_detail 
set 
year=EXTRACT(YEAR FROM episode.episode_end_date)
from episode 
where episode.master_episode_id = report_episode_detail.master_episode_id ;


alter table report_episode_detail add column enrolled_month integer;

update /*+ direct */  report_episode_detail 
set 
enrolled_month=enrolled_month.enrolled_month
from enrolled_month 
where enrolled_month.member_id = report_episode_detail.member_id and enrolled_month.year=report_episode_detail.year ;



alter table report_episode_detail add column co_occurence_count_DEPANX integer;
alter table report_episode_detail add column co_occurence_count_DIVERT integer;
alter table report_episode_detail add column co_occurence_count_RHNTS integer;
alter table report_episode_detail add column co_occurence_count_OSTEOA integer;
alter table report_episode_detail add column co_occurence_count_DIAB integer;
alter table report_episode_detail add column co_occurence_count_DEPRSN integer;
alter table report_episode_detail add column co_occurence_count_LBP integer;
alter table report_episode_detail add column co_occurence_count_CROHNS integer;
alter table report_episode_detail add column co_occurence_count_GLCOMA integer;
alter table report_episode_detail add column co_occurence_count_HTN integer;
alter table report_episode_detail add column co_occurence_count_GERD integer;
alter table report_episode_detail add column co_occurence_count_BIPLR integer;
alter table report_episode_detail add column co_occurence_count_ULCLTS integer;
alter table report_episode_detail add column co_occurence_count_CAD integer;
alter table report_episode_detail add column co_occurence_count_COPD integer;
alter table report_episode_detail add column co_occurence_count_HF integer;
alter table report_episode_detail add column co_occurence_count_ARRBLK integer;
alter table report_episode_detail add column co_occurence_count_ASTHMA integer;
alter table report_episode_detail add column co_occurence_count_PTSD integer;
alter table report_episode_detail add column co_occurence_count_SCHIZO integer;
alter table report_episode_detail add column co_occurence_count_SUDS integer;
alter table report_episode_detail add column co_count_chronic integer;
alter table report_episode_detail add column co_count_all integer;


update /*+ direct */  report_episode_detail 
set 
co_occurence_count_ASTHMA=co_ocurrence_of_chronic_episodes.co_occurence_count_ASTHMA,
co_occurence_count_ARRBLK=co_ocurrence_of_chronic_episodes.co_occurence_count_ARRBLK,
co_occurence_count_HF=co_ocurrence_of_chronic_episodes.co_occurence_count_HF,
co_occurence_count_COPD=co_ocurrence_of_chronic_episodes.co_occurence_count_COPD,
co_occurence_count_CAD=co_ocurrence_of_chronic_episodes.co_occurence_count_CAD,
co_occurence_count_ULCLTS=co_ocurrence_of_chronic_episodes.co_occurence_count_ULCLTS,
co_occurence_count_BIPLR=co_ocurrence_of_chronic_episodes.co_occurence_count_BIPLR,
co_occurence_count_GERD=co_ocurrence_of_chronic_episodes.co_occurence_count_GERD,
co_occurence_count_HTN=co_ocurrence_of_chronic_episodes.co_occurence_count_HTN,
co_occurence_count_GLCOMA=co_ocurrence_of_chronic_episodes.co_occurence_count_GLCOMA,
co_occurence_count_LBP=co_ocurrence_of_chronic_episodes.co_occurence_count_LBP,
co_occurence_count_CROHNS=co_ocurrence_of_chronic_episodes.co_occurence_count_CROHNS,
co_occurence_count_DIAB=co_ocurrence_of_chronic_episodes.co_occurence_count_DIAB,
co_occurence_count_DEPRSN=co_ocurrence_of_chronic_episodes.co_occurence_count_DEPRSN,
co_occurence_count_OSTEOA=co_ocurrence_of_chronic_episodes.co_occurence_count_OSTEOA,
co_occurence_count_RHNTS=co_ocurrence_of_chronic_episodes.co_occurence_count_RHNTS,
co_occurence_count_DIVERT=co_ocurrence_of_chronic_episodes.co_occurence_count_DIVERT,
co_occurence_count_DEPANX=co_ocurrence_of_chronic_episodes.co_occurence_count_DEPANX,
co_occurence_count_PTSD=co_ocurrence_of_chronic_episodes.co_occurence_count_PTSD,
co_occurence_count_SCHIZO=co_ocurrence_of_chronic_episodes.co_occurence_count_SCHIZO,
co_occurence_count_SUDS=co_ocurrence_of_chronic_episodes.co_occurence_count_SUDS,
co_count_chronic=co_ocurrence_of_chronic_episodes.co_count_chronic,
co_count_all=co_ocurrence_of_chronic_episodes.co_count_all
from co_ocurrence_of_chronic_episodes 
where report_episode_detail.master_episode_id=co_ocurrence_of_chronic_episodes.master_episode_id and report_episode_detail.level=co_ocurrence_of_chronic_episodes.level ;



alter table report_episode_detail add column ip_cost float;
alter table report_episode_detail add column op_cost float;
alter table report_episode_detail add column pb_cost float;
alter table report_episode_detail add column rx_cost float;


alter table report_episode_detail add column END_OF_STUDY integer;

update /*+ direct */ report_episode_detail
set END_OF_STUDY=build_episode_reference.END_OF_STUDY
from build_episode_reference
where report_episode_detail.episode_id=build_episode_reference.episode_id;


drop table if exists claim_type_costs;
create table claim_type_costs as
select 
master_epid_level_claim_type.master_episode_id, master_epid_level_claim_type.level, master_epid_level_claim_type.filter_id,
sum(case when build_episode_reference.END_OF_STUDY=1 and split=1 and annualized=1 and claim_type='IP' then master_epid_level_claim_type.cost 
                 when build_episode_reference.END_OF_STUDY=0 and split=1 and annualized=0 and claim_type='IP' then master_epid_level_claim_type.cost else 0 end ) as ip_cost,
sum(case when build_episode_reference.END_OF_STUDY=1 and split=1 and annualized=1 and claim_type='OP' then master_epid_level_claim_type.cost 
                 when build_episode_reference.END_OF_STUDY=0 and split=1 and annualized=0 and claim_type='OP' then master_epid_level_claim_type.cost else 0 end ) as op_cost,
sum(case when build_episode_reference.END_OF_STUDY=1 and split=1 and annualized=1 and claim_type='PB' then master_epid_level_claim_type.cost 
                 when build_episode_reference.END_OF_STUDY=0 and split=1 and annualized=0 and claim_type='PB' then master_epid_level_claim_type.cost else 0 end ) as pb_cost,
sum(case when build_episode_reference.END_OF_STUDY=1 and split=1 and annualized=1 and claim_type='RX' then master_epid_level_claim_type.cost 
                 when build_episode_reference.END_OF_STUDY=0 and split=1 and annualized=0 and claim_type='RX' then master_epid_level_claim_type.cost else 0 end ) as rx_cost
from master_epid_level_claim_type 
left join build_episode_reference
on  left(master_epid_level_claim_type.master_episode_id,6) = build_episode_reference.episode_id  
join filtered_episodes
on master_epid_level_claim_type.master_episode_id=filtered_episodes.master_episode_id
where filtered_episodes.filter_id=1 and filtered_episodes.filter_fail=0 
  group by master_epid_level_claim_type.master_episode_id, master_epid_level_claim_type.level, master_epid_level_claim_type.filter_id;
 





alter table report_episode_detail add column Facility_ID_provider_name varchar(100);
alter table report_episode_detail add column Facility_ID_provider_zipcode varchar(100);
alter table report_episode_detail add column Facility_ID_provider_type varchar(100);
alter table report_episode_detail add column Physician_ID_provider_name varchar(100);
alter table report_episode_detail add column Physician_ID_provider_zipcode varchar(1000);
alter table report_episode_detail add column Physician_ID_provider_type varchar(100);
/*START HERE*/
update /*+ direct */ report_episode_detail
set 
Physician_ID_provider_name=p1.provider_name  ,
Physician_ID_provider_zipcode=p1.provider_zipcode  ,
Physician_ID_provider_type=p1.provider_type  
from provider_prep p1
where report_episode_detail.physician_id=p1.provider_id;

update /*+ direct */ report_episode_detail
set 
Physician_ID_provider_name=case when Physician_ID_provider_name is null then  p1.provider_name  else Physician_ID_provider_name end,
Physician_ID_provider_zipcode=case when Physician_ID_provider_zipcode is null then  p1.provider_zipcode  else Physician_ID_provider_zipcode end,
Physician_ID_provider_type=case when Physician_ID_provider_type is null then  p1.provider_type  else Physician_ID_provider_type end
from provider_prep p1
where report_episode_detail.physician_id=p1.provider_npi;


update /*+ direct */ report_episode_detail
set 
Facility_ID_provider_name=p2.provider_name  ,
Facility_ID_provider_zipcode=p2.provider_zipcode ,
Facility_ID_provider_type=p2.provider_type 
from  provider_prep p2
where report_episode_detail.facility_id=p2.provider_id;





/*NOTES
This is all based off of filter_id 1
Columns have been added for unsplit costs
This field is missing exp_cost_sa_comp_other_l1*/





drop table if exists visual_analysis_table_js; 

create table visual_analysis_table_js as
select
cast('Episode' as varchar(12)) as Analysis_type,
report_episode_detail.master_episode_id as id,
report_episode_detail.episode_id,
cast(report_episode_detail.episode_name as varchar(20)) as episode_name,
report_episode_detail.episode_description,
report_episode_detail.episode_type,
cast('' as varchar(30)) as episode_category,
report_episode_detail.level as episode_level,
report_episode_detail.member_id,
report_episode_detail.year - member_sub_population2.birth_year AS member_age,
(CASE
                WHEN report_episode_detail.year - member_sub_population2.birth_year < 6 THEN '< 6'
                WHEN report_episode_detail.year - member_sub_population2.birth_year BETWEEN 6 and 11 THEN '6 - 11'
                WHEN report_episode_detail.year - member_sub_population2.birth_year BETWEEN 12 and 17 THEN '12 - 17'
                WHEN report_episode_detail.year - member_sub_population2.birth_year BETWEEN 18 and 44 THEN '18 - 44'
                WHEN report_episode_detail.year - member_sub_population2.birth_year BETWEEN 45 and 64 THEN '45 - 64'
                WHEN report_episode_detail.year - member_sub_population2.birth_year BETWEEN 65 and 200 THEN '>= 65'
                WHEN report_episode_detail.year - member_sub_population2.birth_year > 200 THEN 'Unknown'
   END) AS cms_age_group,
   member_sub_population2.gender as gender,
   member_sub_population2.zip_code AS member_zipcode,
   member_sub_population2.county as member_county,
   member_sub_population2.sub_population as member_population,
 member_sub_population2.mcregion as member_region,
case when report_episode_detail.END_OF_STUDY=1 then report_episode_detail.Annualized_Split_Total_Cost else report_episode_detail.Split_Total_Cost end as Split_Total_Cost,
case when report_episode_detail.END_OF_STUDY=1 then report_episode_detail.Annualized_Split_Total_PAC_Cost else report_episode_detail.Split_Total_PAC_Cost  end as Split_Total_PAC_Cost,
case when report_episode_detail.END_OF_STUDY=1 then report_episode_detail.Annualized_Split_Total_Typical_Cost+report_episode_detail.Annualized_Split_Total_TypicalwPAC_Cost else report_episode_detail.Split_Total_Typical_Cost+report_episode_detail.Split_Total_TypicalwPAC_Cost end  as Split_Total_Typical_Cost,
cast(null as numeric(40,20))  as Unsplit_Total_Cost,
cast(null as numeric(40,20))  as Unsplit_Total_PAC_Cost,
cast(null as numeric(40,20))  as Unsplit_Total_Typical_Cost,
case when report_episode_detail.Split_Expected_Total_Cost<0 then 0 else report_episode_detail.Split_Expected_Total_Cost end as Split_Expected_Total_Cost,
case when report_episode_detail.Split_Expected_Typical_IP_Cost<0 then 0 else   report_episode_detail.Split_Expected_Typical_IP_Cost end as Split_Expected_Typical_IP_Cost,
case when report_episode_detail.Split_Expected_Typical_Other_Cost<0 then 0 else   report_episode_detail.Split_Expected_Typical_Other_Cost end as Split_Expected_Typical_Other_Cost,
case when report_episode_detail.Split_Expected_PAC_Cost<0 then 0 else   report_episode_detail.Split_Expected_PAC_Cost end as Split_Expected_PAC_Cost,
cast(null as numeric(40,20)) as Unsplit_Expected_Total_Cost,
cast(null as numeric(40,20)) as Unsplit_Expected_Typical_IP_Cost,
cast(null as numeric(40,20)) as Unsplit_Expected_Typical_Other_Cost,
cast(null as numeric(40,20)) as Unsplit_Expected_PAC_Cost,
claim_type_costs.ip_cost,
claim_type_costs.op_cost,
claim_type_costs.pb_cost,
claim_type_costs.rx_cost,
cast(null as numeric(40,20)) as assigned_cost,
cast(null as  numeric(40,20)) as assigned_ip_cost,
cast(null as  numeric(40,20)) as assigned_op_cost,
cast(null as  numeric(40,20)) as assigned_pb_cost,
cast(null as  numeric(40,20)) as assigned_rx_cost,
cast(null as  numeric(40,20)) as assigned_cost_unfiltered,
cast(null as  numeric(40,20)) as assigned_ip_cost_unfiltered,
cast(null as  numeric(40,20)) as assigned_op_cost_unfiltered,
cast(null as  numeric(40,20)) as assigned_pb_cost_unfiltered,
cast(null as  numeric(40,20)) as assigned_rx_cost_unfiltered,
member_sub_population2.pps,
report_episode_detail.Facility_ID,
report_episode_detail.Facility_ID_provider_name  AS Facility_ID_provider_name,
report_episode_detail.Facility_ID_provider_zipcode  AS Facility_ID_provider_zipcode,
report_episode_detail.Facility_ID_provider_type  AS Facility_ID_provider_type,
report_episode_detail.Physician_ID,
report_episode_detail.Physician_ID_provider_name  AS Physician_ID_provider_name,
report_episode_detail.Physician_ID_provider_zipcode  AS Physician_ID_provider_zipcode,
report_episode_detail.Physician_ID_provider_type  AS Physician_ID_provider_type,
member_sub_population2.mco,
member_sub_population2.hh,
member_sub_population2.pcp,
cast(null as varchar(200)) as vbp_attrib_provider,
cast(null as varchar(12)) as vbp_attrib_provider_zipcode,
cast(null as varchar(200)) as vbp_contractor,
'' as subgroup,
report_episode_detail.year,
case when (case when claim_type_costs.ip_cost is null then 0 else claim_type_costs.ip_cost end )+ 
     (case when claim_type_costs.op_cost is null then 0 else claim_type_costs.op_cost end )+ 
     (case when claim_type_costs.pb_cost is null then 0 else claim_type_costs.pb_cost end )+ 
     (case when claim_type_costs.rx_cost is null then 0 else claim_type_costs.rx_cost end )<200 then 'Low' else 'Non-Low' end  AS utilization,
'' as ppr,
'' as ppv,
member_sub_population2.exclusive as exclusive,
report_episode_detail.co_occurence_count_ASTHMA,
report_episode_detail.co_occurence_count_ARRBLK,
report_episode_detail.co_occurence_count_HF,
report_episode_detail.co_occurence_count_COPD,
report_episode_detail.co_occurence_count_CAD,
report_episode_detail.co_occurence_count_ULCLTS,
report_episode_detail.co_occurence_count_BIPLR,
report_episode_detail.co_occurence_count_GERD,
report_episode_detail.co_occurence_count_HTN,
report_episode_detail.co_occurence_count_GLCOMA,
report_episode_detail.co_occurence_count_LBP,
report_episode_detail.co_occurence_count_CROHNS,
report_episode_detail.co_occurence_count_DIAB,
report_episode_detail.co_occurence_count_DEPRSN,
report_episode_detail.co_occurence_count_OSTEOA,
report_episode_detail.co_occurence_count_RHNTS,
report_episode_detail.co_occurence_count_DIVERT,
report_episode_detail.co_occurence_count_DEPANX,
report_episode_detail.co_occurence_count_PTSD,
report_episode_detail.co_occurence_count_SCHIZO,
report_episode_detail.co_occurence_count_SUDS,
report_episode_detail.co_count_chronic as co_occurence_count_chronic,
report_episode_detail.co_count_all as co_occurence_count_all,
cast(null as float) as episode_count,
cast(null as float) as episode_count_unfiltered,
cast(null as varchar(12)) as qcrg_code,
cast(null as varchar(12)) as qcrg_desc,
cast(null as varchar(12)) as qacrg1_code,
cast(null as varchar(12)) as qacrg1_desc,
cast(null as varchar(12)) as qacrg2_code,
cast(null as varchar(12)) as qacrg2_desc,
cast(null as varchar(12)) as qacrg3_code,
cast(null as varchar(12)) as qacrg3_desc,
cast(null as varchar(12)) as fincrg, 
cast(null as varchar(12)) as q_base,
cast(null as varchar(12)) as q_severity,
report_episode_detail.enrolled_month as enrolled_num_month,
case when member_sub_population2.sub_population = 'General' AND report_episode_detail.
episode_name IN ('URI', 'RHNTS', 'PREVNT', 'SICKCR', 'TONSIL', 'IPC')then 'Integrated Primary Care' 
     when member_sub_population2.sub_population = 'General' AND report_episode_detail.episode_name IN ('PREGN','CSECT', 'VAGDEL', 'NBORN','MTRNTY')then 'Maternity Care' 
     when member_sub_population2.sub_population = 'General' AND report_episode_detail.episode_type = 'Chronic' AND report_episode_detail. episode_name NOT IN ('RHNTS', 'DIVERT', 'ULCLTS', 'GLCOMA', 'SCHIZO', 'CROHNS' ,'ADHD')then 'Chronic Bundle' else null end
     as vbp_arrangement,
cast(null as float) as state_wide_avg_exp_cost,
cast(null as float) as state_wide_exp_pac_rate,
cast(null as float) as state_wide_avg_total_cost,
cast(null as float) as state_wide_pac_percent,
cast(null as float) as state_wide_pac_rate,
cast(null as float) as state_wide_female_percent,
cast(null as float) as state_wide_male_percent,
cast(null as float) as state_wide_NU_percent,
cast(null as float) as state_wide_LU_percent,
cast(null as float) as state_wide_percent_co_ASTHMA,
cast(null as float) as state_wide_percent_co_ARRBLK,
cast(null as float) as state_wide_percent_co_HF,
cast(null as float) as state_wide_percent_co_COPD,
cast(null as float) as state_wide_percent_co_CAD,
cast(null as float) as state_wide_percent_co_ULCLTS,
cast(null as float) as state_wide_percent_co_BIPLR,
cast(null as float) as state_wide_percent_co_GERD,
cast(null as float) as state_wide_percent_co_HTN,
cast(null as float) as state_wide_percent_co_GLCOMA,
cast(null as float) as state_wide_percent_co_LBP,
cast(null as float) as state_wide_percent_co_CROHNS,
cast(null as float) as state_wide_percent_co_DIAB,
cast(null as float) as state_wide_percent_co_DEPRSN,
cast(null as float) as state_wide_percent_co_OSTEOA,
cast(null as float) as state_wide_percent_co_RHNTS,
cast(null as float) as state_wide_percent_co_DIVERT,
cast(null as float) as state_wide_percent_co_DEPANX,
cast(null as float) as state_wide_percent_co_PTSD,
cast(null as float) as state_wide_percent_co_SCHIZO,
cast(null as float) as state_wide_percent_co_SUDS,
cast(null as numeric(40,20)) as ppr_expected_cost,
cast(null as  numeric(40,20)) as ppv_expected_cost
from report_episode_detail
left join member_sub_population2 on
report_episode_detail.member_id=member_sub_population2.member_id and member_sub_population2.year=report_episode_detail.year
left join claim_type_costs 
on claim_type_costs.master_episode_id = report_episode_detail.master_episode_id and 
      claim_type_costs.level = report_episode_detail.level and 
      claim_type_costs.filter_id = report_episode_detail.filter_id

left join sub_distinct
on sub_distinct.child_master_episode_id=report_episode_detail.master_episode_id 

 where episode_id not in ('EX1502','EX1401','EP1403','EP1404') and report_episode_detail.filter_id = 1 and  report_episode_detail.END_OF_STUDY=1 and report_episode_detail.Annualized_Split_Total_Cost>0 and report_episode_detail.level>2 and sub_distinct.association_level is null and report_episode_detail.enrolled_month=12

 or episode_id not in ('EX1502','EX1401','EP1403','EP1404') and  report_episode_detail.filter_id = 1 and report_episode_detail.END_OF_STUDY=0 and report_episode_detail.level>2 and sub_distinct.association_level is null  and report_episode_detail.enrolled_month=12
 
 or episode_id not in ('EX1502','EX1401','EP1403','EP1404') and report_episode_detail.filter_id = 1 and  report_episode_detail.END_OF_STUDY=1 and report_episode_detail.Annualized_Split_Total_Cost>0 and report_episode_detail.level>2 and report_episode_detail.level<sub_distinct.association_level  and report_episode_detail.enrolled_month=12

 or  episode_id not in ('EX1502','EX1401','EP1403','EP1404') and report_episode_detail.filter_id = 1 and report_episode_detail.END_OF_STUDY=0 and report_episode_detail.level>2 and report_episode_detail.level<sub_distinct.association_level  and report_episode_detail.enrolled_month=12
;
alter table report_episode_detail add column trig_begin_year integer;
update /*+ direct */ report_episode_detail
set trig_begin_year=extract(year from episode.trig_begin_date)
from episode 
where report_episode_detail.master_episode_id=episode.master_episode_id;

insert /*+ direct */ into visual_analysis_table_js 
select
cast('Episode' as varchar(12)) as Analysis_type,
report_episode_detail.master_episode_id as id,
report_episode_detail.episode_id,
cast(report_episode_detail.episode_name as varchar(20)) as episode_name,
report_episode_detail.episode_description,
report_episode_detail.episode_type,
cast('' as varchar(30)) as episode_category,
report_episode_detail.level as episode_level,
report_episode_detail.member_id,
report_episode_detail.year - member_sub_population2.birth_year AS member_age,
(CASE
                WHEN report_episode_detail.year - member_sub_population2.birth_year < 6 THEN '< 6'
                WHEN report_episode_detail.year - member_sub_population2.birth_year BETWEEN 6 and 11 THEN '6 - 11'
                WHEN report_episode_detail.year - member_sub_population2.birth_year BETWEEN 12 and 17 THEN '12 - 17'
                WHEN report_episode_detail.year - member_sub_population2.birth_year BETWEEN 18 and 44 THEN '18 - 44'
                WHEN report_episode_detail.year - member_sub_population2.birth_year BETWEEN 45 and 64 THEN '45 - 64'
                WHEN report_episode_detail.year - member_sub_population2.birth_year BETWEEN 65 and 200 THEN '>= 65'
                WHEN report_episode_detail.year - member_sub_population2.birth_year > 200 THEN 'Unknown'
   END) AS cms_age_group,
   member_sub_population2.gender as gender,
   member_sub_population2.zip_code AS member_zipcode,
   member_sub_population2.county as member_county,
   member_sub_population2.sub_population as member_population,
 member_sub_population2.mcregion as member_region,
case when report_episode_detail.END_OF_STUDY=1 then report_episode_detail.Annualized_Split_Total_Cost else report_episode_detail.Split_Total_Cost end as Split_Total_Cost,
case when report_episode_detail.END_OF_STUDY=1 then report_episode_detail.Annualized_Split_Total_PAC_Cost else report_episode_detail.Split_Total_PAC_Cost  end as Split_Total_PAC_Cost,
case when report_episode_detail.END_OF_STUDY=1 then report_episode_detail.Annualized_Split_Total_Typical_Cost+report_episode_detail.Annualized_Split_Total_TypicalwPAC_Cost else report_episode_detail.Split_Total_Typical_Cost+report_episode_detail.Split_Total_TypicalwPAC_Cost end  as Split_Total_Typical_Cost,
cast(null as numeric(40,20))  as Unsplit_Total_Cost,
cast(null as numeric(40,20))  as Unsplit_Total_PAC_Cost,
cast(null as numeric(40,20))  as Unsplit_Total_Typical_Cost,
case when report_episode_detail.Split_Expected_Total_Cost<0 then 0 else report_episode_detail.Split_Expected_Total_Cost end as Split_Expected_Total_Cost,
case when report_episode_detail.Split_Expected_Typical_IP_Cost<0 then 0 else   report_episode_detail.Split_Expected_Typical_IP_Cost end as Split_Expected_Typical_IP_Cost,
case when report_episode_detail.Split_Expected_Typical_Other_Cost<0 then 0 else   report_episode_detail.Split_Expected_Typical_Other_Cost end as Split_Expected_Typical_Other_Cost,
case when report_episode_detail.Split_Expected_PAC_Cost<0 then 0 else   report_episode_detail.Split_Expected_PAC_Cost end as Split_Expected_PAC_Cost,
cast(null as numeric(40,20)) as Unsplit_Expected_Total_Cost,
cast(null as numeric(40,20)) as Unsplit_Expected_Typical_IP_Cost,
cast(null as numeric(40,20)) as Unsplit_Expected_Typical_Other_Cost,
cast(null as numeric(40,20)) as Unsplit_Expected_PAC_Cost,
claim_type_costs.ip_cost,
claim_type_costs.op_cost,
claim_type_costs.pb_cost,
claim_type_costs.rx_cost,
cast(null as numeric(40,20)) as assigned_cost,
cast(null as  numeric(40,20)) as assigned_ip_cost,
cast(null as  numeric(40,20)) as assigned_op_cost,
cast(null as  numeric(40,20)) as assigned_pb_cost,
cast(null as  numeric(40,20)) as assigned_rx_cost,
cast(null as  numeric(40,20)) as assigned_cost_unfiltered,
cast(null as  numeric(40,20)) as assigned_ip_cost_unfiltered,
cast(null as  numeric(40,20)) as assigned_op_cost_unfiltered,
cast(null as  numeric(40,20)) as assigned_pb_cost_unfiltered,
cast(null as  numeric(40,20)) as assigned_rx_cost_unfiltered,
member_sub_population2.pps,
report_episode_detail.Facility_ID,
report_episode_detail.Facility_ID_provider_name  AS Facility_ID_provider_name,
report_episode_detail.Facility_ID_provider_zipcode  AS Facility_ID_provider_zipcode,
report_episode_detail.Facility_ID_provider_type  AS Facility_ID_provider_type,
report_episode_detail.Physician_ID,
report_episode_detail.Physician_ID_provider_name  AS Physician_ID_provider_name,
report_episode_detail.Physician_ID_provider_zipcode  AS Physician_ID_provider_zipcode,
report_episode_detail.Physician_ID_provider_type  AS Physician_ID_provider_type,
member_sub_population2.mco,
member_sub_population2.hh,
member_sub_population2.pcp,
cast(null as varchar(200)) as vbp_attrib_provider,
cast(null as varchar(12)) as vbp_attrib_provider_zipcode,
cast(null as varchar(200)) as vbp_contractor,
'' as subgroup,
report_episode_detail.year,
case when (case when claim_type_costs.ip_cost is null then 0 else claim_type_costs.ip_cost end )+ 
     (case when claim_type_costs.op_cost is null then 0 else claim_type_costs.op_cost end )+ 
     (case when claim_type_costs.pb_cost is null then 0 else claim_type_costs.pb_cost end )+ 
     (case when claim_type_costs.rx_cost is null then 0 else claim_type_costs.rx_cost end )<200 then 'Low' else 'Non-Low' end  AS utilization,
'' as ppr,
'' as ppv,
member_sub_population2.exclusive as exclusive,
report_episode_detail.co_occurence_count_ASTHMA,
report_episode_detail.co_occurence_count_ARRBLK,
report_episode_detail.co_occurence_count_HF,
report_episode_detail.co_occurence_count_COPD,
report_episode_detail.co_occurence_count_CAD,
report_episode_detail.co_occurence_count_ULCLTS,
report_episode_detail.co_occurence_count_BIPLR,
report_episode_detail.co_occurence_count_GERD,
report_episode_detail.co_occurence_count_HTN,
report_episode_detail.co_occurence_count_GLCOMA,
report_episode_detail.co_occurence_count_LBP,
report_episode_detail.co_occurence_count_CROHNS,
report_episode_detail.co_occurence_count_DIAB,
report_episode_detail.co_occurence_count_DEPRSN,
report_episode_detail.co_occurence_count_OSTEOA,
report_episode_detail.co_occurence_count_RHNTS,
report_episode_detail.co_occurence_count_DIVERT,
report_episode_detail.co_occurence_count_DEPANX,
report_episode_detail.co_occurence_count_PTSD,
report_episode_detail.co_occurence_count_SCHIZO,
report_episode_detail.co_occurence_count_SUDS,
report_episode_detail.co_count_chronic as co_occurence_count_chronic,
report_episode_detail.co_count_all as co_occurence_count_all,
cast(null as float) as episode_count,
cast(null as float) as episode_count_unfiltered,
cast(null as varchar(12)) as qcrg_code,
cast(null as varchar(12)) as qcrg_desc,
cast(null as varchar(12)) as qacrg1_code,
cast(null as varchar(12)) as qacrg1_desc,
cast(null as varchar(12)) as qacrg2_code,
cast(null as varchar(12)) as qacrg2_desc,
cast(null as varchar(12)) as qacrg3_code,
cast(null as varchar(12)) as qacrg3_desc,
cast(null as varchar(12)) as fincrg, 
cast(null as varchar(12)) as q_base,
cast(null as varchar(12)) as q_severity,
report_episode_detail.enrolled_month as enrolled_num_month,
case when member_sub_population2.sub_population = 'General' AND report_episode_detail.
episode_name IN ('URI', 'RHNTS', 'PREVNT', 'SICKCR', 'TONSIL', 'IPC')then 'Integrated Primary Care' 
     when member_sub_population2.sub_population = 'General' AND report_episode_detail.episode_name IN ('PREGN','CSECT', 'VAGDEL', 'NBORN','MTRNTY')then 'Maternity Care' 
     when member_sub_population2.sub_population = 'General' AND report_episode_detail.episode_type = 'Chronic' AND report_episode_detail. episode_name NOT IN ('RHNTS', 'DIVERT', 'ULCLTS', 'GLCOMA', 'SCHIZO', 'CROHNS' ,'ADHD')then 'Chronic Bundle' else null end
     as vbp_arrangement,
cast(null as float) as state_wide_avg_exp_cost,
cast(null as float) as state_wide_exp_pac_rate,
cast(null as float) as state_wide_avg_total_cost,
cast(null as float) as state_wide_pac_percent,
cast(null as float) as state_wide_pac_rate,
cast(null as float) as state_wide_female_percent,
cast(null as float) as state_wide_male_percent,
cast(null as float) as state_wide_NU_percent,
cast(null as float) as state_wide_LU_percent,
cast(null as float) as state_wide_percent_co_ASTHMA,
cast(null as float) as state_wide_percent_co_ARRBLK,
cast(null as float) as state_wide_percent_co_HF,
cast(null as float) as state_wide_percent_co_COPD,
cast(null as float) as state_wide_percent_co_CAD,
cast(null as float) as state_wide_percent_co_ULCLTS,
cast(null as float) as state_wide_percent_co_BIPLR,
cast(null as float) as state_wide_percent_co_GERD,
cast(null as float) as state_wide_percent_co_HTN,
cast(null as float) as state_wide_percent_co_GLCOMA,
cast(null as float) as state_wide_percent_co_LBP,
cast(null as float) as state_wide_percent_co_CROHNS,
cast(null as float) as state_wide_percent_co_DIAB,
cast(null as float) as state_wide_percent_co_DEPRSN,
cast(null as float) as state_wide_percent_co_OSTEOA,
cast(null as float) as state_wide_percent_co_RHNTS,
cast(null as float) as state_wide_percent_co_DIVERT,
cast(null as float) as state_wide_percent_co_DEPANX,
cast(null as float) as state_wide_percent_co_PTSD,
cast(null as float) as state_wide_percent_co_SCHIZO,
cast(null as float) as state_wide_percent_co_SUDS,
cast(null as numeric(40,20)) as ppr_expected_cost,
cast(null as  numeric(40,20)) as ppv_expected_cost


from report_episode_detail

JOIN maternity_list on report_episode_detail.member_id=maternity_list.member_id and report_episode_detail.trig_begin_year=maternity_list.year

left join member_sub_population2 on
report_episode_detail.member_id=member_sub_population2.member_id and member_sub_population2.year=report_episode_detail.year
left join claim_type_costs 
on claim_type_costs.master_episode_id = report_episode_detail.master_episode_id and 
      claim_type_costs.level = report_episode_detail.level and 
      claim_type_costs.filter_id = report_episode_detail.filter_id

left join sub_distinct
on sub_distinct.child_master_episode_id=report_episode_detail.master_episode_id 

 where  episode_id  in ('EX1502','EX1401','EP1403','EP1404') and  report_episode_detail.filter_id = 1  and report_episode_detail.level>2 and sub_distinct.association_level is null 

 or  episode_id  in ('EX1502','EX1401','EP1403','EP1404') and report_episode_detail.filter_id = 1  and report_episode_detail.level>2 and report_episode_detail.level<sub_distinct.association_level  
;

 /* THE ISSUE IS THE YEAR!!!!!!!*/

 /********* Add previous year costs for trending **************************/
 
 alter table visual_analysis_table_js add column year_for_trend numeric(4);

update /*+ direct */  visual_analysis_table_js 
set 
year_for_trend=year-1
 ;


drop table if exists master_epid_level_claim_type_year_1_rolled_up;
  
create table master_epid_level_claim_type_year_1_rolled_up as
select
filter_id,
master_episode_id,
level,
split,
annualized,
	sum(master_epid_level_claim_type_year_1.cost) as Split_Total_Cost,
sum(master_epid_level_claim_type_year_1.cost_c) as Split_Total_PAC_Cost,
sum(master_epid_level_claim_type_year_1.cost_t) as Split_Total_Typical_Cost,
	sum(case when master_epid_level_claim_type_year_1.claim_type='IP' then master_epid_level_claim_type_year_1.cost else 0 end) AS ip_cost,
	sum(case when master_epid_level_claim_type_year_1.claim_type='OP' then master_epid_level_claim_type_year_1.cost else 0 end) AS op_cost,
	sum(case when master_epid_level_claim_type_year_1.claim_type='PB' then master_epid_level_claim_type_year_1.cost else 0 end) AS pb_cost,
	sum(case when master_epid_level_claim_type_year_1.claim_type='RX' then master_epid_level_claim_type_year_1.cost else 0 end) AS rx_cost
	from master_epid_level_claim_type_year_1
	group by filter_id,
master_episode_id,
level,
split,
annualized;

drop table if exists trend_pac_costs;
create table trend_pac_costs as
select
filter_id,
level,
episode.member_id,
sum(Split_Total_PAC_Cost) as Split_Total_PAC_Cost,
(EXTRACT(YEAR FROM (episode_end_date)))-1 as year 
from master_epid_level_claim_type_year_1_rolled_up
left join episode on 
 master_epid_level_claim_type_year_1_rolled_up.master_episode_id=episode.master_episode_id 
 
where level=5 and split=1 and annualized=1 and filter_id=1 and left(master_epid_level_claim_type_year_1_rolled_up.master_episode_id,6) in 
('ES9901','EC0801','EC0802','EC1001','EC1903','EC1905','EC1906','EC1909','EC1910','EC0401','EC0402','EC0508','EC0511','EC0518','EC0521','EC0601') 
group by episode.member_id,filter_id,episode_end_date,
level;



 /*next add chronic */
insert /*+ direct */ INTO visual_analysis_table_js
SELECT
	'Episode' AS analysis_type,
	r.id AS id,
	r.episode_id AS episode_id,
	r.episode_name AS episode_name,
	r.episode_description AS episode_description,
	r.episode_type AS episode_type,
	r.episode_category AS episode_category,
	r.episode_level AS episode_level,
	r.member_id AS member_id,
	r.member_age-1 AS member_age,
	r.cms_age_group AS cms_age_group,
	r.gender AS gender,
	r.member_zipcode AS member_zipcode,
	r.member_county AS member_county,
	member_sub_population2.sub_population AS member_population,
	r.member_region as member_region,
master_epid_level_claim_type_year_1_rolled_up.Split_Total_Cost as Split_Total_Cost,
master_epid_level_claim_type_year_1_rolled_up.Split_Total_PAC_Cost as Split_Total_PAC_Cost,
master_epid_level_claim_type_year_1_rolled_up.Split_Total_Typical_Cost as Split_Total_Typical_Cost,
null as Unsplit_Total_Cost,
null as  Unsplit_Total_PAC_Cost,
null as  Unsplit_Total_Typical_Cost,
case when r.episode_name in ('PREVNT', 'SICKCR') then sum(r.Split_Expected_Total_Cost) else 0 end as Split_Expected_Total_Cost,
case when r.episode_name in ('PREVNT', 'SICKCR') then sum(r.Split_Expected_Typical_IP_Cost) else 0 end  as Split_Expected_Typical_IP_Cost,
case when r.episode_name in ('PREVNT', 'SICKCR') then sum(r.Split_Expected_Typical_Other_Cost) else 0 end  as Split_Expected_Typical_Other_Cost,
case when r.episode_name in ('PREVNT', 'SICKCR') then sum(r.Split_Expected_PAC_Cost) else 0 end  as Split_Expected_PAC_Cost,
null as  Unsplit_Expected_Total_Cost,
null as  Unsplit_Expected_Typical_IP_Cost,
null as  Unsplit_Expected_Typical_Other_Cost,
null as  Unsplit_Expected_PAC_Cost,
master_epid_level_claim_type_year_1_rolled_up.ip_cost AS ip_cost,
master_epid_level_claim_type_year_1_rolled_up.op_cost AS op_cost,
master_epid_level_claim_type_year_1_rolled_up.pb_cost AS pb_cost,
master_epid_level_claim_type_year_1_rolled_up.rx_cost AS rx_cost,
	sum(r.assigned_cost) AS assigned_cost,
	sum(r.assigned_ip_cost) AS assigned_ip_cost,
	sum(r.assigned_op_cost) AS assigned_op_cost,
	sum(r.assigned_pb_cost) AS assigned_pb_cost,
	sum(r.assigned_rx_cost) AS assigned_rx_cost,
	sum(r.assigned_cost_unfiltered) AS assigned_cost_unfiltered,
	sum(r.assigned_ip_cost_unfiltered) AS assigned_ip_cost_unfiltered,
	sum(r.assigned_op_cost_unfiltered) AS assigned_op_cost_unfiltered,
	sum(r.assigned_pb_cost_unfiltered) AS assigned_pb_cost_unfiltered,
	sum(r.assigned_rx_cost_unfiltered) AS assigned_rx_cost_unfiltered, 
	MAX(r.pps) AS pps,
null as Facility_ID,
null as Facility_ID_provider_name,
null as Facility_ID_provider_zipcode,
null as Facility_ID_provider_type,
null as Physician_ID,
null as Physician_ID_provider_name,
null as Physician_ID_provider_zipcode,
null as Physician_ID_provider_type,
	MAX(r.mco) AS mco,
	MAX(r.hh) AS hh,
	MAX(r.pcp) AS pcp,
	MAX(r.vbp_attrib_provider) AS vbp_attrib_provider,
	MAX(r.vbp_attrib_provider_zipcode) AS vbp_attrib_provider_zipcode,
	MAX(r.vbp_contractor) AS vbp_contractor,
	r.subgroup as subgroup,
	year_for_trend AS year,
case when (case when master_epid_level_claim_type_year_1_rolled_up.ip_cost is null then 0 else master_epid_level_claim_type_year_1_rolled_up.ip_cost end )+ 
     (case when master_epid_level_claim_type_year_1_rolled_up.op_cost is null then 0 else master_epid_level_claim_type_year_1_rolled_up.op_cost end )+ 
     (case when master_epid_level_claim_type_year_1_rolled_up.pb_cost is null then 0 else master_epid_level_claim_type_year_1_rolled_up.pb_cost end )+ 
     (case when master_epid_level_claim_type_year_1_rolled_up.rx_cost is null then 0 else master_epid_level_claim_type_year_1_rolled_up.rx_cost end )<200 then 'Low' else 'Non-Low' end  AS utilization,
	MAX(r.ppr) AS ppr,
	MAX(r.ppv) AS ppv,
	r.exclusive AS exclusive,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_ASTHMA) AS co_occurence_count_ASTHMA,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_ARRBLK) AS co_occurence_count_ARRBLK,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_HF) AS co_occurence_count_HF,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_COPD) AS co_occurence_count_COPD,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_CAD) AS co_occurence_count_CAD,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_ULCLTS) AS co_occurence_count_ULCLTS,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_BIPLR) AS co_occurence_count_BIPLR,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_GERD) AS co_occurence_count_GERD,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_HTN) AS co_occurence_count_HTN,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_GLCOMA) AS co_occurence_count_GLCOMA,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_LBP) AS co_occurence_count_LBP,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_CROHNS) AS co_occurence_count_CROHNS,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_DIAB) AS co_occurence_count_DIAB,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_DEPRSN) AS co_occurence_count_DEPRSN,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_OSTEOA) AS co_occurence_count_OSTEOA,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_RHNTS) AS co_occurence_count_RHNTS,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_DIVERT) AS co_occurence_count_DIVERT,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_DEPANX) AS co_occurence_count_DEPANX,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_PTSD) AS co_occurence_count_PTSD,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_SCHIZO) AS co_occurence_count_SCHIZO,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_occurence_count_SUDS) AS co_occurence_count_SUDS,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_count_chronic) AS co_occurence_count_chronic,
	MAX(co_ocurrence_of_chronic_episodes_trend_year.co_count_all) as co_occurence_count_all,

	null as episode_count,
null as episode_count_unfiltered,
null as qcrg_code,
null as qcrg_desc,
null as qacrg1_code,
null as qacrg1_desc,
null as qacrg2_code,
null as qacrg2_desc,
null as qacrg3_code,
null as qacrg3_desc,
null as fincrg,
null as q_base,
null as q_severity,
enrolled_month.enrolled_month as enrolled_num_month,
r.vbp_arrangement as vbp_arrangement,
null as state_wide_avg_exp_cost,
null as state_wide_exp_pac_rate,
null as state_wide_avg_total_cost,
null as state_wide_pac_percent,
null as state_wide_pac_rate,
null as state_wide_female_percent,
null as state_wide_male_percent,
null as state_wide_NU_percent,
null as state_wide_LU_percent,
null as state_wide_percent_co_ASTHMA,
null as state_wide_percent_co_ARRBLK,
null as state_wide_percent_co_HF,
null as state_wide_percent_co_COPD,
null as state_wide_percent_co_CAD,
null as state_wide_percent_co_ULCLTS,
null as state_wide_percent_co_BIPLR,
null as state_wide_percent_co_GERD,
null as state_wide_percent_co_HTN,
null as state_wide_percent_co_GLCOMA,
null as state_wide_percent_co_LBP,
null as state_wide_percent_co_CROHNS,
null as state_wide_percent_co_DIAB,
null as state_wide_percent_co_DEPRSN,
null as state_wide_percent_co_OSTEOA,
null as state_wide_percent_co_RHNTS,
null as state_wide_percent_co_DIVERT,
null as state_wide_percent_co_DEPANX,
null as state_wide_percent_co_PTSD,
null as state_wide_percent_co_SCHIZO,
null as state_wide_percent_co_SUDS,
null as ppr_expected_cost,
null as ppv_expected_cost,
year_for_trend as year_for_trend

FROM
	visual_analysis_table_js r
join master_epid_level_claim_type_year_1_rolled_up
on master_epid_level_claim_type_year_1_rolled_up.master_episode_id=r.id and master_epid_level_claim_type_year_1_rolled_up.level=r.episode_level
left join enrolled_month
on r.member_id=enrolled_month.member_id and enrolled_month.year=r.year_for_trend
left join co_ocurrence_of_chronic_episodes_trend_year
on r.id=co_ocurrence_of_chronic_episodes_trend_year.master_episode_id and r.episode_level=co_ocurrence_of_chronic_episodes_trend_year.level 
left join member_sub_population2 on
r.member_id=member_sub_population2.member_id and r.year_for_trend=member_sub_population2.year
WHERE
	r.analysis_type = 'Episode'
	AND r.episode_type='Chronic'
	AND r.episode_name not in ('RHNTS','DIVERT', 'ULCLTS', 'GLCOMA', 'SCHIZO', 'CROHNS','ADHD')
	AND r.episode_level = 5 and enrolled_month.enrolled_month=12 and master_epid_level_claim_type_year_1_rolled_up.Split_Total_Cost>0
	
	or 
	
		r.analysis_type = 'Episode'
	AND r.episode_name in ('PREVNT', 'SICKCR')
	AND  r.episode_level = 5 and enrolled_month.enrolled_month=12  and master_epid_level_claim_type_year_1_rolled_up.Split_Total_Cost>0
GROUP BY
r.id, r.episode_id,
	r.episode_name,
	r.episode_description,
	r.episode_type ,
	r.episode_category ,
	r.episode_level ,
	r.member_id ,member_sub_population2.member_id,
	r.member_age ,
	r.cms_age_group ,
	r.gender ,
	r.member_zipcode ,
	r.member_county ,
	r.member_population ,
	r.member_region,
	r.subgroup,
	r.exclusive,
	r.year_for_trend,member_sub_population2.year,
	master_epid_level_claim_type_year_1_rolled_up.Split_Total_Cost,
	master_epid_level_claim_type_year_1_rolled_up.Split_Total_PAC_Cost,
	master_epid_level_claim_type_year_1_rolled_up.Split_Total_Typical_Cost,
	master_epid_level_claim_type_year_1_rolled_up.ip_cost,
	master_epid_level_claim_type_year_1_rolled_up.op_cost,
	master_epid_level_claim_type_year_1_rolled_up.pb_cost,
	master_epid_level_claim_type_year_1_rolled_up.rx_cost,
	r.ppr,
	r.ppv,
	enrolled_month.enrolled_month,
	r.vbp_arrangement,
	r.year_for_trend
	;
	
	
	
	  
	  
alter table visual_analysis_table_js drop column year_for_trend;


  /********* add prevent and sick care averages for expected costs **********************/


DROP TABLE IF EXISTS temp_sick_pvnt_expected;
CREATE TABLE temp_sick_pvnt_expected as
SELECT
		member_population,
		episode_id,
		year,
                      member_region,
                      episode_level,
		AVG(split_total_cost) as Split_Expected_Total_Cost,
		AVG(Split_Total_Typical_Cost) as Split_Expected_Typical_Other_Cost,
		AVG(Split_Total_PAC_Cost) as Split_Expected_PAC_Cost

	FROM
		visual_analysis_table_js
		left join member 
		on member.member_id=visual_analysis_table_js.member_id
	WHERE
		episode_id ='ES9901'  and split_total_cost>0 and dual_eligible<>1
	GROUP BY
		member_population,
		episode_id,
		year,
                      member_region,
                      episode_level
	;
	

	  
update /*+ direct */ visual_analysis_table_js
set 
Split_Expected_Total_Cost=case when visual_analysis_table_js.episode_id ='ES9901' then p1.Split_Expected_Total_Cost when visual_analysis_table_js.episode_id ='EX9901' then 0 else  visual_analysis_table_js.Split_Expected_Total_Cost end,
Split_Expected_Typical_Other_Cost=case when visual_analysis_table_js.episode_id ='ES9901' then p1.Split_Expected_Typical_Other_Cost when visual_analysis_table_js.episode_id ='EX9901' then 0 else  visual_analysis_table_js.Split_Expected_Typical_Other_Cost end,
Split_Expected_PAC_Cost=case when visual_analysis_table_js.episode_id ='ES9901' then p1.Split_Expected_PAC_Cost when visual_analysis_table_js.episode_id ='EX9901' then 0 else  visual_analysis_table_js.Split_Expected_PAC_Cost end
from temp_sick_pvnt_expected p1
where 
visual_analysis_table_js.member_population=p1.member_population and 
visual_analysis_table_js.episode_id=p1.episode_id and 
visual_analysis_table_js.episode_level=p1.episode_level and 
visual_analysis_table_js.year=p1.year and
visual_analysis_table_js.member_region=p1.member_region;

 /********* Chronic bundle **************************/


insert /*+ direct */ INTO visual_analysis_table_js
SELECT
	'Episode' AS analysis_type,
	'Chronic_bundle' || '-' || r.member_id || '-' || r.year AS id,
	 'CHRNC' AS episode_id,
	'Chronic bundle' AS episode_name,
	'Chronic bundle' AS episode_description,
	'Chronic' AS episode_type,
	'High relevance' AS episode_category,
	5 AS episode_level,
	r.member_id AS member_id,
	MAX(r.member_age) AS member_age,
	MAX(r.cms_age_group) AS cms_age_group,
	MAX(r.gender) AS gender,
	MAX(r.member_zipcode) AS member_zipcode,
	MAX(r.member_county) AS member_county,
	r.member_population AS member_population,
	r.member_region as member_region,
	sum(r.Split_Total_Cost) as Split_Total_Cost,
sum(r.Split_Total_PAC_Cost) as Split_Total_PAC_Cost,
sum(r.Split_Total_Typical_Cost) as Split_Total_Typical_Cost,
null as Unsplit_Total_Cost,
null as  Unsplit_Total_PAC_Cost,
null as  Unsplit_Total_Typical_Cost,
sum(r.Split_Expected_Total_Cost) as Split_Expected_Total_Cost,
sum(r.Split_Expected_Typical_IP_Cost) as Split_Expected_Typical_IP_Cost,
sum(r.Split_Expected_Typical_Other_Cost) as Split_Expected_Typical_Other_Cost,
sum(r.Split_Expected_PAC_Cost) as Split_Expected_PAC_Cost,
null as  Unsplit_Expected_Total_Cost,
null as  Unsplit_Expected_Typical_IP_Cost,
null as  Unsplit_Expected_Typical_Other_Cost,
null as  Unsplit_Expected_PAC_Cost,
	sum(r.ip_cost) AS ip_cost,
	sum(r.op_cost) AS op_cost,
	sum(r.pb_cost) AS pb_cost,
	sum(r.rx_cost) AS rx_cost,
	sum(r.assigned_cost) AS assigned_cost,
	sum(r.assigned_ip_cost) AS assigned_ip_cost,
	sum(r.assigned_op_cost) AS assigned_op_cost,
	sum(r.assigned_pb_cost) AS assigned_pb_cost,
	sum(r.assigned_rx_cost) AS assigned_rx_cost,
	sum(r.assigned_cost_unfiltered) AS assigned_cost_unfiltered,
	sum(r.assigned_ip_cost_unfiltered) AS assigned_ip_cost_unfiltered,
	sum(r.assigned_op_cost_unfiltered) AS assigned_op_cost_unfiltered,
	sum(r.assigned_pb_cost_unfiltered) AS assigned_pb_cost_unfiltered,
	sum(r.assigned_rx_cost_unfiltered) AS assigned_rx_cost_unfiltered, 
	MAX(r.pps) AS pps,
null as Facility_ID,
null as Facility_ID_provider_name,
null as Facility_ID_provider_zipcode,
null as Facility_ID_provider_type,
null as Physician_ID,
null as Physician_ID_provider_name,
null as Physician_ID_provider_zipcode,
null as Physician_ID_provider_type,
	MAX(r.mco) AS mco,
	MAX(r.hh) AS hh,
	MAX(r.pcp) AS pcp,
	MAX(r.vbp_attrib_provider) AS vbp_attrib_provider,
	MAX(r.vbp_attrib_provider_zipcode) AS vbp_attrib_provider_zipcode,
	MAX(r.vbp_contractor) AS vbp_contractor,
	r.subgroup as subgroup,
	r.year AS year,
	MAX(r.utilization) AS utilization,
	MAX(r.ppr) AS ppr,
	MAX(r.ppv) AS ppv,
	r.exclusive AS exclusive,
	MAX(r.co_occurence_count_ASTHMA) AS co_occurence_count_ASTHMA,
	MAX(r.co_occurence_count_ARRBLK) AS co_occurence_count_ARRBLK,
	MAX(r.co_occurence_count_HF) AS co_occurence_count_HF,
	MAX(r.co_occurence_count_COPD) AS co_occurence_count_COPD,
	MAX(r.co_occurence_count_CAD) AS co_occurence_count_CAD,
	MAX(r.co_occurence_count_ULCLTS) AS co_occurence_count_ULCLTS,
	MAX(r.co_occurence_count_BIPLR) AS co_occurence_count_BIPLR,
	MAX(r.co_occurence_count_GERD) AS co_occurence_count_GERD,
	MAX(r.co_occurence_count_HTN) AS co_occurence_count_HTN,
	MAX(r.co_occurence_count_GLCOMA) AS co_occurence_count_GLCOMA,
	MAX(r.co_occurence_count_LBP) AS co_occurence_count_LBP,
	MAX(r.co_occurence_count_CROHNS) AS co_occurence_count_CROHNS,
	MAX(r.co_occurence_count_DIAB) AS co_occurence_count_DIAB,
	MAX(r.co_occurence_count_DEPRSN) AS co_occurence_count_DEPRSN,
	MAX(r.co_occurence_count_OSTEOA) AS co_occurence_count_OSTEOA,
	MAX(r.co_occurence_count_RHNTS) AS co_occurence_count_RHNTS,
	MAX(r.co_occurence_count_DIVERT) AS co_occurence_count_DIVERT,
	MAX(r.co_occurence_count_DEPANX) AS co_occurence_count_DEPANX,
	MAX(r.co_occurence_count_PTSD) AS co_occurence_count_PTSD,
	MAX(r.co_occurence_count_SCHIZO) AS co_occurence_count_SCHIZO,
	MAX(r.co_occurence_count_SUDS) AS co_occurence_count_SUDS,
	MAX(r.co_occurence_count_chronic) AS co_occurence_count_chronic,
	MAX(r.co_occurence_count_all) as co_occurence_count_all,

	null as episode_count,
null as episode_count_unfiltered,
null as qcrg_code,
null as qcrg_desc,
null as qacrg1_code,
null as qacrg1_desc,
null as qacrg2_code,
null as qacrg2_desc,
null as qacrg3_code,
null as qacrg3_desc,
null as fincrg,
null as q_base,
null as q_severity,
max(r.enrolled_num_month) as enrolled_num_month,
'Chronic Bundle' as vbp_arrangement,
null as state_wide_avg_exp_cost,
null as state_wide_exp_pac_rate,
null as state_wide_avg_total_cost,
null as state_wide_pac_percent,
null as state_wide_pac_rate,
null as state_wide_female_percent,
null as state_wide_male_percent,
null as state_wide_NU_percent,
null as state_wide_LU_percent,
null as state_wide_percent_co_ASTHMA,
null as state_wide_percent_co_ARRBLK,
null as state_wide_percent_co_HF,
null as state_wide_percent_co_COPD,
null as state_wide_percent_co_CAD,
null as state_wide_percent_co_ULCLTS,
null as state_wide_percent_co_BIPLR,
null as state_wide_percent_co_GERD,
null as state_wide_percent_co_HTN,
null as state_wide_percent_co_GLCOMA,
null as state_wide_percent_co_LBP,
null as state_wide_percent_co_CROHNS,
null as state_wide_percent_co_DIAB,
null as state_wide_percent_co_DEPRSN,
null as state_wide_percent_co_OSTEOA,
null as state_wide_percent_co_RHNTS,
null as state_wide_percent_co_DIVERT,
null as state_wide_percent_co_DEPANX,
null as state_wide_percent_co_PTSD,
null as state_wide_percent_co_SCHIZO,
null as state_wide_percent_co_SUDS,
null as ppr_expected_cost,
null as ppv_expected_cost
FROM
	visual_analysis_table_js r
WHERE
	r.analysis_type = 'Episode'
	AND r.episode_type='Chronic'
	AND r.episode_name not in ('RHNTS','DIVERT', 'ULCLTS', 'GLCOMA', 'SCHIZO', 'CROHNS','ADHD')
	AND r.episode_level = 5
GROUP BY
	r.member_id,
	r.member_population,
	r.member_region,
	r.subgroup,
	r.exclusive,
	r.Year
	;



		
/********* IPC bundle **********************/


insert /*+ direct */ INTO visual_analysis_table_js
SELECT
	'Episode' AS analysis_type,
	'IPC' || '-' || r.member_id || '-' || r.year AS id,
	 'IPC' AS episode_id,
	'IPC' AS episode_name,
	'Integrated Primary Care' AS episode_description,
	'Other' AS episode_type,
	'High relevance' AS episode_category,
	5 AS episode_level,
	r.member_id AS member_id,
	MAX(r.member_age) AS member_age,
	MAX(r.cms_age_group) AS cms_age_group,
	MAX(r.gender) AS gender,
	MAX(r.member_zipcode) AS member_zipcode,
	MAX(r.member_county) AS member_county,
	r.member_population AS member_population,
	r.member_region as member_region,
	sum(ifnull(r.Split_Total_Cost,0)) as Split_Total_Cost,
sum(ifnull(r.Split_Total_PAC_Cost,0)) as Split_Total_PAC_Cost,
sum(ifnull(r.Split_Total_Typical_Cost,0)) as Split_Total_Typical_Cost,
null as  Unsplit_Total_Cost,
null as  Unsplit_Total_PAC_Cost,
null as  Unsplit_Total_Typical_Cost,
sum(ifnull(r.Split_Expected_Total_Cost,0)) as Split_Expected_Total_Cost,
sum(ifnull(r.Split_Expected_Typical_IP_Cost,0)) as Split_Expected_Typical_IP_Cost,
sum(ifnull(r.Split_Expected_Typical_Other_Cost,0)) as Split_Expected_Typical_Other_Cost,
sum(ifnull(r.Split_Expected_PAC_Cost,0)) as Split_Expected_PAC_Cost,
null as  Unsplit_Expected_Total_Cost,
null as  Unsplit_Expected_Typical_IP_Cost,
null as  Unsplit_Expected_Typical_Other_Cost,
null as  Unsplit_Expected_PAC_Cost,
	sum(ifnull(r.ip_cost,0)) AS ip_cost,
	sum(ifnull(r.op_cost,0)) AS op_cost,
	sum(ifnull(r.pb_cost,0)) AS pb_cost,
	sum(ifnull(r.rx_cost,0)) AS rx_cost,
	sum(ifnull(r.assigned_cost,0)) AS assigned_cost,
	sum(ifnull(r.assigned_ip_cost,0)) AS assigned_ip_cost,
	sum(ifnull(r.assigned_op_cost,0)) AS assigned_op_cost,
	sum(ifnull(r.assigned_pb_cost,0)) AS assigned_pb_cost,
	sum(ifnull(r.assigned_rx_cost,0)) AS assigned_rx_cost,
	sum(ifnull(r.assigned_cost_unfiltered,0)) AS assigned_cost_unfiltered,
	sum(ifnull(r.assigned_ip_cost_unfiltered,0)) AS assigned_ip_cost_unfiltered,
	sum(ifnull(r.assigned_op_cost_unfiltered,0)) AS assigned_op_cost_unfiltered,
	sum(ifnull(r.assigned_pb_cost_unfiltered,0)) AS assigned_pb_cost_unfiltered,
	sum(ifnull(r.assigned_rx_cost_unfiltered,0)) AS assigned_rx_cost_unfiltered, 
	MAX(r.pps) AS pps,
null as Facility_ID,
null as Facility_ID_provider_name,
null as Facility_ID_provider_zipcode,
null as Facility_ID_provider_type,
null as Physician_ID,
null as Physician_ID_provider_name,
null as Physician_ID_provider_zipcode,
null as Physician_ID_provider_type,
	MAX(r.mco) AS mco,
	MAX(r.hh) AS hh,
	MAX(r.pcp) AS pcp,
	MAX(r.vbp_attrib_provider) AS vbp_attrib_provider,
	MAX(r.vbp_attrib_provider_zipcode) AS vbp_attrib_provider_zipcode,
	MAX(r.vbp_contractor) AS vbp_contractor,
	r.subgroup as subgroup,
	r.year AS year,
	MAX(r.utilization) AS utilization,
	MAX(r.ppr) AS ppr,
	MAX(r.ppv) AS ppv,
	r.exclusive AS exclusive,
	MAX(r.co_occurence_count_ASTHMA) AS co_occurence_count_ASTHMA,
	MAX(r.co_occurence_count_ARRBLK) AS co_occurence_count_ARRBLK,
	MAX(r.co_occurence_count_HF) AS co_occurence_count_HF,
	MAX(r.co_occurence_count_COPD) AS co_occurence_count_COPD,
	MAX(r.co_occurence_count_CAD) AS co_occurence_count_CAD,
	MAX(r.co_occurence_count_ULCLTS) AS co_occurence_count_ULCLTS,
	MAX(r.co_occurence_count_BIPLR) AS co_occurence_count_BIPLR,
	MAX(r.co_occurence_count_GERD) AS co_occurence_count_GERD,
	MAX(r.co_occurence_count_HTN) AS co_occurence_count_HTN,
	MAX(r.co_occurence_count_GLCOMA) AS co_occurence_count_GLCOMA,
	MAX(r.co_occurence_count_LBP) AS co_occurence_count_LBP,
	MAX(r.co_occurence_count_CROHNS) AS co_occurence_count_CROHNS,
	MAX(r.co_occurence_count_DIAB) AS co_occurence_count_DIAB,
	MAX(r.co_occurence_count_DEPRSN) AS co_occurence_count_DEPRSN,
	MAX(r.co_occurence_count_OSTEOA) AS co_occurence_count_OSTEOA,
	MAX(r.co_occurence_count_RHNTS) AS co_occurence_count_RHNTS,
	MAX(r.co_occurence_count_DIVERT) AS co_occurence_count_DIVERT,
	MAX(r.co_occurence_count_DEPANX) AS co_occurence_count_DEPANX,
	MAX(r.co_occurence_count_PTSD) AS co_occurence_count_PTSD,
	MAX(r.co_occurence_count_SCHIZO) AS co_occurence_count_SCHIZO,
	MAX(r.co_occurence_count_SUDS) AS co_occurence_count_SUDS,
	MAX(r.co_occurence_count_chronic) AS co_occurence_count_chronic,
	MAX(r.co_occurence_count_all) AS co_occurence_count_all,
	null as episode_count,
null as episode_count_unfiltered,
null as qcrg_code,
null as qcrg_desc,
null as qacrg1_code,
null as qacrg1_desc,
null as qacrg2_code,
null as qacrg2_desc,
null as qacrg3_code,
null as qacrg3_desc,
null as fincrg,
null as q_base,
null as q_severity,
max(r.enrolled_num_month) as enrolled_num_month,
'Integrated Primary Care' as vbp_arrangement,
null as state_wide_avg_exp_cost,
null as state_wide_exp_pac_rate,
null as state_wide_avg_total_cost,
null as state_wide_pac_percent,
null as state_wide_pac_rate,
null as state_wide_female_percent,
null as state_wide_male_percent,
null as state_wide_NU_percent,
null as state_wide_LU_percent,
null as state_wide_percent_co_ASTHMA,
null as state_wide_percent_co_ARRBLK,
null as state_wide_percent_co_HF,
null as state_wide_percent_co_COPD,
null as state_wide_percent_co_CAD,
null as state_wide_percent_co_ULCLTS,
null as state_wide_percent_co_BIPLR,
null as state_wide_percent_co_GERD,
null as state_wide_percent_co_HTN,
null as state_wide_percent_co_GLCOMA,
null as state_wide_percent_co_LBP,
null as state_wide_percent_co_CROHNS,
null as state_wide_percent_co_DIAB,
null as state_wide_percent_co_DEPRSN,
null as state_wide_percent_co_OSTEOA,
null as state_wide_percent_co_RHNTS,
null as state_wide_percent_co_DIVERT,
null as state_wide_percent_co_DEPANX,
null as state_wide_percent_co_PTSD,
null as state_wide_percent_co_SCHIZO,
null as state_wide_percent_co_SUDS,
null as ppr_expected_cost,
null as ppv_expected_cost
FROM
	visual_analysis_table_js r
WHERE
	r.analysis_type = 'Episode'
	AND r.episode_name in ('PREVNT', 'SICKCR','Chronic bundle' )
	AND  r.episode_level = 5 
GROUP BY
	r.member_id,
	r.member_population,
	r.member_region,
	r.subgroup,
	r.exclusive,
	r.Year
	;

 /*********  ADD MATERNITY BUNDLE **********************/

DROP TABLE IF EXISTS temp_main_state_wide_values_2;
CREATE TABLE temp_main_state_wide_values_2 as
SELECT
		analysis_type,
		vbp_arrangement,
		member_population,
		episode_id,
		year,
                      episode_level,
		AVG(Split_Total_Cost) as AVE_SPLIT_COSTS,
		AVG(Split_Total_PAC_Cost) as AVE_SPLIT_PAC_COSTS

	FROM
		visual_analysis_table_js		
		left join member 
		on member.member_id=visual_analysis_table_js.member_id
	WHERE
		episode_id in ('EX1401','EX1502') and episode_level=4 and member_county IS NOT NULL  and dual_eligible<>1
	GROUP BY
		analysis_type, vbp_arrangement, member_population, episode_id, year, episode_level
	;

drop table if exists MATERNITY_BUNDLE_COSTFIELDS_state;
create table MATERNITY_BUNDLE_COSTFIELDS_state as 
select 
MATERNITY_BUNDLE_COSTFIELDS.*,
max(case when temp_main_state_wide_values_2.episode_id='EX1401' then temp_main_state_wide_values_2.AVE_SPLIT_COSTS end) as state_ave_preg,
max(case when temp_main_state_wide_values_2.episode_id='EX1502' then temp_main_state_wide_values_2.AVE_SPLIT_COSTS end) as state_ave_nborn,
max(case when temp_main_state_wide_values_2.episode_id='EX1401' then temp_main_state_wide_values_2.AVE_SPLIT_PAC_COSTS end) as state_ave_preg_PAC,
max(case when temp_main_state_wide_values_2.episode_id='EX1502' then temp_main_state_wide_values_2.AVE_SPLIT_PAC_COSTS end) as state_ave_nborn_PAC

from MATERNITY_BUNDLE_COSTFIELDS
join visual_analysis_table_js r
on MATERNITY_BUNDLE_COSTFIELDS.ENCRYPT_RECIP_ID_MOM=r.member_id and MATERNITY_BUNDLE_COSTFIELDS.year=r.year
join temp_main_state_wide_values_2
on r.year=temp_main_state_wide_values_2.year 
  and r.analysis_type=temp_main_state_wide_values_2.analysis_type
  and  r.vbp_arrangement=temp_main_state_wide_values_2.vbp_arrangement 
  and  r.member_population=temp_main_state_wide_values_2.member_population
GROUP BY
		MATERNITY_BUNDLE_COSTFIELDS.ENCRYPT_RECIP_ID_MOM, MATERNITY_BUNDLE_COSTFIELDS.ENCRYPT_RECIP_ID_BABY,	MATERNITY_BUNDLE_COSTFIELDS.PREG_master_episode_id,	
		MATERNITY_BUNDLE_COSTFIELDS.trig_begin_date,	MATERNITY_BUNDLE_COSTFIELDS.year,	MATERNITY_BUNDLE_COSTFIELDS.SPLIT_TOTAL_PREGN_COST,	
		MATERNITY_BUNDLE_COSTFIELDS.SPLIT_TYPICAL_PREGN_COST,	MATERNITY_BUNDLE_COSTFIELDS.SPLIT_PAC_PREGN_COST,	MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_TOTAL_PREGN_COST,	
		MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_TYPICAL_PREGN_COST,	MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_PAC_PREGN_COST,	MATERNITY_BUNDLE_COSTFIELDS.DEL_master_episode_id,	
		MATERNITY_BUNDLE_COSTFIELDS.DELIVERY_TYPE,	MATERNITY_BUNDLE_COSTFIELDS.SPLIT_TOTAL_DEL_COST,	MATERNITY_BUNDLE_COSTFIELDS.SPLIT_TYPICAL_DEL_COST,	
		MATERNITY_BUNDLE_COSTFIELDS.SPLIT_PAC_DEL_COST,	MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_TOTAL_DEL_COST,	MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_TYPICAL_DEL_COST,	
		MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_PAC_DEL_COST,	MATERNITY_BUNDLE_COSTFIELDS.split_expected_del_total_cost,	MATERNITY_BUNDLE_COSTFIELDS.split_expected_del_typical_cost,	
		MATERNITY_BUNDLE_COSTFIELDS.split_expected_del_pac_cost,	MATERNITY_BUNDLE_COSTFIELDS.unsplit_expected_del_total_cost,	MATERNITY_BUNDLE_COSTFIELDS.unsplit_expected_del_typical_cost,	
		MATERNITY_BUNDLE_COSTFIELDS. unsplit_expected_del_pac_cost,	MATERNITY_BUNDLE_COSTFIELDS.NEWBORN_master_episode_id,	MATERNITY_BUNDLE_COSTFIELDS.SPLIT_TOTAL_NEWBORN_COST,	
		MATERNITY_BUNDLE_COSTFIELDS.SPLIT_TYPICAL_NEWBORN_COST,	MATERNITY_BUNDLE_COSTFIELDS.SPLIT_PAC_NEWBORN_COST,	MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_TOTAL_NEWBORN_COST,	
		MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_TYPICAL_NEWBORN_COST,	MATERNITY_BUNDLE_COSTFIELDS.UNSPLIT_PAC_NEWBORN_COST,	MATERNITY_BUNDLE_COSTFIELDS.NURSERY_LEVEL,
		r.analysis_type, r.vbp_arrangement, r.member_population, r.year
	;
	
	



insert /*+ direct */ INTO visual_analysis_table_js
SELECT
	'Episode' AS analysis_type,
	'MTRNTY' || '-' || MATERNITY_BUNDLE_COSTFIELDS_state.ENCRYPT_RECIP_ID_MOM || '-' ||  MATERNITY_BUNDLE_COSTFIELDS_state.ENCRYPT_RECIP_ID_BABY || '-' || r.year AS id,
	 'MTRNTY' AS episode_id,
	'MTRNTY' AS episode_name,
	'Maternity bundle' AS episode_description,
	'Other' AS episode_type,
	'High relevance' AS episode_category,
	5 AS episode_level,
	r.member_id AS member_id,
	MAX(r.member_age) AS member_age,
	MAX(r.cms_age_group) AS cms_age_group,
	MAX(r.gender) AS gender,
	MAX(r.member_zipcode) AS member_zipcode,
	MAX(r.member_county) AS member_county,
	r.member_population AS member_population,
	r.member_region as member_region,
	sum(MATERNITY_BUNDLE_COSTFIELDS_state.SPLIT_TOTAL_PREGN_COST + MATERNITY_BUNDLE_COSTFIELDS_state.SPLIT_TOTAL_DEL_COST + MATERNITY_BUNDLE_COSTFIELDS_state.SPLIT_TOTAL_NEWBORN_COST) as Split_Total_Cost,
sum(MATERNITY_BUNDLE_COSTFIELDS_state.SPLIT_PAC_PREGN_COST +  MATERNITY_BUNDLE_COSTFIELDS_state.SPLIT_PAC_DEL_COST  +  MATERNITY_BUNDLE_COSTFIELDS_state.SPLIT_PAC_NEWBORN_COST )as Split_Total_PAC_Cost,
sum(MATERNITY_BUNDLE_COSTFIELDS_state.SPLIT_TYPICAL_PREGN_COST +  MATERNITY_BUNDLE_COSTFIELDS_state.SPLIT_TYPICAL_DEL_COST + MATERNITY_BUNDLE_COSTFIELDS_state.SPLIT_TYPICAL_NEWBORN_COST) as Split_Total_Typical_Cost,
null as Unsplit_Total_Cost,
null  as Unsplit_Total_PAC_Cost,
null  as Unsplit_Total_Typical_Cost,
state_ave_preg+ split_expected_del_total_cost + state_ave_nborn as Split_Expected_Total_Cost,
null  as Split_Expected_Typical_IP_Cost,
(state_ave_preg+ split_expected_del_total_cost + state_ave_nborn)-(state_ave_preg_PAC+ split_expected_del_pac_cost + state_ave_nborn_PAC)  as Split_Expected_Typical_Other_Cost,
state_ave_preg_PAC+ split_expected_del_pac_cost +state_ave_nborn_PAC  as Split_Expected_PAC_Cost,
null  as Unsplit_Expected_Total_Cost,
null  as Unsplit_Expected_Typical_IP_Cost,
null  as Unsplit_Expected_Typical_Other_Cost,
null  as Unsplit_Expected_PAC_Cost,
	preg.ip_cost+del.ip_cost+newborn.ip_cost AS ip_cost,
	preg.op_cost+del.op_cost+newborn.op_cost AS op_cost,
	preg.pb_cost+del.pb_cost+newborn.pb_cost AS pb_cost,
	preg.rx_cost+del.rx_cost+newborn.rx_cost AS rx_cost,
	null AS assigned_cost,
	null AS assigned_ip_cost,
	null AS assigned_op_cost,
	null AS assigned_pb_cost,
	null AS assigned_rx_cost,
	null AS assigned_cost_unfiltered,
	null AS assigned_ip_cost_unfiltered,
	null AS assigned_op_cost_unfiltered,
	null AS assigned_pb_cost_unfiltered,
	null AS assigned_rx_cost_unfiltered, 
	MAX(r.pps) AS pps,
null as Facility_ID,
null as Facility_ID_provider_name,
null as Facility_ID_provider_zipcode,
null as Facility_ID_provider_type,
r.Physician_ID as Physician_ID,
r.Physician_ID_provider_name as Physician_ID_provider_name,
r.Physician_ID_provider_zipcode as Physician_ID_provider_zipcode,
r.Physician_ID_provider_type as Physician_ID_provider_type,
	MAX(r.mco) AS mco,
	MAX(r.hh) AS hh,
	MAX(r.pcp) AS pcp,
	MAX(r.vbp_attrib_provider) AS vbp_attrib_provider,
	MAX(r.vbp_attrib_provider_zipcode) AS vbp_attrib_provider_zipcode,
	MAX(r.vbp_contractor) AS vbp_contractor,
	r.subgroup as subgroup,
	r.year AS year,
	MAX(r.utilization) AS utilization,
	MAX(r.ppr) AS ppr,
	MAX(r.ppv) AS ppv,
	r.exclusive AS exclusive,
	MAX(r.co_occurence_count_ASTHMA) AS co_occurence_count_ASTHMA,
	MAX(r.co_occurence_count_ARRBLK) AS co_occurence_count_ARRBLK,
	MAX(r.co_occurence_count_HF) AS co_occurence_count_HF,
	MAX(r.co_occurence_count_COPD) AS co_occurence_count_COPD,
	MAX(r.co_occurence_count_CAD) AS co_occurence_count_CAD,
	MAX(r.co_occurence_count_ULCLTS) AS co_occurence_count_ULCLTS,
	MAX(r.co_occurence_count_BIPLR) AS co_occurence_count_BIPLR,
	MAX(r.co_occurence_count_GERD) AS co_occurence_count_GERD,
	MAX(r.co_occurence_count_HTN) AS co_occurence_count_HTN,
	MAX(r.co_occurence_count_GLCOMA) AS co_occurence_count_GLCOMA,
	MAX(r.co_occurence_count_LBP) AS co_occurence_count_LBP,
	MAX(r.co_occurence_count_CROHNS) AS co_occurence_count_CROHNS,
	MAX(r.co_occurence_count_DIAB) AS co_occurence_count_DIAB,
	MAX(r.co_occurence_count_DEPRSN) AS co_occurence_count_DEPRSN,
	MAX(r.co_occurence_count_OSTEOA) AS co_occurence_count_OSTEOA,
	MAX(r.co_occurence_count_RHNTS) AS co_occurence_count_RHNTS,
	MAX(r.co_occurence_count_DIVERT) AS co_occurence_count_DIVERT,
	MAX(r.co_occurence_count_DEPANX) AS co_occurence_count_DEPANX,
	MAX(r.co_occurence_count_PTSD) AS co_occurence_count_PTSD,
	MAX(r.co_occurence_count_SCHIZO) AS co_occurence_count_SCHIZO,
	MAX(r.co_occurence_count_SUDS) AS co_occurence_count_SUDS,
	MAX(r.co_occurence_count_chronic) AS co_occurence_count_chronic,
	MAX(r.co_occurence_count_all) AS co_occurence_count_all,
	null as episode_count,
null as episode_count_unfiltered,
null as qcrg_code,
null as qcrg_desc,
null as qacrg1_code,
null as qacrg1_desc,
null as qacrg2_code,
null as qacrg2_desc,
null as qacrg3_code,
null as qacrg3_desc,
null as fincrg,
null as q_base,
null as q_severity,
max(r.enrolled_num_month) as enrolled_num_month,
'Maternity Care'  as vbp_arrangement,
null as state_wide_avg_exp_cost,
null as state_wide_exp_pac_rate,
null as state_wide_avg_total_cost,
null as state_wide_pac_percent,
null as state_wide_pac_rate,
null as state_wide_female_percent,
null as state_wide_male_percent,
null as state_wide_NU_percent,
null as state_wide_LU_percent,
null as state_wide_percent_co_ASTHMA,
null as state_wide_percent_co_ARRBLK,
null as state_wide_percent_co_HF,
null as state_wide_percent_co_COPD,
null as state_wide_percent_co_CAD,
null as state_wide_percent_co_ULCLTS,
null as state_wide_percent_co_BIPLR,
null as state_wide_percent_co_GERD,
null as state_wide_percent_co_HTN,
null as state_wide_percent_co_GLCOMA,
null as state_wide_percent_co_LBP,
null as state_wide_percent_co_CROHNS,
null as state_wide_percent_co_DIAB,
null as state_wide_percent_co_DEPRSN,
null as state_wide_percent_co_OSTEOA,
null as state_wide_percent_co_RHNTS,
null as state_wide_percent_co_DIVERT,
null as state_wide_percent_co_DEPANX,
null as state_wide_percent_co_PTSD,
null as state_wide_percent_co_SCHIZO,
null as state_wide_percent_co_SUDS,
null as ppr_expected_cost,
null as ppv_expected_cost
FROM
	visual_analysis_table_js r
left join MATERNITY_BUNDLE_COSTFIELDS_state
on r.member_id=MATERNITY_BUNDLE_COSTFIELDS_state.ENCRYPT_RECIP_ID_MOM and r.year=MATERNITY_BUNDLE_COSTFIELDS_state.year
 join claim_type_costs preg
on preg.master_episode_id=MATERNITY_BUNDLE_COSTFIELDS_state.PREG_master_episode_id 
 join claim_type_costs del
on del.master_episode_id=MATERNITY_BUNDLE_COSTFIELDS_state.DEL_master_episode_id 
 join claim_type_costs newborn
on newborn.master_episode_id=MATERNITY_BUNDLE_COSTFIELDS_state.NEWBORN_master_episode_id 
WHERE
	r.analysis_type = 'Episode' 
	AND r.episode_id='EX1401' and r.episode_level=5 and preg.level=4 and preg.filter_id=1 and del.level=3 and del.filter_id=1 and newborn.level=1 and newborn.filter_id=1 
GROUP BY
	r.member_id, MATERNITY_BUNDLE_COSTFIELDS_state.ENCRYPT_RECIP_ID_MOM,MATERNITY_BUNDLE_COSTFIELDS_state.ENCRYPT_RECIP_ID_BABY,
	r.member_population,
	r.member_region,
	r.subgroup,
	r.exclusive,r.Physician_ID,r.Physician_ID_provider_name,r.Physician_ID_provider_zipcode,r.Physician_ID_provider_type,
	r.Year,MATERNITY_BUNDLE_COSTFIELDS_state.split_expected_del_total_cost, split_expected_del_pac_cost,
		preg.ip_cost,del.ip_cost,newborn.ip_cost,
	preg.op_cost,del.op_cost,newborn.op_cost,
	preg.pb_cost,del.pb_cost,newborn.pb_cost,
	preg.rx_cost,del.rx_cost,newborn.rx_cost, MATERNITY_BUNDLE_COSTFIELDS_state.state_ave_preg, MATERNITY_BUNDLE_COSTFIELDS_state.state_ave_preg_PAC, MATERNITY_BUNDLE_COSTFIELDS_state.state_ave_nborn, MATERNITY_BUNDLE_COSTFIELDS_state.state_ave_nborn_PAC
	;
	
update /*+ direct */  	visual_analysis_table_js r
set
Split_Expected_Total_Cost=  case when r.episode_id = 'EX1401' then state_ave_preg when r.episode_id='EX1502' then state_ave_nborn else r.Split_Expected_Total_Cost end ,
Split_Expected_Typical_Other_Cost=case when r.episode_id = 'EX1401' and state_ave_preg_PAC=state_ave_preg then 0
  when  r.episode_id = 'EX1401' and state_ave_preg_PAC<state_ave_preg then state_ave_preg-state_ave_preg_PAC
  when r.episode_id = 'EX1502' and state_ave_nborn_PAC=state_ave_nborn then 0
  when  r.episode_id = 'EX1502' and state_ave_nborn_PAC<state_ave_nborn then state_ave_nborn-state_ave_nborn_PAC else r.Split_Expected_Typical_Other_Cost end ,
Split_Expected_PAC_Cost=case when r.episode_id = 'EX1401' then state_ave_preg_PAC when r.episode_id='EX1502' then state_ave_nborn_PAC else r.Split_Expected_PAC_Cost end
from MATERNITY_BUNDLE_COSTFIELDS_state
where MATERNITY_BUNDLE_COSTFIELDS_state.preg_master_episode_id=r.id;

update /*+ direct */  	visual_analysis_table_js r
set
Split_Expected_Total_Cost=  case when r.episode_id = 'EX1401' then state_ave_preg when r.episode_id='EX1502' then state_ave_nborn else r.Split_Expected_Total_Cost end ,
Split_Expected_Typical_Other_Cost=case when r.episode_id = 'EX1401' and state_ave_preg_PAC=state_ave_preg then 0
  when  r.episode_id = 'EX1401' and state_ave_preg_PAC<state_ave_preg then state_ave_preg-state_ave_preg_PAC
  when r.episode_id = 'EX1502' and state_ave_nborn_PAC=state_ave_nborn then 0
  when  r.episode_id = 'EX1502' and state_ave_nborn_PAC<state_ave_nborn then state_ave_nborn-state_ave_nborn_PAC else r.Split_Expected_Typical_Other_Cost end ,
Split_Expected_PAC_Cost=case when r.episode_id = 'EX1401' then state_ave_preg_PAC when r.episode_id='EX1502' then state_ave_nborn_PAC else r.Split_Expected_PAC_Cost end
from MATERNITY_BUNDLE_COSTFIELDS_state
where MATERNITY_BUNDLE_COSTFIELDS_state.newborn_master_episode_id=r.id;
 /*********  ADD POPULATION **********************/
 -- Populate member level data




insert /*+ direct */ INTO visual_analysis_table_js
SELECT
	'Population' AS analysis_type,
	member_vistualization_claim_type.member_id || '-' || member_vistualization_claim_type.year AS id,
	 null AS episode_id,
	null AS episode_name,
	null AS episode_description,
	null AS episode_type,
	null AS episode_category,
	null AS episode_level,
	member_vistualization_claim_type.member_id AS member_id,
	member_vistualization_claim_type.age AS member_age,
	member_vistualization_claim_type.cms_age_group AS cms_age_group,
	member_vistualization_claim_type.gender AS gender,
	member_vistualization_claim_type.zip_code AS member_zipcode,
	member_vistualization_claim_type.county AS member_county,
	member_vistualization_claim_type.member_population AS member_population,
	member_vistualization_claim_type.mcregion as member_region,
null as Split_Total_Cost,
null as Split_Total_PAC_Cost,
null as Split_Total_Typical_Cost,
(ifnull(member_vistualization_claim_type.ip_cost,0) + ifnull(member_vistualization_claim_type.op_cost,0) + ifnull(member_vistualization_claim_type.pb_cost,0) + ifnull(member_vistualization_claim_type.rx_cost,0)) as Unsplit_Total_Cost,

(case when member_vistualization_claim_type.member_population in ('HIV','HARP & HIV') then ifnull(hivpac.pac_cost,0)+ ifnull(member_vistualization_claim_type.pac_cost,0)+ ifnull(rup.Split_Total_PAC_Cost,0) 
      when member_vistualization_claim_type.member_population not in ('HIV','HARP & HIV') then ifnull(member_vistualization_claim_type.pac_cost,0)+  ifnull(rup.Split_Total_PAC_Cost,0)  end )  as Unsplit_Total_PAC_Cost,

(ifnull(member_vistualization_claim_type.ip_cost,0) + ifnull(member_vistualization_claim_type.op_cost,0) + ifnull(member_vistualization_claim_type.pb_cost,0) + ifnull(member_vistualization_claim_type.rx_cost,0))
-(case when member_vistualization_claim_type.member_population in ('HIV','HARP & HIV') then ifnull(hivpac.pac_cost,0)+ ifnull(member_vistualization_claim_type.pac_cost,0)+ ifnull(rup.Split_Total_PAC_Cost,0) 
      when member_vistualization_claim_type.member_population not in ('HIV','HARP & HIV') then ifnull(member_vistualization_claim_type.pac_cost,0)+  ifnull(rup.Split_Total_PAC_Cost,0)  end )  as Unsplit_Total_Typical_Cost,
null as Split_Expected_Total_Cost,
null as Split_Expected_Typical_IP_Cost,
null as Split_Expected_Typical_Other_Cost,
null as Split_Expected_PAC_Cost,


(crg_cost_summary_by_member.expected_cost) as Unsplit_Expected_Total_Cost,
null as Unsplit_Expected_Typical_IP_Cost,


case when (sum(case when analysis_type = 'Episode' AND r.episode_name ='Chronic bundle'  AND  r.episode_level = 5 then r.Split_Expected_PAC_Cost else 0 end)+
sum(case when analysis_type = 'Episode' AND r.episode_name ='SICKCR'  AND  r.episode_level = 5 then r.Split_Total_PAC_Cost else 0 end))> (crg_cost_summary_by_member.expected_cost) then 0
else ((crg_cost_summary_by_member.expected_cost)-(sum(case when analysis_type = 'Episode' AND r.episode_name ='Chronic bundle'  AND  r.episode_level = 5 then r.Split_Expected_PAC_Cost else 0 end)+
sum(case when analysis_type = 'Episode' AND r.episode_name ='SICKCR'  AND  r.episode_level = 5 then r.Split_Total_PAC_Cost else 0 end) ) ) end  as Unsplit_Expected_Typical_Other_Cost,

case when (sum(case when analysis_type = 'Episode' AND r.episode_name ='Chronic bundle'  AND  r.episode_level = 5 then r.Split_Expected_PAC_Cost else 0 end)+
sum(case when analysis_type = 'Episode' AND r.episode_name ='SICKCR'  AND  r.episode_level = 5 then r.Split_Total_PAC_Cost else 0 end))> (crg_cost_summary_by_member.expected_cost) then (crg_cost_summary_by_member.expected_cost)
else (sum(case when analysis_type = 'Episode' AND r.episode_name ='Chronic bundle'  AND  r.episode_level = 5 then r.Split_Expected_PAC_Cost else 0 end)+
sum(case when analysis_type = 'Episode' AND r.episode_name ='SICKCR'  AND  r.episode_level = 5 then r.Split_Total_PAC_Cost else 0 end)) end as Unsplit_Expected_PAC_Cost,
	(member_vistualization_claim_type.ip_cost) AS ip_cost,
	(member_vistualization_claim_type.op_cost) AS op_cost,
	(member_vistualization_claim_type.pb_cost) AS pb_cost,
	(member_vistualization_claim_type.rx_cost) AS rx_cost,
	(member_vistualization_claim_type.assigned_cost) AS assigned_cost,
	(member_vistualization_claim_type.assigned_ip_cost) AS assigned_ip_cost,
	(member_vistualization_claim_type.assigned_op_cost) AS assigned_op_cost,
	(member_vistualization_claim_type.assigned_pb_cost) AS assigned_pb_cost,
	(member_vistualization_claim_type.assigned_rx_cost) AS assigned_rx_cost,
	(member_vistualization_claim_type.assigned_cost_unfiltered) AS assigned_cost_unfiltered,
	(member_vistualization_claim_type.assigned_ip_cost_unfiltered) AS assigned_ip_cost_unfiltered,
	(member_vistualization_claim_type.assigned_op_cost_unfiltered) AS assigned_op_cost_unfiltered,
	(member_vistualization_claim_type.assigned_pb_cost_unfiltered) AS assigned_pb_cost_unfiltered,
	(member_vistualization_claim_type.assigned_rx_cost_unfiltered) AS assigned_rx_cost_unfiltered, 
	member_vistualization_claim_type.pps AS pps,
null as Facility_ID,
null as Facility_ID_provider_name,
null as Facility_ID_provider_zipcode,
null as Facility_ID_provider_type,
null as Physician_ID,
null as Physician_ID_provider_name,
null as Physician_ID_provider_zipcode,
null as Physician_ID_provider_type,
	member_vistualization_claim_type.mco AS mco,
	member_vistualization_claim_type.hh AS hh,
	member_vistualization_claim_type.pcp AS pcp,
	MAX(r.vbp_attrib_provider) AS vbp_attrib_provider,
	MAX(r.vbp_attrib_provider_zipcode) AS vbp_attrib_provider_zipcode,
	MAX(r.vbp_contractor) AS vbp_contractor,
	r.subgroup as subgroup,
	member_vistualization_claim_type.year AS year,
case when (case when member_vistualization_claim_type.ip_cost is null then 0 else member_vistualization_claim_type.ip_cost end )+ 
     (case when member_vistualization_claim_type.op_cost is null then 0 else member_vistualization_claim_type.op_cost end )+ 
     (case when member_vistualization_claim_type.pb_cost is null then 0 else member_vistualization_claim_type.pb_cost end )+ 
     (case when member_vistualization_claim_type.rx_cost is null then 0 else member_vistualization_claim_type.rx_cost end )<200 then 'Low' else 'Non-Low' end  AS utilization,
	MAX(member_vistualization_claim_type.PPR) AS ppr,
	MAX(member_vistualization_claim_type.PPV) AS ppv,
	member_vistualization_claim_type.exclusive AS exclusive,
	case when sum(co_occurence_count_ASTHMA)>=1 then 1 else 0 end  AS co_occurence_count_ASTHMA,
		case when sum(co_occurence_count_ARRBLK)>=1 then 1 else 0 end    AS co_occurence_count_ARRBLK,
		case when sum(co_occurence_count_HF)>=1 then 1 else 0 end    AS co_occurence_count_HF,
		case when sum(co_occurence_count_COPD)>=1 then 1 else 0 end    AS co_occurence_count_COPD,
		case when sum(co_occurence_count_CAD)>=1 then 1 else 0 end    AS co_occurence_count_CAD,
		case when sum(co_occurence_count_ULCLTS)>=1 then 1 else 0 end    AS co_occurence_count_ULCLTS,
		case when sum(co_occurence_count_BIPLR)>=1 then 1 else 0 end    AS co_occurence_count_BIPLR,
		case when sum(co_occurence_count_GERD)>=1 then 1 else 0 end    AS co_occurence_count_GERD,
		case when sum(co_occurence_count_HTN)>=1 then 1 else 0 end    AS co_occurence_count_HTN,
		case when sum(co_occurence_count_GLCOMA)>=1 then 1 else 0 end    AS co_occurence_count_GLCOMA,
		case when sum(co_occurence_count_LBP)>=1 then 1 else 0 end    AS co_occurence_count_LBP,
		case when sum(co_occurence_count_CROHNS)>=1 then 1 else 0 end    AS co_occurence_count_CROHNS,
		case when sum(co_occurence_count_DIAB)>=1 then 1 else 0 end    AS co_occurence_count_DIAB,
		case when sum(co_occurence_count_DEPRSN)>=1 then 1 else 0 end    AS co_occurence_count_DEPRSN,
		case when sum(co_occurence_count_OSTEOA)>=1 then 1 else 0 end    AS co_occurence_count_OSTEOA,
		case when sum(co_occurence_count_RHNTS)>=1 then 1 else 0 end    AS co_occurence_count_RHNTS,
		case when sum(co_occurence_count_DIVERT)>=1 then 1 else 0 end    AS co_occurence_count_DIVERT,
		case when sum(co_occurence_count_DEPANX)>=1 then 1 else 0 end    AS co_occurence_count_DEPANX,
		case when sum(co_occurence_count_PTSD)>=1 then 1 else 0 end    AS co_occurence_count_PTSD,
		case when sum(co_occurence_count_SCHIZO)>=1 then 1 else 0 end    AS co_occurence_count_SCHIZO,
		case when sum(co_occurence_count_SUDS)>=1 then 1 else 0 end    AS co_occurence_count_SUDS,
		case when sum(co_occurence_count_chronic)>=1 then 1 else 0 end    AS co_occurence_count_chronic,
		case when sum(co_occurence_count_all)>=1 then 1 else 0 end    AS co_occurence_count_all,
	null as episode_count, /*NEED TO ADD*/
null as episode_count_unfiltered, /*NEED TO ADD*/
null as qcrg_code,
null as qcrg_desc,
null as qacrg1_code,
null as qacrg1_desc,
null as qacrg2_code,
null as qacrg2_desc,
null as qacrg3_code,
crg_cost_summary_by_member.qacrg3_desc as qacrg3_desc,
crg_cost_summary_by_member.fincrg as fincrg,
null as q_base,
null as q_severity,
max(r.enrolled_num_month) as enrolled_num_month,
case when member_vistualization_claim_type.member_population = 'HARP' then 'HARP'
     when member_vistualization_claim_type.member_population = 'HIV' then 'HIV'
     when member_vistualization_claim_type.member_population = 'General'then 'General'
     else null end
  as vbp_arrangement,
null as state_wide_avg_exp_cost,
null as state_wide_exp_pac_rate,
null as state_wide_avg_total_cost,
null as state_wide_pac_percent,
null as state_wide_pac_rate,
null as state_wide_female_percent,
null as state_wide_male_percent,
null as state_wide_NU_percent,
null as state_wide_LU_percent,
null as state_wide_percent_co_ASTHMA,
null as state_wide_percent_co_ARRBLK,
null as state_wide_percent_co_HF,
null as state_wide_percent_co_COPD,
null as state_wide_percent_co_CAD,
null as state_wide_percent_co_ULCLTS,
null as state_wide_percent_co_BIPLR,
null as state_wide_percent_co_GERD,
null as state_wide_percent_co_HTN,
null as state_wide_percent_co_GLCOMA,
null as state_wide_percent_co_LBP,
null as state_wide_percent_co_CROHNS,
null as state_wide_percent_co_DIAB,
null as state_wide_percent_co_DEPRSN,
null as state_wide_percent_co_OSTEOA,
null as state_wide_percent_co_RHNTS,
null as state_wide_percent_co_DIVERT,
null as state_wide_percent_co_DEPANX,
null as state_wide_percent_co_PTSD,
null as state_wide_percent_co_SCHIZO,
null as state_wide_percent_co_SUDS,
crg_cost_summary_by_member.ppr_expected_cost as ppr_expected_cost,
crg_cost_summary_by_member.ppv_expected_cost as ppv_expected_cost

from member_vistualization_claim_type
left join visual_analysis_table_js r
on r.member_id=member_vistualization_claim_type.member_id and r.year=member_vistualization_claim_type.year


left join trend_pac_costs rup
on rup.member_id=member_vistualization_claim_type.member_id and rup.year=member_vistualization_claim_type.year

 join crg_cost_summary_by_member
on r.member_id=crg_cost_summary_by_member.member_id and r.year=crg_cost_summary_by_member.year 
left join hiv_code_lvl_pac_totals hivpac
on r.member_id=hivpac.member_id and r.year=hivpac.year 
where r.enrolled_num_month = 12 
GROUP BY
member_vistualization_claim_type.member_id,
	member_vistualization_claim_type.age,
	member_vistualization_claim_type.cms_age_group ,
	member_vistualization_claim_type.gender , r.subgroup,
	member_vistualization_claim_type.zip_code ,
	member_vistualization_claim_type.county ,member_vistualization_claim_type.MCO, member_vistualization_claim_type.HH, member_vistualization_claim_type.PCP,
	member_vistualization_claim_type.member_population , member_vistualization_claim_type.PPS, member_vistualization_claim_type.exclusive,
	member_vistualization_claim_type.mcregion ,member_vistualization_claim_type.year,
	 member_vistualization_claim_type.ip_cost , member_vistualization_claim_type.op_cost , member_vistualization_claim_type.pb_cost, member_vistualization_claim_type.rx_cost,
member_vistualization_claim_type.op_cost,
member_vistualization_claim_type.pb_cost,
member_vistualization_claim_type.rx_cost,
member_vistualization_claim_type.assigned_cost,
member_vistualization_claim_type.assigned_ip_cost,
member_vistualization_claim_type.assigned_op_cost,
member_vistualization_claim_type.assigned_pb_cost,
member_vistualization_claim_type.assigned_rx_cost,
member_vistualization_claim_type.assigned_cost_unfiltered,
member_vistualization_claim_type.assigned_ip_cost_unfiltered,
member_vistualization_claim_type.assigned_op_cost_unfiltered,
member_vistualization_claim_type.assigned_pb_cost_unfiltered,
member_vistualization_claim_type.assigned_rx_cost_unfiltered,
member_vistualization_claim_type.pac_cost, rup.Split_Total_PAC_Cost, crg_cost_summary_by_member.expected_cost, crg_cost_summary_by_member.qacrg3_desc,
crg_cost_summary_by_member.fincrg, crg_cost_summary_by_member.ppv_expected_cost, crg_cost_summary_by_member.ppr_expected_cost, hivpac.pac_cost
	;
	

 /********* add population episode counts **********************/
drop table if exists epi_countsa;
 create table epi_countsa as
 select
 cast(episode.member_id as varchar(50)) as member_id,
 cast(extract(year from episode.episode_end_date) as numeric(4)) as year,
 cast(count(distinct(case when filter_id=0 then filtered_episodes.master_episode_id end)) as float) as episode_count,
  cast(count(distinct(case when filter_id=1 and filter_fail=0 then filtered_episodes.master_episode_id end)) as float) as filtered_episode_count
  from episode
  left join filtered_episodes
  on episode.master_episode_id=filtered_episodes.master_episode_id
   where episode_id in ('EC0401',
'EC0402',
'EC0508',
'EC0511',
'EC0518',
'EC0521',
'EC0601',
'EC0801',
'EC0802',
'EC1001',
'EC1903',
'EC1905',
'EC1909',
'EC1910','EX1401','EP1403','EP1404','EX1502','EX9901','ES9901')
  group by episode.member_id,
 extract(year from episode.episode_end_date)
;
  /********* add trend year population episode counts **********************/
 

 insert /*+ direct */ into epi_countsa
 select
 cast(episode.member_id as varchar(50)) as member_id,
 cast(extract(year from episode.episode_end_date) as numeric(4))-1 as year,
 cast(count(distinct(case when filter_id=0 then filtered_episodes.master_episode_id end)) as float) as episode_count,
  cast(count(distinct(case when filter_id=1 and filter_fail=0 then filtered_episodes.master_episode_id end)) as float) as filtered_episode_count
  from episode
  left join filtered_episodes
  on episode.master_episode_id=filtered_episodes.master_episode_id
WHERE   episode.episode_id in ('EC0401',
'EC0402',
'EC0508',
'EC0511',
'EC0518',
'EC0521',
'EC0601',
'EC0801',
'EC0802',
'EC1001',
'EC1903',
'EC1905',
'EC1909',
'EC1910','EX9901','ES9901') and trig_begin_date<
(SELECT TIMESTAMPADD('yyyy', -2, data_latest_begin_date) FROM run)

  group by episode.member_id,
 extract(year from episode.episode_end_date);
 
   /********* sum epi counts **********************/
   
   drop table if exists epi_counts;
    create table epi_counts as
    select
    member_id,
    year,
    sum(episode_count) as episode_count,
    sum(filtered_episode_count) as filtered_episode_count
    from epi_countsa
    group by member_id, year;
    
   
 update /*+ direct */ visual_analysis_table_js
 set 
episode_count=epi_counts.filtered_episode_count, 
episode_count_unfiltered=epi_counts.episode_count
from epi_counts
where visual_analysis_table_js.member_id=epi_counts.member_id and visual_analysis_table_js.year=epi_counts.year and visual_analysis_table_js.analysis_type='Population';
 

 

  /********* add statewide information **********************/



DROP TABLE IF EXISTS temp_main_state_wide_values_2;
CREATE TABLE temp_main_state_wide_values_2 as
SELECT
		analysis_type,
		vbp_arrangement,
		member_population,
		episode_name,
		year,
                      episode_level,
		CAST(SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) AS Float)/SUM(CASE WHEN gender in ('M','F') THEN 1 ELSE 0 END) as state_wide_female_percent,
		CAST(SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS Float)/SUM(CASE WHEN gender in ('M','F') THEN 1 ELSE 0 END) as state_wide_male_percent,
		CAST(SUM(CASE WHEN utilization = 'Low' THEN 1 ELSE 0 END) AS Float)/COUNT(distinct visual_analysis_table_js.member_id) as state_wide_LU_percent,
		CAST(SUM(CASE WHEN utilization = 'Non-Utilizers' THEN 1 ELSE 0 END) AS Float)/COUNT(distinct visual_analysis_table_js.member_id) as state_wide_NU_percent,
		SUM(CASE WHEN gender in ('M','F') THEN 1 ELSE 0 END) as state_wide_total,
		AVG(split_expected_total_cost) as state_wide_avg_split_exp_cost,
		AVG(unsplit_expected_total_cost) as state_wide_avg_unsplit_exp_cost,
		AVG(split_total_cost) as state_wide_avg_split_total_cost,
		AVG(unsplit_total_cost) as state_wide_avg_unsplit_total_cost,
		SUM(split_total_pac_cost)/SUM(split_total_cost) as split_state_wide_pac_percent,
		SUM(unsplit_total_pac_cost)/SUM(unsplit_total_cost) as unsplit_state_wide_pac_percent,

		AVG(CAST(co_occurence_count_ASTHMA AS Float)) as state_wide_percent_co_ASTHMA,
		AVG(CAST(co_occurence_count_ARRBLK AS Float)) as state_wide_percent_co_ARRBLK,
		AVG(CAST(co_occurence_count_HF AS Float)) as state_wide_percent_co_HF,
		AVG(CAST(co_occurence_count_COPD AS Float)) as state_wide_percent_co_COPD,
		AVG(CAST(co_occurence_count_CAD AS Float)) as state_wide_percent_co_CAD,
		AVG(CAST(co_occurence_count_ULCLTS AS Float)) as state_wide_percent_co_ULCLTS,
		AVG(CAST(co_occurence_count_BIPLR AS Float)) as state_wide_percent_co_BIPLR,
		AVG(CAST(co_occurence_count_GERD AS Float)) as state_wide_percent_co_GERD,
		AVG(CAST(co_occurence_count_HTN AS Float)) as state_wide_percent_co_HTN,
		AVG(CAST(co_occurence_count_GLCOMA AS Float)) as state_wide_percent_co_GLCOMA,
		AVG(CAST(co_occurence_count_LBP AS Float)) as state_wide_percent_co_LBP,
		AVG(CAST(co_occurence_count_CROHNS AS Float)) as state_wide_percent_co_CROHNS,
		AVG(CAST(co_occurence_count_DIAB AS Float)) as state_wide_percent_co_DIAB,
		AVG(CAST(co_occurence_count_DEPRSN AS Float)) as state_wide_percent_co_DEPRSN,
		AVG(CAST(co_occurence_count_OSTEOA AS Float)) as state_wide_percent_co_OSTEOA,
		AVG(CAST(co_occurence_count_RHNTS AS Float)) as state_wide_percent_co_RHNTS,
		AVG(CAST(co_occurence_count_DIVERT AS Float)) as state_wide_percent_co_DIVERT,
		AVG(CAST(co_occurence_count_DEPANX AS Float)) as state_wide_percent_co_DEPANX,
		AVG(CAST(co_occurence_count_PTSD AS Float)) as state_wide_percent_co_PTSD,
		AVG(CAST(co_occurence_count_SCHIZO AS Float)) as state_wide_percent_co_SCHIZO,
		AVG(CAST(co_occurence_count_SUDS AS Float))  as state_wide_percent_co_SUDS
	FROM
		visual_analysis_table_js
				left join member 
		on member.member_id=visual_analysis_table_js.member_id
	WHERE
		member_county IS NOT NULL and dual_eligible<>1
	GROUP BY
		analysis_type, vbp_arrangement, member_population, episode_name, year, episode_level
	;

alter table visual_analysis_table_js drop column state_wide_avg_exp_cost;
alter table visual_analysis_table_js drop column state_wide_exp_pac_rate;
alter table visual_analysis_table_js drop column state_wide_avg_total_cost;
alter table visual_analysis_table_js drop column state_wide_pac_percent;
alter table visual_analysis_table_js drop column state_wide_pac_rate;

  
  alter table visual_analysis_table_js add column state_wide_avg_split_exp_cost float;
    alter table visual_analysis_table_js add column state_wide_avg_unsplit_exp_cost float;
      alter table visual_analysis_table_js add column state_wide_avg_split_total_cost float;
        alter table visual_analysis_table_js add column state_wide_avg_unsplit_total_cost float;
          alter table visual_analysis_table_js add column split_state_wide_pac_percent float;
            alter table visual_analysis_table_js add column unsplit_state_wide_pac_percent float;
            
            
            update /*+ direct */ visual_analysis_table_js
            set
            state_wide_percent_co_ASTHMA=temp_main_state_wide_values_2.state_wide_percent_co_ASTHMA,
state_wide_percent_co_ARRBLK=temp_main_state_wide_values_2.state_wide_percent_co_ARRBLK,
state_wide_percent_co_HF=temp_main_state_wide_values_2.state_wide_percent_co_HF,
state_wide_percent_co_COPD=temp_main_state_wide_values_2.state_wide_percent_co_COPD,
state_wide_percent_co_CAD=temp_main_state_wide_values_2.state_wide_percent_co_CAD,
state_wide_percent_co_ULCLTS=temp_main_state_wide_values_2.state_wide_percent_co_ULCLTS,
state_wide_percent_co_BIPLR=temp_main_state_wide_values_2.state_wide_percent_co_BIPLR,
state_wide_percent_co_GERD=temp_main_state_wide_values_2.state_wide_percent_co_GERD,
state_wide_percent_co_HTN=temp_main_state_wide_values_2.state_wide_percent_co_HTN,
state_wide_percent_co_GLCOMA=temp_main_state_wide_values_2.state_wide_percent_co_GLCOMA,
state_wide_percent_co_LBP=temp_main_state_wide_values_2.state_wide_percent_co_LBP,
state_wide_percent_co_CROHNS=temp_main_state_wide_values_2.state_wide_percent_co_CROHNS,
state_wide_percent_co_DIAB=temp_main_state_wide_values_2.state_wide_percent_co_DIAB,
state_wide_percent_co_DEPRSN=temp_main_state_wide_values_2.state_wide_percent_co_DEPRSN,
state_wide_percent_co_OSTEOA=temp_main_state_wide_values_2.state_wide_percent_co_OSTEOA,
state_wide_percent_co_RHNTS=temp_main_state_wide_values_2.state_wide_percent_co_RHNTS,
state_wide_percent_co_DIVERT=temp_main_state_wide_values_2.state_wide_percent_co_DIVERT,
state_wide_percent_co_DEPANX=temp_main_state_wide_values_2.state_wide_percent_co_DEPANX,
state_wide_percent_co_PTSD=temp_main_state_wide_values_2.state_wide_percent_co_PTSD,
state_wide_percent_co_SCHIZO=temp_main_state_wide_values_2.state_wide_percent_co_SCHIZO,
state_wide_percent_co_SUDS=temp_main_state_wide_values_2.state_wide_percent_co_SUDS,
state_wide_female_percent=temp_main_state_wide_values_2.state_wide_female_percent,
state_wide_male_percent=temp_main_state_wide_values_2.state_wide_male_percent,
state_wide_NU_percent=temp_main_state_wide_values_2.state_wide_LU_percent,
state_wide_LU_percent=temp_main_state_wide_values_2.state_wide_NU_percent,
            state_wide_avg_split_exp_cost=temp_main_state_wide_values_2.state_wide_avg_split_exp_cost,
state_wide_avg_unsplit_exp_cost=temp_main_state_wide_values_2.state_wide_avg_unsplit_exp_cost,
state_wide_avg_split_total_cost=temp_main_state_wide_values_2.state_wide_avg_split_total_cost,
state_wide_avg_unsplit_total_cost=temp_main_state_wide_values_2.state_wide_avg_unsplit_total_cost,
split_state_wide_pac_percent=temp_main_state_wide_values_2.split_state_wide_pac_percent,
unsplit_state_wide_pac_percent=temp_main_state_wide_values_2.unsplit_state_wide_pac_percent
            from temp_main_state_wide_values_2
            where visual_analysis_table_js.analysis_type<=>temp_main_state_wide_values_2.analysis_type 
            and visual_analysis_table_js.vbp_arrangement<=>temp_main_state_wide_values_2.vbp_arrangement 
            and visual_analysis_table_js.member_population<=> temp_main_state_wide_values_2.member_population 
            and visual_analysis_table_js.episode_name<=> temp_main_state_wide_values_2.episode_name 
            and visual_analysis_table_js.year<=> temp_main_state_wide_values_2.year
           and visual_analysis_table_js.episode_level <=> temp_main_state_wide_values_2.episode_level;
            

 /********* VBP contractor attribution START **********************/
/* HIV, MLTC, DD */
update /*+ direct */ visual_analysis_table_js
SET
	vbp_contractor = cast(vbp.vbp_contractor as varchar(200)),
	vbp_attrib_provider = cast(visual_analysis_table_js.pcp as varchar(200)),
	vbp_attrib_provider_zipcode = p.provider_zipcode
FROM
	pcp_table pcp
LEFT JOIN
	vbp_contractor_provider_npi vbp ON pcp.npi = vbp.provider_npi
LEFT JOIN
	provider_prep p ON pcp.npi = p.provider_npi
WHERE

	visual_analysis_table_js.member_population in ('HIV', 'MLTC', 'DD','HARP' , 'HARP & HIV')
	AND visual_analysis_table_js.member_id = pcp.member_id

;



/* General population (applied only to Population type analysis) */

update /*+ direct */ visual_analysis_table_js
SET
	vbp_contractor = cast(vbp.vbp_contractor as varchar(200)),
	vbp_attrib_provider = cast(visual_analysis_table_js.pcp as varchar(200)),
	vbp_attrib_provider_zipcode = p.provider_zipcode

FROM
	pcp_table pcp
LEFT JOIN
	vbp_contractor_provider_npi vbp ON pcp.npi = vbp.provider_npi
LEFT JOIN
	provider_prep p ON pcp.npi = p.provider_npi
WHERE
	visual_analysis_table_js.member_population = 'General' AND visual_analysis_table_js.analysis_type = 'Population'
	AND visual_analysis_table_js.member_id = pcp.member_id

;


/* General population (applied only to Episode type analysis) */


update /*+ direct */ visual_analysis_table_js
SET
	vbp_contractor = cast(vbp.vbp_contractor as varchar(200)),
	vbp_attrib_provider = cast(visual_analysis_table_js.pcp as varchar(200)),
	vbp_attrib_provider_zipcode = p.provider_zipcode
FROM
	pcp_table pcp
LEFT JOIN
	vbp_contractor_provider_npi vbp ON pcp.npi = vbp.provider_npi
LEFT JOIN
	provider_prep p ON pcp.npi = p.provider_npi
WHERE
	visual_analysis_table_js.member_population = 'General' AND visual_analysis_table_js.analysis_type = 'Episode'
	AND (visual_analysis_table_js.episode_type IN ('Chronic','Other', 'System Related Failure') OR visual_analysis_table_js.episode_name IN ('URI','TONSIL'))
	AND visual_analysis_table_js.episode_name NOT IN ('MTRNTY', 'PREGN', 'NBORN', 'CSECT', 'VAGDEL')
	AND visual_analysis_table_js.member_id = pcp.member_id

;
/*START HERE*/


drop table if exists visual_analysis_table_js_subset;

create table visual_analysis_table_js_subset as
select visual_analysis_table_js.* 
from  visual_analysis_table_js
left join member
on visual_analysis_table_js.member_id=member.member_id
WHERE
		analysis_type = 'Episode' AND member_population = 'General' AND
		episode_name IN ('PREVNT', 'SICKCR',  'IPC') 
		AND episode_level = 5 
		and dual_eligible<>1

	
union	

select visual_analysis_table_js.*
from  visual_analysis_table_js
left join member
on visual_analysis_table_js.member_id=member.member_id
	WHERE
		analysis_type = 'Episode' AND member_population = 'General' AND
		episode_name IN ('PREGN','CSECT', 'VAGDEL', 'NBORN','MTRNTY')
		AND (CASE WHEN episode_name IN ('CSECT', 'VAGDEL') THEN episode_level = 3
			  WHEN episode_name = 'PREGN' THEN episode_level = 4
			  ELSE episode_level = 5 END)
			  		and dual_eligible<>1
			  
union

select visual_analysis_table_js.* from visual_analysis_table_js
left join member
on visual_analysis_table_js.member_id=member.member_id
	WHERE
		analysis_type = 'Population' AND member_population = 'HARP'	 		and dual_eligible<>1		  

union

select visual_analysis_table_js.* from visual_analysis_table_js
left join member
on visual_analysis_table_js.member_id=member.member_id
	WHERE
		analysis_type = 'Population' AND member_population = 'HIV' 		and dual_eligible<>1

union
select visual_analysis_table_js.* from visual_analysis_table_js
left join member
on visual_analysis_table_js.member_id=member.member_id
	WHERE
		analysis_type = 'Population' AND member_population = 'General' 		and dual_eligible<>1
union
select visual_analysis_table_js.* from visual_analysis_table_js	
left join member
on visual_analysis_table_js.member_id=member.member_id	
	WHERE
		analysis_type = 'Episode' AND member_population = 'General' AND episode_type = 'Chronic'
	    	AND episode_name NOT IN ('RHNTS', 'DIVERT', 'ULCLTS', 'GLCOMA', 'SCHIZO', 'CROHNS','ADHD')
	    	AND episode_level = 5
	    			and dual_eligible<>1
	;

/* CRG update /*+ direct */ */
update /*+ direct */ visual_analysis_table_js_subset SET
	qcrg_code = crg.qcrg_code,
   	qcrg_desc = crg.qcrg_desc,
	qacrg1_code = crg.qacrg1_code,
   	qacrg1_desc = crg.qacrg1_desc,
  	qacrg2_code = crg.qacrg2_code,
   	qacrg2_desc = crg.qacrg2_desc,
   	qacrg3_code = crg.qacrg3_code,
   	qacrg3_desc = crg.qacrg3_desc,
   	fincrg = crg.fincrg,
   	q_base = crg.q_base,
   	q_severity = crg.q_severity
FROM
	crg
WHERE
	visual_analysis_table_js_subset.year = crg.year AND visual_analysis_table_js_subset.member_id = crg.recip_id
;

update /*+ direct */ visual_analysis_table_js_subset
set 
Physician_ID_provider_name=case when Physician_ID_provider_name is null then  p1.provider_name  else Physician_ID_provider_name end,
Physician_ID_provider_zipcode=case when Physician_ID_provider_zipcode is null then  p1.provider_zipcode  else Physician_ID_provider_zipcode end,
Physician_ID_provider_type=case when Physician_ID_provider_type is null then  p1.provider_type  else Physician_ID_provider_type end
from provider_prep p1
where visual_analysis_table_js_subset.physician_id=p1.provider_npi;


update /*+ direct */ visual_analysis_table_js_subset
set 
Facility_ID=null,
Facility_ID_provider_name=null,
Facility_ID_provider_zipcode=null,
Facility_ID_provider_type=null
from provider_prep p1
where visual_analysis_table_js_subset.physician_id=p1.provider_npi;


drop table if exists visual_analysis_table_va;
create table visual_analysis_table_va as
select 'Proxy Pricing' as Data_Processing, 
visual_analysis_table_js_subset.* 
from visual_analysis_table_js_subset;

/* take out after tables are fixed so county matches in zip to count and count to mc region table*/

update /*+ direct */ visual_analysis_table_va
set
member_region=case when member_county='New York (Manhattan)' then 'New York City'
 when member_county='Richmond (Staten Island)' then 'New York City'
  when member_county='Kings (Brooklyn)' then 'New York City'
   when member_county='Saint Lawrence' then 'Utica-Adirondack' else visual_analysis_table_va.member_region end;

Grant select on visual_analysis_table_va to epbuilder;



drop table if exists provider_ids_for_salient;
create table provider_ids_for_salient as
select distinct
va.member_id, 
va.id,
va.year,
va.Physician_ID,
HH_table.hh,
HH_table.mmis as hh_mmis,
HH_table.npi as hh_npi,
pcp.npi as pcp_npi,
pcp.mmis as pcp_mmis,
vbp.provider_npi as vbp_contractor_npi,
vbp.vbp_contractor,
va.Physician_ID_provider_type as Physician_ID_provider_type,
va.PPS,
PPS_table.PPS_ID,
MCO_table.mco,
MCO_table.mmis as MCO_table_mmis,
MCO_table.npi as MCO_table_npi
from visual_analysis_table_va va
left join  HH_table on va.member_id=HH_table.member_id
LEFT JOIN  pcp_table pcp on pcp.member_id=va.member_id
LEFT JOIN  vbp_contractor_provider_npi vbp ON pcp.npi = vbp.provider_npi
left join MCO_table on va.member_id=MCO_table.member_id
left join pps_table on va.PPS=pps_table.PPS_name and va.member_id=PPS_table.MBR_ID
;


Grant select on provider_ids_for_salient to epbuilder ;


grant select on all tables in schema epbuilder to epbuilder_read_only_role;
grant select on all tables in schema epbuilder_proxy to epbuilder_read_only_role;
grant select on all tables in schema epbuilder_real to epbuilder_read_only_role;
