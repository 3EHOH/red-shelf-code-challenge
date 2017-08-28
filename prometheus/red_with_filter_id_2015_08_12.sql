DROP TABLE IF EXISTS report_episode_detail;

CREATE TABLE `report_episode_detail` (
  Filter_ID int(11) DEFAULT NULL,
  `Member_ID` varchar(50) DEFAULT NULL,
  `Master_Episode_ID` varchar(255) DEFAULT NULL,
  `Episode_ID` varchar(6) DEFAULT NULL,
  `Episode_Name` varchar(6) DEFAULT NULL,
  `Episode_Description` varchar(255) DEFAULT NULL,
  `Episode_Type` varchar(50) DEFAULT NULL,
  `MDC` int(2) DEFAULT NULL,
  `MDC_Description` varchar(255) DEFAULT NULL,
  `Episode_Begin_Date` varchar(10) DEFAULT NULL,
  `Episode_End_Date` varchar(10) DEFAULT NULL,
  `Episode_Length` int(6) DEFAULT NULL,
  `Level` int(1) DEFAULT NULL,
  `Split_Total_Cost` decimal(13,2) DEFAULT NULL,
  `Split_1stPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Split_99thPercentile_Cost` decimal(13,2) DEFAULT NULL,
    `Split_80thPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Split_Total_PAC_Cost` decimal(13,2) DEFAULT NULL,
  `Split_Total_Typical_Cost` decimal(13,2) DEFAULT NULL,
  `Split_Total_TypicalwPAC_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_Total_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_1stPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_99thPercentile_Cost` decimal(13,2) DEFAULT NULL,
    `Annualized_Split_80thPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_Total_PAC_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_Total_Typical_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_Total_TypicalwPAC_Cost` decimal(13,2) DEFAULT NULL,
  `Unsplit_Total_Cost` decimal(13,2) DEFAULT NULL,
  `Unsplit_1stPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Unsplit_99thPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Unsplit_Total_PAC_Cost` decimal(13,2) DEFAULT NULL,
  `Unsplit_Total_Typical_Cost` decimal(13,2) DEFAULT NULL,
  `Unsplit_Total_TypicalwPAC_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_Total_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_1stPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_99thPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_Total_PAC_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_Total_Typical_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_Total_TypicalwPAC_Cost` decimal(13,2) DEFAULT NULL,
  `Facility_ID` varchar(50) DEFAULT NULL,
  `Physician_ID` varchar(50) DEFAULT NULL,
  `Physician_Specialty` varchar(2) DEFAULT NULL,
  Split_Expected_Total_Cost decimal(13,2) DEFAULT NULL,
  Split_Expected_Typical_IP_Cost decimal(13,2) DEFAULT NULL,
  Split_Expected_Typical_Other_Cost decimal(13,2) DEFAULT NULL,
  Split_Expected_PAC_Cost decimal(13,2) DEFAULT NULL,
  Unsplit_Expected_Total_Cost decimal(13,2) DEFAULT NULL,
  Unsplit_Expected_Typical_IP_Cost decimal(13,2) DEFAULT NULL,
  Unsplit_Expected_Typical_Other_Cost decimal(13,2) DEFAULT NULL,
  Unsplit_Expected_PAC_Cost decimal(13,2) DEFAULT NULL,
  IP_PAC_Count decimal(13,2) DEFAULT NULL,
  OP_PAC_Count decimal(13,2) DEFAULT NULL,
  PB_PAC_Count decimal(13,2) DEFAULT NULL,
  RX_PAC_Count decimal(13,2) DEFAULT NULL,
  KEY `episode_id` (`Episode_ID`),
  KEY `Master_Episode_ID` (`Master_Episode_ID`)
) ;

insert into report_episode_detail
(Filter_ID, Member_ID, Master_Episode_ID, Episode_ID, Episode_Name, Episode_Description, Episode_Type, MDC, MDC_Description, Episode_Begin_Date, Episode_End_Date, Episode_Length, Level, Split_Total_Cost, Split_Total_PAC_Cost, Split_Total_Typical_Cost, Split_Total_TypicalwPAC_Cost, Facility_ID, Physician_ID)

SELECT
master_epid_level.filter_id,
episode.member_id,
episode.master_episode_id,
episode.episode_id,
build_episode_reference.NAME,
build_episode_reference.DESCRIPTION,
build_episode_reference.TYPE,
build_episode_reference.MDC_CATEGORY,
mdc_desc.mdc_description,
episode.episode_begin_date,
episode.episode_end_date,
episode.episode_length_days,
master_epid_level.level,
master_epid_level.cost,
master_epid_level.cost_c,
master_epid_level.cost_t,
master_epid_level.cost_tc,
episode.attrib_default_facility,
episode.attrib_default_physician
#provider.specialty_id

FROM episode, build_episode_reference, master_epid_level, mdc_desc #provider
WHERE episode.episode_id=build_episode_reference.episode_id
AND episode.master_episode_id=master_epid_level.master_episode_id
AND build_episode_reference.MDC_CATEGORY=mdc_desc.mdc
AND master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0
#AND episode.attrib_default_physician=provider.provider_id
#AND provider.specialty_postion=1
;

CREATE TEMPORARY TABLE IF NOT EXISTS melDet 
(
master_episode_id varchar(73),
level tinyint(4),
cost decimal(15,4),
cost_c decimal(15,4),
cost_t decimal(15,4),
cost_tc decimal(15,4),
index(master_episode_id),
index(level)
)

AS	
SELECT
mel.filter_id,
mel.master_episode_id,
mel.level,
mel.cost,
mel.cost_c,
mel.cost_t,
mel.cost_tc
FROM master_epid_level mel
WHERE mel.claim_type='CL'
AND mel.split=0
AND mel.annualized=0;

UPDATE report_episode_detail red
JOIN melDet mel ON mel.master_episode_id=red.Master_Episode_ID
AND mel.level=red.Level
and  mel.filter_id=red.filter_id
SET 
red.Unsplit_Total_Cost=mel.cost, 
red.Unsplit_Total_PAC_Cost=mel.cost_c, 
red.Unsplit_Total_Typical_Cost=mel.cost_t, 
red.Unsplit_Total_TypicalwPAC_Cost=mel.cost_tc;

DROP TABLE melDet;

CREATE TEMPORARY TABLE IF NOT EXISTS melDet AS	
SELECT
mel.filter_id,
mel.master_episode_id,
mel.level,
mel.cost,
mel.cost_c,
mel.cost_t,
mel.cost_tc
FROM master_epid_level mel
WHERE mel.claim_type='CL'
AND mel.split=1
AND mel.annualized=1;

UPDATE report_episode_detail red
JOIN melDet mel ON mel.master_episode_id=red.Master_Episode_ID
AND mel.level=red.Level
and  mel.filter_id=red.filter_id
SET 
red.Annualized_Split_Total_Cost=mel.cost, 
red.Annualized_Split_Total_PAC_Cost=mel.cost_c, 
red.Annualized_Split_Total_Typical_Cost=mel.cost_t, 
red.Annualized_Split_Total_TypicalwPAC_Cost=mel.cost_tc;

DROP TABLE melDet;


CREATE TEMPORARY TABLE IF NOT EXISTS melDet AS	
SELECT
mel.filter_id,
mel.master_episode_id,
mel.level,
mel.cost,
mel.cost_c,
mel.cost_t,
mel.cost_tc
FROM master_epid_level mel
WHERE mel.claim_type='CL'
AND mel.split=0
AND mel.annualized=1;

UPDATE report_episode_detail red
JOIN melDet mel ON mel.master_episode_id=red.Master_Episode_ID
AND mel.level=red.Level
and  mel.filter_id=red.filter_id
SET 
red.Annualized_Unsplit_Total_Cost=mel.cost, 
red.Annualized_Unsplit_Total_PAC_Cost=mel.cost_c, 
red.Annualized_Unsplit_Total_Typical_Cost=mel.cost_t, 
red.Annualized_Unsplit_Total_TypicalwPAC_Cost=mel.cost_tc;

DROP TABLE melDet;
					
				
DROP TABLE IF EXISTS percentiles;	
					
CREATE TABLE `percentiles` (
  Filter_ID int(11) DEFAULT NULL,
  `Episode_ID` varchar(6) DEFAULT NULL,
  `Level` int(1) DEFAULT NULL,
  `Split_1stPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Split_99thPercentile_Cost` decimal(13,2) DEFAULT NULL,
    `Split_80thPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_1stPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_99thPercentile_Cost` decimal(13,2) DEFAULT NULL,
    `Annualized_Split_80thPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Unsplit_1stPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Unsplit_99thPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_1stPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_99thPercentile_Cost` decimal(13,2) DEFAULT NULL,
  KEY `Episode_ID` (`Episode_ID`),
  KEY `Level` (`Level`)
) ;
  
  
  set group_concat_max_len = 10485760;

insert into percentiles
(Filter_id, Episode_ID, Level, Split_1stPercentile_Cost, Split_99thPercentile_Cost, Split_80thPercentile_Cost, Unsplit_1stPercentile_Cost, Unsplit_99thPercentile_Cost)

select
red.Filter_id,
red.episode_id,
red.level,
cast(substring_index(substring_index(
	group_concat(red.split_total_cost order by red.split_total_cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'Split_1stPercentile_Cost',
cast(substring_index(substring_index(
	group_concat(red.split_total_cost order by red.split_total_cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'Split_99thPercentile_Cost',
	
cast(substring_index(substring_index(
	group_concat(red.split_total_cost order by red.split_total_cost separator ','),
	',', 80/100 * count(*) +1), ',', -1) as decimal) as 'Split_80thPercentile_Cost',

	
cast(substring_index(substring_index(
	group_concat(red.unsplit_total_cost order by red.unsplit_total_cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'Unsplit_1stPercentile_Cost',
cast(substring_index(substring_index(
	group_concat(red.unsplit_total_cost order by red.unsplit_total_cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'Unsplit_99thPercentile_Cost'

from report_episode_detail red
where red.level=1
and red.split_total_cost>0
group by red.Filter_id, red.episode_id ;
	
	
	
	
  set group_concat_max_len = 10485760;

insert into percentiles
(Filter_id, Episode_ID, Level, Split_1stPercentile_Cost, Split_99thPercentile_Cost, Split_80thPercentile_Cost, Unsplit_1stPercentile_Cost, Unsplit_99thPercentile_Cost)

select
red.Filter_id,
red.episode_id,
red.level,
cast(substring_index(substring_index(
	group_concat(red.split_total_cost order by red.split_total_cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'Split_1stPercentile_Cost',
cast(substring_index(substring_index(
	group_concat(red.split_total_cost order by red.split_total_cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'Split_99thPercentile_Cost',
	
cast(substring_index(substring_index(
	group_concat(red.split_total_cost order by red.split_total_cost separator ','),
	',', 80/100 * count(*) +1), ',', -1) as decimal) as 'Split_80thPercentile_Cost',

	
cast(substring_index(substring_index(
	group_concat(red.unsplit_total_cost order by red.unsplit_total_cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'Unsplit_1stPercentile_Cost',
cast(substring_index(substring_index(
	group_concat(red.unsplit_total_cost order by red.unsplit_total_cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'Unsplit_99thPercentile_Cost'

from report_episode_detail red
where red.level=2
and red.split_total_cost>0
group by red.Filter_id, red.episode_id ;




  set group_concat_max_len = 10485760;

insert into percentiles
(Filter_id, Episode_ID, Level, Split_1stPercentile_Cost, Split_99thPercentile_Cost, Split_80thPercentile_Cost, Unsplit_1stPercentile_Cost, Unsplit_99thPercentile_Cost)

select
red.Filter_id,
red.episode_id,
red.level,
cast(substring_index(substring_index(
	group_concat(red.split_total_cost order by red.split_total_cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'Split_1stPercentile_Cost',
cast(substring_index(substring_index(
	group_concat(red.split_total_cost order by red.split_total_cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'Split_99thPercentile_Cost',
	
cast(substring_index(substring_index(
	group_concat(red.split_total_cost order by red.split_total_cost separator ','),
	',', 80/100 * count(*) +1), ',', -1) as decimal) as 'Split_80thPercentile_Cost',

	
cast(substring_index(substring_index(
	group_concat(red.unsplit_total_cost order by red.unsplit_total_cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'Unsplit_1stPercentile_Cost',
cast(substring_index(substring_index(
	group_concat(red.unsplit_total_cost order by red.unsplit_total_cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'Unsplit_99thPercentile_Cost'

from report_episode_detail red
where red.level=3
and red.split_total_cost>0
group by red.Filter_id, red.episode_id ;
	
	
	
	
  set group_concat_max_len = 10485760;

insert into percentiles
(Filter_id, Episode_ID, Level, Split_1stPercentile_Cost, Split_99thPercentile_Cost, Split_80thPercentile_Cost, Unsplit_1stPercentile_Cost, Unsplit_99thPercentile_Cost)

select
red.Filter_id,
red.episode_id,
red.level,
cast(substring_index(substring_index(
	group_concat(red.split_total_cost order by red.split_total_cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'Split_1stPercentile_Cost',
cast(substring_index(substring_index(
	group_concat(red.split_total_cost order by red.split_total_cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'Split_99thPercentile_Cost',
	
cast(substring_index(substring_index(
	group_concat(red.split_total_cost order by red.split_total_cost separator ','),
	',', 80/100 * count(*) +1), ',', -1) as decimal) as 'Split_80thPercentile_Cost',

	
cast(substring_index(substring_index(
	group_concat(red.unsplit_total_cost order by red.unsplit_total_cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'Unsplit_1stPercentile_Cost',
cast(substring_index(substring_index(
	group_concat(red.unsplit_total_cost order by red.unsplit_total_cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'Unsplit_99thPercentile_Cost'

from report_episode_detail red
where red.level=4
and red.split_total_cost>0
group by red.Filter_id, red.episode_id ;
	
	
  set group_concat_max_len = 10485760;

insert into percentiles
(Filter_id, Episode_ID, Level, Split_1stPercentile_Cost, Split_99thPercentile_Cost, Split_80thPercentile_Cost, Unsplit_1stPercentile_Cost, Unsplit_99thPercentile_Cost)

select
red.Filter_id,
red.episode_id,
red.level,
cast(substring_index(substring_index(
	group_concat(red.split_total_cost order by red.split_total_cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'Split_1stPercentile_Cost',
cast(substring_index(substring_index(
	group_concat(red.split_total_cost order by red.split_total_cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'Split_99thPercentile_Cost',
	
cast(substring_index(substring_index(
	group_concat(red.split_total_cost order by red.split_total_cost separator ','),
	',', 80/100 * count(*) +1), ',', -1) as decimal) as 'Split_80thPercentile_Cost',

	
cast(substring_index(substring_index(
	group_concat(red.unsplit_total_cost order by red.unsplit_total_cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'Unsplit_1stPercentile_Cost',
cast(substring_index(substring_index(
	group_concat(red.unsplit_total_cost order by red.unsplit_total_cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'Unsplit_99thPercentile_Cost'

from report_episode_detail red
where red.level=5
and red.split_total_cost>0
group by red.Filter_id, red.episode_id ;



set group_concat_max_len = 10485760;

CREATE TEMPORARY TABLE IF NOT EXISTS ann_pct
(
  Filter_ID int(11) DEFAULT NULL,
episode_id varchar(6),
level tinyint(4),
ann_split_1st decimal(15,4),
ann_split_99th decimal(15,4),
ann_split_80th decimal(15,4),
annd_unsplit_1st decimal(15,4),
ann_unsplit_99th decimal(15,4),
index(episode_id),
index(level)
)

AS	
SELECT
red.Filter_id,
red.episode_id,red.level,cast(substring_index(substring_index(	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_1st',cast(substring_index(substring_index(	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_99th',
cast(substring_index(substring_index(	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),	',', 80/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_80th',cast(substring_index(substring_index(	group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_1st',cast(substring_index(substring_index(	group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_99th'from report_episode_detail redwhere red.level=1
and red.annualized_split_total_cost>0and red.episode_id IN ('EC0202', 'EC0301', 'EC0401', 'EC0402', 'EC0508', 'EC0510', 'EC0511', 'EC0518', 'EC0601', 'EC0801', 'EC0802', 'EC1001', 'EC1902', 'EC1903', 'EX9901') 
group by red.Filter_id, red.episode_id ;

UPDATE percentiles pt
JOIN ann_pct ap ON ap.episode_id=pt.Episode_ID
AND ap.level=pt.Level
and ap.Filter_id=pt.filter_id
SET 
pt.Annualized_Split_1stPercentile_Cost=ap.ann_split_1st, 
pt.Annualized_Split_99thPercentile_Cost=ap.ann_split_99th, 
pt.Annualized_Split_80thPercentile_Cost=ap.ann_split_80th, 
pt.Annualized_Unsplit_1stPercentile_Cost=ap.ann_unsplit_1st, 
pt.Annualized_Unsplit_99thPercentile_Cost=ap.ann_unsplit_99th;

DROP TABLE ann_pct;

	
set group_concat_max_len = 10485760;

CREATE TEMPORARY TABLE IF NOT EXISTS ann_pct
(
  Filter_ID int(11) DEFAULT NULL,
episode_id varchar(6),
level tinyint(4),
ann_split_1st decimal(15,4),
ann_split_99th decimal(15,4),
ann_split_80th decimal(15,4),
annd_unsplit_1st decimal(15,4),
ann_unsplit_99th decimal(15,4),
index(episode_id),
index(level)
)

AS	
SELECT
red.Filter_id,
red.episode_id,red.level,cast(substring_index(substring_index(	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_1st',cast(substring_index(substring_index(	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_99th',
cast(substring_index(substring_index(	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),	',', 80/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_80th',cast(substring_index(substring_index(	group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_1st',cast(substring_index(substring_index(	group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_99th'from report_episode_detail redwhere red.level=2
and red.annualized_split_total_cost>0and red.episode_id IN ('EC0202', 'EC0301', 'EC0401', 'EC0402', 'EC0508', 'EC0510', 'EC0511', 'EC0518', 'EC0601', 'EC0801', 'EC0802', 'EC1001', 'EC1902', 'EC1903', 'EX9901') 
group by red.Filter_id, red.episode_id ;

UPDATE percentiles pt
JOIN ann_pct ap ON ap.episode_id=pt.Episode_ID
AND ap.level=pt.Level
and ap.Filter_id=pt.filter_id
SET 
pt.Annualized_Split_1stPercentile_Cost=ap.ann_split_1st, 
pt.Annualized_Split_99thPercentile_Cost=ap.ann_split_99th, 
pt.Annualized_Split_80thPercentile_Cost=ap.ann_split_80th, 
pt.Annualized_Unsplit_1stPercentile_Cost=ap.ann_unsplit_1st, 
pt.Annualized_Unsplit_99thPercentile_Cost=ap.ann_unsplit_99th;

DROP TABLE ann_pct;

	
	
set group_concat_max_len = 10485760;

CREATE TEMPORARY TABLE IF NOT EXISTS ann_pct
(
  Filter_ID int(11) DEFAULT NULL,
episode_id varchar(6),
level tinyint(4),
ann_split_1st decimal(15,4),
ann_split_99th decimal(15,4),
ann_split_80th decimal(15,4),
annd_unsplit_1st decimal(15,4),
ann_unsplit_99th decimal(15,4),
index(episode_id),
index(level)
)

AS	
SELECT
red.Filter_id,
red.episode_id,red.level,cast(substring_index(substring_index(	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_1st',cast(substring_index(substring_index(	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_99th',
cast(substring_index(substring_index(	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),	',', 80/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_80th',cast(substring_index(substring_index(	group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_1st',cast(substring_index(substring_index(	group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_99th'from report_episode_detail redwhere red.level=3
and red.annualized_split_total_cost>0and red.episode_id IN ('EC0202', 'EC0301', 'EC0401', 'EC0402', 'EC0508', 'EC0510', 'EC0511', 'EC0518', 'EC0601', 'EC0801', 'EC0802', 'EC1001', 'EC1902', 'EC1903', 'EX9901') 
group by red.Filter_id, red.episode_id ;

UPDATE percentiles pt
JOIN ann_pct ap ON ap.episode_id=pt.Episode_ID
AND ap.level=pt.Level
and ap.Filter_id=pt.filter_id
SET 
pt.Annualized_Split_1stPercentile_Cost=ap.ann_split_1st, 
pt.Annualized_Split_99thPercentile_Cost=ap.ann_split_99th, 
pt.Annualized_Split_80thPercentile_Cost=ap.ann_split_80th, 
pt.Annualized_Unsplit_1stPercentile_Cost=ap.ann_unsplit_1st, 
pt.Annualized_Unsplit_99thPercentile_Cost=ap.ann_unsplit_99th;

DROP TABLE ann_pct;

	
set group_concat_max_len = 10485760;

CREATE TEMPORARY TABLE IF NOT EXISTS ann_pct
(
  Filter_ID int(11) DEFAULT NULL,
episode_id varchar(6),
level tinyint(4),
ann_split_1st decimal(15,4),
ann_split_99th decimal(15,4),
ann_split_80th decimal(15,4),
annd_unsplit_1st decimal(15,4),
ann_unsplit_99th decimal(15,4),
index(episode_id),
index(level)
)

AS	
SELECT
red.Filter_id,
red.episode_id,red.level,cast(substring_index(substring_index(	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_1st',cast(substring_index(substring_index(	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_99th',
cast(substring_index(substring_index(	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),	',', 80/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_80th',cast(substring_index(substring_index(	group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_1st',cast(substring_index(substring_index(	group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_99th'from report_episode_detail redwhere red.level=4
and red.annualized_split_total_cost>0and red.episode_id IN ('EC0202', 'EC0301', 'EC0401', 'EC0402', 'EC0508', 'EC0510', 'EC0511', 'EC0518', 'EC0601', 'EC0801', 'EC0802', 'EC1001', 'EC1902', 'EC1903', 'EX9901') 
group by red.Filter_id, red.episode_id ;

UPDATE percentiles pt
JOIN ann_pct ap ON ap.episode_id=pt.Episode_ID
AND ap.level=pt.Level
and ap.Filter_id=pt.filter_id
SET 
pt.Annualized_Split_1stPercentile_Cost=ap.ann_split_1st, 
pt.Annualized_Split_99thPercentile_Cost=ap.ann_split_99th, 
pt.Annualized_Split_80thPercentile_Cost=ap.ann_split_80th, 
pt.Annualized_Unsplit_1stPercentile_Cost=ap.ann_unsplit_1st, 
pt.Annualized_Unsplit_99thPercentile_Cost=ap.ann_unsplit_99th;

DROP TABLE ann_pct;

	
set group_concat_max_len = 10485760;

CREATE TEMPORARY TABLE IF NOT EXISTS ann_pct
(
  Filter_ID int(11) DEFAULT NULL,
episode_id varchar(6),
level tinyint(4),
ann_split_1st decimal(15,4),
ann_split_99th decimal(15,4),
ann_split_80th decimal(15,4),
annd_unsplit_1st decimal(15,4),
ann_unsplit_99th decimal(15,4),
index(episode_id),
index(level)
)

AS	
SELECT
red.Filter_id,
red.episode_id,red.level,cast(substring_index(substring_index(	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_1st',cast(substring_index(substring_index(	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_99th',
cast(substring_index(substring_index(	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),	',', 80/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_80th',cast(substring_index(substring_index(	group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_1st',cast(substring_index(substring_index(	group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_99th'from report_episode_detail redwhere red.level=5
and red.annualized_split_total_cost>0and red.episode_id IN ('EC0202', 'EC0301', 'EC0401', 'EC0402', 'EC0508', 'EC0510', 'EC0511', 'EC0518', 'EC0601', 'EC0801', 'EC0802', 'EC1001', 'EC1902', 'EC1903', 'EX9901') 
group by red.Filter_id, red.episode_id ;

UPDATE percentiles pt
JOIN ann_pct ap ON ap.episode_id=pt.Episode_ID
AND ap.level=pt.Level
and ap.Filter_id=pt.filter_id
SET 
pt.Annualized_Split_1stPercentile_Cost=ap.ann_split_1st, 
pt.Annualized_Split_99thPercentile_Cost=ap.ann_split_99th, 
pt.Annualized_Split_80thPercentile_Cost=ap.ann_split_80th, 
pt.Annualized_Unsplit_1stPercentile_Cost=ap.ann_unsplit_1st, 
pt.Annualized_Unsplit_99thPercentile_Cost=ap.ann_unsplit_99th;

DROP TABLE ann_pct;

create index red on report_episode_detail(episode_id);
create index punt on percentiles(episode_id);

SELECT *
FROM percentiles p;

UPDATE report_episode_detail red
JOIN percentiles p ON p.episode_id=red.episode_id
AND p.level=red.level
and p.filter_id=red.filter_id
SET 
red.Split_1stPercentile_Cost=p.Split_1stPercentile_Cost,
red.Split_99thPercentile_Cost=p.Split_99thPercentile_Cost,
red.Split_80thPercentile_Cost=p.Split_80thPercentile_Cost,
red.Annualized_Split_1stPercentile_Cost=p.Annualized_Split_1stPercentile_Cost,
red.Annualized_Split_99thPercentile_Cost=p.Annualized_Split_99thPercentile_Cost,
red.Annualized_Split_80thPercentile_Cost=p.Annualized_Split_80thPercentile_Cost,
red.Unsplit_1stPercentile_Cost=p.Unsplit_1stPercentile_Cost,
red.Unsplit_99thPercentile_Cost=p.Unsplit_99thPercentile_Cost,
red.Annualized_Unsplit_1stPercentile_Cost=p.Annualized_Unsplit_1stPercentile_Cost,
red.Annualized_Unsplit_99thPercentile_Cost=p.Annualized_Unsplit_99thPercentile_Cost;

DROP TABLE IF EXISTS assign_1;

create table assign_1 as
select assignment.*, 
association.Parent_master_episode_id as EPISODE,
'2' as Level_Assignment
from assignment
left join association
on assignment.master_episode_id = `Child_master_episode_id`
where assigned_type='C' and association.`association_level`=2

union

select assignment.*, 
master_episode_id as EPISODE,
'1' as Level_Assignment
from assignment
where assigned_type='C' 

union

select assignment.*, 
association.Parent_master_episode_id as EPISODE,
'3' as Level_Assignment
from assignment
left join association
on assignment.master_episode_id = `Child_master_episode_id`
where assigned_type='C' and association.`association_level`=3

union

select assignment.*, 
association.Parent_master_episode_id as EPISODE,
'4' as Level_Assignment
from assignment
left join association
on assignment.master_episode_id = `Child_master_episode_id`
where assigned_type='C' and association.`association_level`=4

union

select assignment.*, 
association.Parent_master_episode_id as EPISODE,
'5' as Level_Assignment
from assignment
left join association
on assignment.master_episode_id = `Child_master_episode_id`
where assigned_type='C' and association.`association_level`=5

;


DROP TABLE IF EXISTS Assign_PAC_Totals;

create table Assign_PAC_Totals as
select
EPISODE,
'1' as Level_Assignment,
count(distinct (case when claim_source = 'IP' then master_claim_id end)) as IP_PAC_Count,
count(distinct (case when claim_source = 'OP'  then master_claim_id end)) as OP_PAC_Count,
count(distinct (case when claim_source = 'PB' then master_claim_id end))  as PB_PAC_Count,
count(distinct (case when claim_source = 'RX' then master_claim_id end)) as RX_PAC_Count
from assign_1
where Level_Assignment='1'
group by EPISODE

union 

select
EPISODE,
'2' as Level_Assignment,
count(distinct (case when claim_source = 'IP' then master_claim_id end))  as IP_PAC_Count,
count(distinct (case when claim_source = 'OP'  then master_claim_id end)) as OP_PAC_Count,
count(distinct (case when claim_source = 'PB' then master_claim_id end))  as PB_PAC_Count,
count(distinct (case when claim_source = 'RX' then master_claim_id end)) as RX_PAC_Count
from assign_1
where Level_Assignment in ('1','2') 
group by EPISODE
union 

select
EPISODE,
'3' as Level_Assignment,
count(distinct (case when claim_source = 'IP' then master_claim_id end))  as IP_PAC_Count,
count(distinct (case when claim_source = 'OP'  then master_claim_id end)) as OP_PAC_Count,
count(distinct (case when claim_source = 'PB' then master_claim_id end)) as PB_PAC_Count,
count(distinct (case when claim_source = 'RX' then master_claim_id end)) as RX_PAC_Count
from assign_1
where Level_Assignment in ('1','2','3') 
group by EPISODE

union 

select
EPISODE,
'4' as Level_Assignment,
count(distinct (case when claim_source = 'IP' then master_claim_id end))  as IP_PAC_Count,
count(distinct (case when claim_source = 'OP'  then master_claim_id end)) as OP_PAC_Count,
count(distinct (case when claim_source = 'PB' then master_claim_id end))   as PB_PAC_Count,
count(distinct (case when claim_source = 'RX' then master_claim_id end)) as RX_PAC_Count
from assign_1
where Level_Assignment in ('1','2','3','4') 
group by EPISODE

union 

select
EPISODE,
'5' as Level_Assignment,
count(distinct (case when claim_source = 'IP' then master_claim_id end)) as IP_PAC_Count,
count(distinct (case when claim_source = 'OP' then master_claim_id end)) as OP_PAC_Count,
count(distinct (case when claim_source = 'PB' then master_claim_id end)) as PB_PAC_Count,
count(distinct (case when claim_source = 'RX' then master_claim_id end)) as RX_PAC_Count
from assign_1
group by EPISODE
;

create index redpac3 on report_episode_detail(master_episode_id);
create index redpac2 on report_episode_detail(level);

create index aptpac on Assign_PAC_Totals(episode);
create index aptpac2 on Assign_PAC_Totals(Level_Assignment);

UPDATE report_episode_detail red
JOIN Assign_PAC_Totals p ON p.episode=red.master_episode_id
AND p.Level_Assignment=red.level
SET 
red.IP_PAC_Count=p.IP_PAC_Count,
red.OP_PAC_Count=p.OP_PAC_Count,
red.PB_PAC_Count=p.PB_PAC_Count,
red.RX_PAC_Count=p.RX_PAC_Count;


#add expected costs to report_episode_detail table for level 1 and the level at which the episode is complete

#add level 1 expected costs for chronic and other condition episodes to report_episode_detail

create temporary table if not exists x as

select
rec.epi_id as master_episode_id,
red.level,
rec.total_exp_cost_sa_l1,
rec.exp_cost_sa_typ_l1,
rec.exp_cost_sa_comp_l1,
rec.total_exp_cost_ra_l1,
rec.exp_cost_ra_typ_l1,
rec.exp_cost_ra_comp_l1

from ra_exp_cost rec
left join report_episode_detail red
on rec.epi_id=red.master_episode_id
where rec.exp_cost_sa_typ_l1 is not null
and red.level=1;

update report_episode_detail red
join x
on x.master_episode_id=red.master_episode_id
and x.level=red.level
set
red.split_expected_total_cost=x.total_exp_cost_sa_l1,
red.split_expected_typical_other_cost=x.exp_cost_sa_typ_l1,
red.split_expected_pac_cost=x.exp_cost_sa_comp_l1,
red.unsplit_expected_total_cost=x.total_exp_cost_ra_l1,
red.unsplit_expected_typical_other_cost=x.exp_cost_ra_typ_l1,
red.unsplit_expected_pac_cost=x.exp_cost_ra_comp_l1;

drop table x;

#add level 5 expected costs for chronic and other condition episodes to report_episode_detail
create temporary table if not exists x as

select
rec.epi_id as master_episode_id,
red.level,
rec.total_exp_cost_sa_l5,
rec.exp_cost_sa_typ_l5,
rec.exp_cost_sa_comp_l5,
rec.total_exp_cost_ra_l5,
rec.exp_cost_ra_typ_l5,
rec.exp_cost_ra_comp_l5

from ra_exp_cost rec
left join report_episode_detail red
on rec.epi_id=red.master_episode_id
where rec.exp_cost_sa_typ_l5 is not null
and red.level=5;

update report_episode_detail red
join x
on x.master_episode_id=red.master_episode_id
and x.level=red.level
set
red.split_expected_total_cost=x.total_exp_cost_sa_l5,
red.split_expected_typical_other_cost=x.exp_cost_sa_typ_l5,
red.split_expected_pac_cost=x.exp_cost_sa_comp_l5,
red.unsplit_expected_total_cost=x.total_exp_cost_ra_l5,
red.unsplit_expected_typical_other_cost=x.exp_cost_ra_typ_l5,
red.unsplit_expected_pac_cost=x.exp_cost_ra_comp_l5;

drop table x;

#add level 1 expected costs for acute and procedural episodes to report_episode_detail

create temporary table if not exists x as

select
rec.epi_id as master_episode_id,
red.level,
rec.total_exp_cost_sa_l1,
rec.exp_cost_sa_typ_ip_l1,
rec.exp_cost_sa_typ_other_l1,
rec.exp_cost_sa_comp_other_l1,
rec.total_exp_cost_ra_l1,
rec.exp_cost_ra_typ_ip_l1,
rec.exp_cost_ra_typ_other_l1,
rec.exp_cost_ra_comp_other_l1

from ra_exp_cost rec
left join report_episode_detail red
on rec.epi_id=red.master_episode_id
where rec.exp_cost_sa_typ_ip_l1 is not null
and red.level=1;

update report_episode_detail red
join x
on x.master_episode_id=red.master_episode_id
and x.level=red.level
set
red.split_expected_total_cost=x.total_exp_cost_sa_l1,
red.split_expected_typical_ip_cost=x.exp_cost_sa_typ_ip_l1,
red.split_expected_typical_other_cost=x.exp_cost_sa_typ_other_l1,
red.split_expected_pac_cost=x.exp_cost_sa_comp_other_l1,
red.unsplit_expected_total_cost=x.total_exp_cost_ra_l1,
red.Unsplit_Expected_Typical_IP_Cost=x.exp_cost_ra_typ_ip_l1,
red.unsplit_expected_typical_other_cost=x.exp_cost_ra_typ_other_l1,
red.unsplit_expected_pac_cost=x.exp_cost_ra_comp_other_l1;

drop table x;

#add level 3 expected costs for procedural episodes to report_episode_detail

create temporary table if not exists x as

select
rec.epi_id as master_episode_id,
red.level,
rec.total_exp_cost_sa_l3,
rec.exp_cost_sa_typ_ip_l3,
rec.exp_cost_sa_typ_other_l3,
rec.exp_cost_sa_comp_other_l3,
rec.total_exp_cost_ra_l3,
rec.exp_cost_ra_typ_ip_l3,
rec.exp_cost_ra_typ_other_l3,
rec.exp_cost_ra_comp_other_l3

from ra_exp_cost rec
left join report_episode_detail red
on rec.epi_id=red.master_episode_id
where rec.exp_cost_sa_typ_ip_l3 is not null
and red.level=3;

update report_episode_detail red
join x
on x.master_episode_id=red.master_episode_id
and x.level=red.level
set
red.split_expected_total_cost=x.total_exp_cost_sa_l3,
red.split_expected_typical_ip_cost=x.exp_cost_sa_typ_ip_l3,
red.split_expected_typical_other_cost=x.exp_cost_sa_typ_other_l3,
red.split_expected_pac_cost=x.exp_cost_sa_comp_other_l3,
red.unsplit_expected_total_cost=x.total_exp_cost_ra_l3,
red.Unsplit_Expected_Typical_IP_Cost=x.exp_cost_ra_typ_ip_l3,
red.unsplit_expected_typical_other_cost=x.exp_cost_ra_typ_other_l3,
red.unsplit_expected_pac_cost=x.exp_cost_ra_comp_other_l3;

drop table x;

#add level 4 expected costs for acute episodes to report_episode_detail

create temporary table if not exists x as

select
rec.epi_id as master_episode_id,
red.level,
rec.total_exp_cost_sa_l4,
rec.exp_cost_sa_typ_ip_l4,
rec.exp_cost_sa_typ_other_l4,
rec.exp_cost_sa_comp_other_l4,
rec.total_exp_cost_ra_l4,
rec.exp_cost_ra_typ_ip_l4,
rec.exp_cost_ra_typ_other_l4,
rec.exp_cost_ra_comp_other_l4

from ra_exp_cost rec
left join report_episode_detail red
on rec.epi_id=red.master_episode_id
where rec.exp_cost_sa_typ_ip_l4 is not null
and red.level=4;

update report_episode_detail red
join x
on x.master_episode_id=red.master_episode_id
and x.level=red.level
set
red.split_expected_total_cost=x.total_exp_cost_sa_l4,
red.split_expected_typical_ip_cost=x.exp_cost_sa_typ_ip_l4,
red.split_expected_typical_other_cost=x.exp_cost_sa_typ_other_l4,
red.split_expected_pac_cost=x.exp_cost_sa_comp_other_l4,
red.unsplit_expected_total_cost=x.total_exp_cost_ra_l4,
red.Unsplit_Expected_Typical_IP_Cost=x.exp_cost_ra_typ_ip_l4,
red.unsplit_expected_typical_other_cost=x.exp_cost_ra_typ_other_l4,
red.unsplit_expected_pac_cost=x.exp_cost_ra_comp_other_l4;

drop table x;

#compare actual vs. expected

SELECT
red.filter_id,
red.episode_id,
red.episode_name,
red.episode_type,
red.level,
count(*) as episode_volume,
sum(red.split_total_cost),
sum(red.annualized_split_total_cost),
sum(red.split_expected_total_cost),
(sum(red.split_total_cost))/count(*) as average_split_costs,
(sum(red.annualized_split_total_cost))/count(*) as average_annualized_split_costs,
(sum(red.split_expected_total_cost))/count(*) as average_expected_split_costs

from report_episode_detail red
where red.episode_type <> 'System Related Failure'
group by red.filter_id, red.episode_id, red.level
order by red.level, red.episode_id
