/* Generate report episode detail (RED) table */

/* Drop and recreate red table */

DROP TABLE IF EXISTS report_episode_detail;

CREATE TABLE `report_episode_detail` (
    Filter_ID INT(11) DEFAULT NULL,
    `Member_ID` VARCHAR(50) DEFAULT NULL,
    `Master_Episode_ID` VARCHAR(255) DEFAULT NULL,
    `Episode_ID` VARCHAR(6) DEFAULT NULL,
    `Episode_Name` VARCHAR(6) DEFAULT NULL,
    `Episode_Description` VARCHAR(255) DEFAULT NULL,
    `Episode_Type` VARCHAR(50) DEFAULT NULL,
    `MDC` INT(2) DEFAULT NULL,
    `MDC_Description` VARCHAR(255) DEFAULT NULL,
    `Episode_Begin_Date` VARCHAR(10) DEFAULT NULL,
    `Episode_End_Date` VARCHAR(10) DEFAULT NULL,
    `Episode_Length` INT(6) DEFAULT NULL,
    `Level` INT(1) DEFAULT NULL,
    `Split_Total_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Split_1stPercentile_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Split_99thPercentile_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Split_80thPercentile_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Split_Total_PAC_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Split_Total_Typical_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Split_Total_TypicalwPAC_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Annualized_Split_Total_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Annualized_Split_1stPercentile_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Annualized_Split_99thPercentile_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Annualized_Split_80thPercentile_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Annualized_Split_Total_PAC_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Annualized_Split_Total_Typical_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Annualized_Split_Total_TypicalwPAC_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Unsplit_Total_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Unsplit_1stPercentile_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Unsplit_99thPercentile_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Unsplit_Total_PAC_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Unsplit_Total_Typical_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Unsplit_Total_TypicalwPAC_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Annualized_Unsplit_Total_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Annualized_Unsplit_1stPercentile_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Annualized_Unsplit_99thPercentile_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Annualized_Unsplit_Total_PAC_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Annualized_Unsplit_Total_Typical_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Annualized_Unsplit_Total_TypicalwPAC_Cost` DECIMAL(13 , 2 ) DEFAULT NULL,
    `Facility_ID` VARCHAR(50) DEFAULT NULL,
    `Physician_ID` VARCHAR(50) DEFAULT NULL,
    `Physician_Specialty` VARCHAR(2) DEFAULT NULL,
    Split_Expected_Total_Cost DECIMAL(13 , 2 ) DEFAULT NULL,
    Split_Expected_Typical_IP_Cost DECIMAL(13 , 2 ) DEFAULT NULL,
    Split_Expected_Typical_Other_Cost DECIMAL(13 , 2 ) DEFAULT NULL,
    Split_Expected_PAC_Cost DECIMAL(13 , 2 ) DEFAULT NULL,
    Unsplit_Expected_Total_Cost DECIMAL(13 , 2 ) DEFAULT NULL,
    Unsplit_Expected_Typical_IP_Cost DECIMAL(13 , 2 ) DEFAULT NULL,
    Unsplit_Expected_Typical_Other_Cost DECIMAL(13 , 2 ) DEFAULT NULL,
    Unsplit_Expected_PAC_Cost DECIMAL(13 , 2 ) DEFAULT NULL,
    IP_PAC_Count DECIMAL(13 , 2 ) DEFAULT NULL,
    OP_PAC_Count DECIMAL(13 , 2 ) DEFAULT NULL,
    PB_PAC_Count DECIMAL(13 , 2 ) DEFAULT NULL,
    RX_PAC_Count DECIMAL(13 , 2 ) DEFAULT NULL,
    KEY `episode_id` (`Episode_ID`),
    KEY `Master_Episode_ID` (`Master_Episode_ID`),
    KEY `user_level` (`Level`)
);


/* build the rows of the RED table from master_epid_level, episode, build_episode_reference, and mdc_desc tables */

INSERT into report_episode_detail
	(Filter_ID, Member_ID, Master_Episode_ID, Episode_ID, Episode_Name, Episode_Description, Episode_Type, 
	MDC, MDC_Description, Episode_Begin_Date, Episode_End_Date, Episode_Length, Level, 
    Split_Total_Cost, Split_Total_PAC_Cost, Split_Total_Typical_Cost, Split_Total_TypicalwPAC_Cost, Facility_ID, Physician_ID)
    
    
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

	FROM episode, build_episode_reference, master_epid_level, mdc_desc
	WHERE episode.episode_id=build_episode_reference.episode_id
		AND episode.master_episode_id=master_epid_level.master_episode_id
		AND build_episode_reference.MDC_CATEGORY=mdc_desc.mdc
		AND master_epid_level.claim_type='CL'
		AND master_epid_level.split=1
		AND master_epid_level.annualized=0;



/* Create first version of master episode level detail table */
/* load total unsplit costs, then update RED */

DROP TABLE IF EXISTS melDet;

CREATE TEMPORARY TABLE melDet 
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

	AS SELECT mel.filter_id, mel.master_episode_id, mel.level, mel.cost, mel.cost_c, mel.cost_t, mel.cost_tc
		FROM master_epid_level mel
		WHERE mel.claim_type='CL'
			AND mel.split=0
			AND mel.annualized=0;

UPDATE report_episode_detail red
	JOIN melDet mel ON mel.master_episode_id=red.Master_Episode_ID
		AND mel.level=red.Level
		AND mel.filter_id=red.filter_id
	SET 
		red.Unsplit_Total_Cost=mel.cost, 
		red.Unsplit_Total_PAC_Cost=mel.cost_c, 
		red.Unsplit_Total_Typical_Cost=mel.cost_t, 
		red.Unsplit_Total_TypicalwPAC_Cost=mel.cost_tc;


/* Create second version of master episode level detail table */
/* loads annualized split costs, then update RED  */

DROP TABLE IF EXISTS melDet;

CREATE TEMPORARY TABLE melDet AS	
	SELECT mel.filter_id, mel.master_episode_id, mel.level, mel.cost, mel.cost_c, mel.cost_t, mel.cost_tc
	FROM master_epid_level mel
	WHERE mel.claim_type='CL'
		AND mel.split=1
		AND mel.annualized=1;

UPDATE report_episode_detail red
	JOIN melDet mel ON mel.master_episode_id=red.Master_Episode_ID
		AND mel.level=red.Level
		AND mel.filter_id=red.filter_id
	SET 
		red.Annualized_Split_Total_Cost=mel.cost, 
		red.Annualized_Split_Total_PAC_Cost=mel.cost_c, 
		red.Annualized_Split_Total_Typical_Cost=mel.cost_t, 
		red.Annualized_Split_Total_TypicalwPAC_Cost=mel.cost_tc;
		


/* Create third version of master episode level detail table */
/* loads annualized unsplit costs, then update RED  */

DROP TABLE IF EXISTS melDet;

CREATE TEMPORARY TABLE IF NOT EXISTS melDet AS	
	SELECT mel.filter_id, mel.master_episode_id, mel.level, mel.cost, mel.cost_c, mel.cost_t, mel.cost_tc
	FROM master_epid_level mel
	WHERE mel.claim_type='CL'
		AND mel.split=0
		AND mel.annualized=1;

UPDATE report_episode_detail red
	JOIN melDet mel ON mel.master_episode_id=red.Master_Episode_ID
		AND mel.level=red.Level
		AND mel.filter_id=red.filter_id
	SET 
		red.Annualized_Unsplit_Total_Cost=mel.cost, 
		red.Annualized_Unsplit_Total_PAC_Cost=mel.cost_c, 
		red.Annualized_Unsplit_Total_Typical_Cost=mel.cost_t, 
		red.Annualized_Unsplit_Total_TypicalwPAC_Cost=mel.cost_tc;

DROP TABLE IF EXISTS melDet;
		
		
/* percentiles */	
/* create percentiles table */		
				
DROP TABLE IF EXISTS percentiles;	
SET group_concat_max_len = 10485760;
					
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
);
  

/* load level 1 percentiles */

INSERT into percentiles
	(Filter_id, Episode_ID, Level, Split_1stPercentile_Cost, Split_99thPercentile_Cost, Split_80thPercentile_Cost, Unsplit_1stPercentile_Cost, Unsplit_99thPercentile_Cost)

	SELECT red.Filter_id, red.episode_id, red.level,
		cast(substring_index(substring_index(group_concat(red.split_total_cost order by red.split_total_cost separator ','),',', 1/100 * count(*) +1), ',', -1) as decimal) as 'Split_1stPercentile_Cost',
		cast(substring_index(substring_index(group_concat(red.split_total_cost order by red.split_total_cost separator ','),',', 99/100 * count(*) +1), ',', -1) as decimal) as 'Split_99thPercentile_Cost',
		cast(substring_index(substring_index(group_concat(red.split_total_cost order by red.split_total_cost separator ','),',', 80/100 * count(*) +1), ',', -1) as decimal) as 'Split_80thPercentile_Cost',
		cast(substring_index(substring_index(group_concat(red.unsplit_total_cost order by red.unsplit_total_cost separator ','),',', 1/100 * count(*) +1), ',', -1) as decimal) as 'Unsplit_1stPercentile_Cost',
		cast(substring_index(substring_index(group_concat(red.unsplit_total_cost order by red.unsplit_total_cost separator ','),',', 99/100 * count(*) +1), ',', -1) as decimal) as 'Unsplit_99thPercentile_Cost'

	FROM report_episode_detail red
	WHERE red.level=1
		AND red.split_total_cost>0
	GROUP BY red.Filter_id, red.episode_id;
	
	
/* load level 2 percentiles */

INSERT into percentiles
	(Filter_id, Episode_ID, Level, Split_1stPercentile_Cost, Split_99thPercentile_Cost, Split_80thPercentile_Cost, Unsplit_1stPercentile_Cost, Unsplit_99thPercentile_Cost)

	SELECT red.Filter_id, red.episode_id, red.level,
		cast(substring_index(substring_index(group_concat(red.split_total_cost order by red.split_total_cost separator ','),',', 1/100 * count(*) +1), ',', -1) as decimal) as 'Split_1stPercentile_Cost',
		cast(substring_index(substring_index(group_concat(red.split_total_cost order by red.split_total_cost separator ','),',', 99/100 * count(*) +1), ',', -1) as decimal) as 'Split_99thPercentile_Cost',
		cast(substring_index(substring_index(group_concat(red.split_total_cost order by red.split_total_cost separator ','),',', 80/100 * count(*) +1), ',', -1) as decimal) as 'Split_80thPercentile_Cost',
		cast(substring_index(substring_index(group_concat(red.unsplit_total_cost order by red.unsplit_total_cost separator ','),',', 1/100 * count(*) +1), ',', -1) as decimal) as 'Unsplit_1stPercentile_Cost',
		cast(substring_index(substring_index(group_concat(red.unsplit_total_cost order by red.unsplit_total_cost separator ','),',', 99/100 * count(*) +1), ',', -1) as decimal) as 'Unsplit_99thPercentile_Cost'

	FROM report_episode_detail red
	WHERE red.level=2
		AND red.split_total_cost>0
	GROUP BY red.Filter_id, red.episode_id;
	
	
/* load level 3 percentiles */

INSERT into percentiles
	(Filter_id, Episode_ID, Level, Split_1stPercentile_Cost, Split_99thPercentile_Cost, Split_80thPercentile_Cost, Unsplit_1stPercentile_Cost, Unsplit_99thPercentile_Cost)

	SELECT red.Filter_id, red.episode_id, red.level,
		cast(substring_index(substring_index(group_concat(red.split_total_cost order by red.split_total_cost separator ','),',', 1/100 * count(*) +1), ',', -1) as decimal) as 'Split_1stPercentile_Cost',
		cast(substring_index(substring_index(group_concat(red.split_total_cost order by red.split_total_cost separator ','),',', 99/100 * count(*) +1), ',', -1) as decimal) as 'Split_99thPercentile_Cost',
		cast(substring_index(substring_index(group_concat(red.split_total_cost order by red.split_total_cost separator ','),',', 80/100 * count(*) +1), ',', -1) as decimal) as 'Split_80thPercentile_Cost',
		cast(substring_index(substring_index(group_concat(red.unsplit_total_cost order by red.unsplit_total_cost separator ','),',', 1/100 * count(*) +1), ',', -1) as decimal) as 'Unsplit_1stPercentile_Cost',
		cast(substring_index(substring_index(group_concat(red.unsplit_total_cost order by red.unsplit_total_cost separator ','),',', 99/100 * count(*) +1), ',', -1) as decimal) as 'Unsplit_99thPercentile_Cost'

	FROM report_episode_detail red
	WHERE red.level=3
		AND red.split_total_cost>0
	GROUP BY red.Filter_id, red.episode_id;
	
	
/* load level 4 percentiles */

INSERT into percentiles
	(Filter_id, Episode_ID, Level, Split_1stPercentile_Cost, Split_99thPercentile_Cost, Split_80thPercentile_Cost, Unsplit_1stPercentile_Cost, Unsplit_99thPercentile_Cost)

	SELECT red.Filter_id, red.episode_id, red.level,
		cast(substring_index(substring_index(group_concat(red.split_total_cost order by red.split_total_cost separator ','),',', 1/100 * count(*) +1), ',', -1) as decimal) as 'Split_1stPercentile_Cost',
		cast(substring_index(substring_index(group_concat(red.split_total_cost order by red.split_total_cost separator ','),',', 99/100 * count(*) +1), ',', -1) as decimal) as 'Split_99thPercentile_Cost',
		cast(substring_index(substring_index(group_concat(red.split_total_cost order by red.split_total_cost separator ','),',', 80/100 * count(*) +1), ',', -1) as decimal) as 'Split_80thPercentile_Cost',
		cast(substring_index(substring_index(group_concat(red.unsplit_total_cost order by red.unsplit_total_cost separator ','),',', 1/100 * count(*) +1), ',', -1) as decimal) as 'Unsplit_1stPercentile_Cost',
		cast(substring_index(substring_index(group_concat(red.unsplit_total_cost order by red.unsplit_total_cost separator ','),',', 99/100 * count(*) +1), ',', -1) as decimal) as 'Unsplit_99thPercentile_Cost'

	FROM report_episode_detail red
	WHERE red.level=4
		AND red.split_total_cost>0
	GROUP BY red.Filter_id, red.episode_id;
		
	
	
/* load level 5 percentiles */

INSERT into percentiles
	(Filter_id, Episode_ID, Level, Split_1stPercentile_Cost, Split_99thPercentile_Cost, Split_80thPercentile_Cost, Unsplit_1stPercentile_Cost, Unsplit_99thPercentile_Cost)

	SELECT red.Filter_id, red.episode_id, red.level,
		cast(substring_index(substring_index(group_concat(red.split_total_cost order by red.split_total_cost separator ','),',', 1/100 * count(*) +1), ',', -1) as decimal) as 'Split_1stPercentile_Cost',
		cast(substring_index(substring_index(group_concat(red.split_total_cost order by red.split_total_cost separator ','),',', 99/100 * count(*) +1), ',', -1) as decimal) as 'Split_99thPercentile_Cost',
		cast(substring_index(substring_index(group_concat(red.split_total_cost order by red.split_total_cost separator ','),',', 80/100 * count(*) +1), ',', -1) as decimal) as 'Split_80thPercentile_Cost',
		cast(substring_index(substring_index(group_concat(red.unsplit_total_cost order by red.unsplit_total_cost separator ','),',', 1/100 * count(*) +1), ',', -1) as decimal) as 'Unsplit_1stPercentile_Cost',
		cast(substring_index(substring_index(group_concat(red.unsplit_total_cost order by red.unsplit_total_cost separator ','),',', 99/100 * count(*) +1), ',', -1) as decimal) as 'Unsplit_99thPercentile_Cost'

	FROM report_episode_detail red
	WHERE red.level=5
		AND red.split_total_cost>0
	GROUP BY red.Filter_id, red.episode_id;


/* generate annual percentage table for specified episodes */
/* annual percentages for level 1 */

DROP TABLE IF EXISTS ann_pct;

CREATE TEMPORARY TABLE IF NOT EXISTS ann_pct (
	Filter_ID int(11) DEFAULT NULL,
	episode_id varchar(6),
	level tinyint(4),
	ann_split_1st decimal(15,4),
	ann_split_99th decimal(15,4),
	ann_split_80th decimal(15,4),
	ann_unsplit_1st decimal(15,4),
	ann_unsplit_99th decimal(15,4),
	index(episode_id),
	index(level)
	);

INSERT INTO ann_pct
	(Filter_id, episode_id, level, ann_split_1st, ann_split_99th, ann_split_80th, ann_unsplit_1st, ann_unsplit_99th)
	
	SELECT red.Filter_id, red.episode_id, red.level,		cast(substring_index(substring_index(group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_1st',		cast(substring_index(substring_index(group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_99th',
		cast(substring_index(substring_index(group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),',', 80/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_80th',		cast(substring_index(substring_index(group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_1st',		cast(substring_index(substring_index(group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_99th'	FROM report_episode_detail red	WHERE red.level=1
		AND red.annualized_split_total_cost>0		AND red.episode_id IN ('EC0202', 'EC0301', 'EC0401', 'EC0402', 'EC0508', 'EC0510', 'EC0511', 'EC0518', 'EC0601', 'EC0801', 'EC0802', 'EC1001', 'EC1902', 'EC1903', 'EX9901') 
	GROUP BY red.Filter_id, red.episode_id;

UPDATE percentiles pt
	JOIN ann_pct ap ON ap.episode_id=pt.Episode_ID
		AND ap.level=pt.Level
		AND ap.Filter_id=pt.filter_id
	SET 
		pt.Annualized_Split_1stPercentile_Cost=ap.ann_split_1st, 
		pt.Annualized_Split_99thPercentile_Cost=ap.ann_split_99th, 
		pt.Annualized_Split_80thPercentile_Cost=ap.ann_split_80th, 
		pt.Annualized_Unsplit_1stPercentile_Cost=ap.ann_unsplit_1st, 
		pt.Annualized_Unsplit_99thPercentile_Cost=ap.ann_unsplit_99th;

TRUNCATE TABLE ann_pct;


/* annual percentages for level 2 */

INSERT INTO ann_pct
	(Filter_id, episode_id, level, ann_split_1st, ann_split_99th, ann_split_80th, ann_unsplit_1st, ann_unsplit_99th)
	
	SELECT red.Filter_id, red.episode_id, red.level,
		cast(substring_index(substring_index(group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_1st',
		cast(substring_index(substring_index(group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_99th',
		cast(substring_index(substring_index(group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),',', 80/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_80th',
		cast(substring_index(substring_index(group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_1st',
		cast(substring_index(substring_index(group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_99th'

	FROM report_episode_detail red
	WHERE red.level=2
		AND red.annualized_split_total_cost>0
		AND red.episode_id IN ('EC0202', 'EC0301', 'EC0401', 'EC0402', 'EC0508', 'EC0510', 'EC0511', 'EC0518', 'EC0601', 'EC0801', 'EC0802', 'EC1001', 'EC1902', 'EC1903', 'EX9901') 
	GROUP BY red.Filter_id, red.episode_id;

UPDATE percentiles pt
	JOIN ann_pct ap ON ap.episode_id=pt.Episode_ID
		AND ap.level=pt.Level
		AND ap.Filter_id=pt.filter_id
	SET 
		pt.Annualized_Split_1stPercentile_Cost=ap.ann_split_1st, 
		pt.Annualized_Split_99thPercentile_Cost=ap.ann_split_99th, 
		pt.Annualized_Split_80thPercentile_Cost=ap.ann_split_80th, 
		pt.Annualized_Unsplit_1stPercentile_Cost=ap.ann_unsplit_1st, 
		pt.Annualized_Unsplit_99thPercentile_Cost=ap.ann_unsplit_99th;

TRUNCATE TABLE ann_pct;

	

/* annual percentages for level 3 */

INSERT INTO ann_pct
	(Filter_id, episode_id, level, ann_split_1st, ann_split_99th, ann_split_80th, ann_unsplit_1st, ann_unsplit_99th)
	
	SELECT red.Filter_id, red.episode_id, red.level,
		cast(substring_index(substring_index(group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_1st',
		cast(substring_index(substring_index(group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_99th',
		cast(substring_index(substring_index(group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),',', 80/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_80th',
		cast(substring_index(substring_index(group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_1st',
		cast(substring_index(substring_index(group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_99th'

	FROM report_episode_detail red
	WHERE red.level=3
		AND red.annualized_split_total_cost>0
		AND red.episode_id IN ('EC0202', 'EC0301', 'EC0401', 'EC0402', 'EC0508', 'EC0510', 'EC0511', 'EC0518', 'EC0601', 'EC0801', 'EC0802', 'EC1001', 'EC1902', 'EC1903', 'EX9901') 
	GROUP BY red.Filter_id, red.episode_id;

UPDATE percentiles pt
	JOIN ann_pct ap ON ap.episode_id=pt.Episode_ID
		AND ap.level=pt.Level
		AND ap.Filter_id=pt.filter_id
	SET 
		pt.Annualized_Split_1stPercentile_Cost=ap.ann_split_1st, 
		pt.Annualized_Split_99thPercentile_Cost=ap.ann_split_99th, 
		pt.Annualized_Split_80thPercentile_Cost=ap.ann_split_80th, 
		pt.Annualized_Unsplit_1stPercentile_Cost=ap.ann_unsplit_1st, 
		pt.Annualized_Unsplit_99thPercentile_Cost=ap.ann_unsplit_99th;

TRUNCATE TABLE ann_pct;

	

/* annual percentages for level 4 */

INSERT INTO ann_pct
	(Filter_id, episode_id, level, ann_split_1st, ann_split_99th, ann_split_80th, ann_unsplit_1st, ann_unsplit_99th)
	
	SELECT red.Filter_id, red.episode_id, red.level,
		cast(substring_index(substring_index(group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_1st',
		cast(substring_index(substring_index(group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_99th',
		cast(substring_index(substring_index(group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),',', 80/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_80th',
		cast(substring_index(substring_index(group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_1st',
		cast(substring_index(substring_index(group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_99th'

	FROM report_episode_detail red
	WHERE red.level=4
		AND red.annualized_split_total_cost>0
		AND red.episode_id IN ('EC0202', 'EC0301', 'EC0401', 'EC0402', 'EC0508', 'EC0510', 'EC0511', 'EC0518', 'EC0601', 'EC0801', 'EC0802', 'EC1001', 'EC1902', 'EC1903', 'EX9901') 
	GROUP BY red.Filter_id, red.episode_id;

UPDATE percentiles pt
	JOIN ann_pct ap ON ap.episode_id=pt.Episode_ID
		AND ap.level=pt.Level
		AND ap.Filter_id=pt.filter_id
	SET 
		pt.Annualized_Split_1stPercentile_Cost=ap.ann_split_1st, 
		pt.Annualized_Split_99thPercentile_Cost=ap.ann_split_99th, 
		pt.Annualized_Split_80thPercentile_Cost=ap.ann_split_80th, 
		pt.Annualized_Unsplit_1stPercentile_Cost=ap.ann_unsplit_1st, 
		pt.Annualized_Unsplit_99thPercentile_Cost=ap.ann_unsplit_99th;

TRUNCATE TABLE ann_pct;

	

/* annual percentages for level 5 */

INSERT INTO ann_pct
	(Filter_id, episode_id, level, ann_split_1st, ann_split_99th, ann_split_80th, ann_unsplit_1st, ann_unsplit_99th)
	
	SELECT red.Filter_id, red.episode_id, red.level,
		cast(substring_index(substring_index(group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_1st',
		cast(substring_index(substring_index(group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_99th',
		cast(substring_index(substring_index(group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),',', 80/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_80th',
		cast(substring_index(substring_index(group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_1st',
		cast(substring_index(substring_index(group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_99th'

	FROM report_episode_detail red
	WHERE red.level=5
		AND red.annualized_split_total_cost>0
		AND red.episode_id IN ('EC0202', 'EC0301', 'EC0401', 'EC0402', 'EC0508', 'EC0510', 'EC0511', 'EC0518', 'EC0601', 'EC0801', 'EC0802', 'EC1001', 'EC1902', 'EC1903', 'EX9901') 
	GROUP BY red.Filter_id, red.episode_id;

UPDATE percentiles pt
	JOIN ann_pct ap ON ap.episode_id=pt.Episode_ID
		AND ap.level=pt.Level
		AND ap.Filter_id=pt.filter_id
	SET 
		pt.Annualized_Split_1stPercentile_Cost=ap.ann_split_1st, 
		pt.Annualized_Split_99thPercentile_Cost=ap.ann_split_99th, 
		pt.Annualized_Split_80thPercentile_Cost=ap.ann_split_80th, 
		pt.Annualized_Unsplit_1stPercentile_Cost=ap.ann_unsplit_1st, 
		pt.Annualized_Unsplit_99thPercentile_Cost=ap.ann_unsplit_99th;

	
DROP TABLE ann_pct;


/* create two RED indexes */

//create index red on report_episode_detail(episode_id);  // <-- duplicate
//create index punt on percentiles(episode_id);  // <-- duplicate



/*  SELECT * FROM percentiles p;  <-- this doesn't do anything  */

UPDATE report_episode_detail red
	JOIN percentiles p ON p.episode_id=red.episode_id
		AND p.level=red.level
		AND p.filter_id=red.filter_id
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

//CREATE INDEX emeid on episode(master_episode_id);  // <-- duplicate
//CREATE INDEX asmed on association(child_master_episode_id);  // duplicate

DROP TABLE IF EXISTS level_2;
DROP TABLE IF EXISTS level_3;
DROP TABLE IF EXISTS level_4;
DROP TABLE IF EXISTS level_5;

CREATE TABLE level_2a as 
	SELECT 
		episode.`member_id`,
		episode.master_episode_id as LEVEL_1,
		case 
			when association.association_level=2 
			then association.parent_master_episode_id 
			else episode.master_episode_id 
		end as LEVEL_2
	FROM episode
	JOIN association
		ON episode.master_episode_id=association.child_master_episode_id
		WHERE association.association_level=2;



CREATE TABLE level_2b AS
  SELECT episode.`member_id`,
         episode.master_episode_id AS LEVEL_1,
         episode.master_episode_id AS LEVEL_2
  FROM   episode
         LEFT JOIN association
                ON episode.master_episode_id =
                   association.child_master_episode_id
  WHERE  association.association_level <> 2
          OR association.association_level IS NULL; 


CREATE TABLE level_2 AS
  SELECT *
  FROM   level_2a
  UNION
  SELECT *
  FROM   level_2b;

DROP TABLE level_2a;

DROP TABLE level_2b; 



//

CREATE TABLE level_3a AS
  SELECT level_2.*,
         CASE
           WHEN association.association_level = 3 THEN
           association.parent_master_episode_id
           ELSE level_2.level_2
         end AS LEVEL_3
  FROM   level_2
         JOIN association
           ON level_2.level_2 = association.child_master_episode_id
  WHERE  association.association_level = 3;



CREATE TABLE level_3b AS
  SELECT level_2.*,
         level_2.level_2 AS LEVEL_3
  FROM   level_2
         LEFT JOIN association
                ON level_2.level_2 = association.child_master_episode_id
  WHERE  association.association_level <> 3
          OR association.association_level IS NULL; 


CREATE TABLE level_3 AS
  SELECT *
  FROM   level_3a
  UNION
  SELECT *
  FROM   level_3b;

DROP TABLE level_3a;

DROP TABLE level_3b;



//

CREATE TABLE level_4a AS
  SELECT level_3.*,
         CASE
           WHEN association.association_level = 4 THEN
           association.parent_master_episode_id
           ELSE level_3.level_3
         end AS LEVEL_4
  FROM   level_3
         JOIN association
           ON level_3.level_3 = association.child_master_episode_id
  WHERE  association.association_level = 4; 




CREATE TABLE level_4b AS
  SELECT level_3.*,
         level_3.level_3 AS LEVEL_4
  FROM   level_3
         LEFT JOIN association
                ON level_3.level_3 = association.child_master_episode_id
  WHERE  association.association_level <> 4
          OR association.association_level IS NULL;



CREATE TABLE level_4 AS
  SELECT *
  FROM   level_4a
  UNION
  SELECT *
  FROM   level_4b;

DROP TABLE level_4a;

DROP TABLE level_4b; 





//
CREATE TABLE level_5a AS
  SELECT level_4.*,
         CASE
           WHEN association.association_level = 5 THEN
           association.parent_master_episode_id
           ELSE level_4.level_4
         end AS LEVEL_5
  FROM   level_4
         JOIN association
           ON level_4.level_4 = association.child_master_episode_id
  WHERE  association.association_level = 5; 


CREATE TABLE level_5b AS
  SELECT level_4.*,
         level_4.level_4 AS LEVEL_5
  FROM   level_4
         LEFT JOIN association
                ON level_4.level_4 = association.child_master_episode_id
  WHERE  association.association_level <> 5
          OR association.association_level IS NULL; 


CREATE TABLE level_5 AS
  SELECT *
  FROM   level_5a
  UNION
  SELECT *
  FROM   level_5b;
  

DROP TABLE level_5a;

DROP TABLE level_5b;




/*********************************************/

DROP TABLE IF EXISTS assign_1;

DROP TABLE IF EXISTS assign_pac_totals;


CREATE TABLE assign_1 AS
  SELECT level_5.*,
         assignment.claim_source,
         assignment.assigned_type,
         assignment.master_claim_id
  FROM   level_5
         JOIN assignment
           ON level_5.level_1 = assignment.master_episode_id
  WHERE  assigned_type = 'C'; 



CREATE TABLE assign_pac_totals AS
  SELECT level_1                 AS EPISODE,
         '1'                     AS Level_Assignment,
         Count(DISTINCT ( CASE
                            WHEN claim_source = 'IP' THEN master_claim_id
                          end )) AS IP_PAC_Count,
         Count(DISTINCT ( CASE
                            WHEN claim_source = 'OP' THEN master_claim_id
                          end )) AS OP_PAC_Count,
         Count(DISTINCT ( CASE
                            WHEN claim_source = 'PB' THEN master_claim_id
                          end )) AS PB_PAC_Count,
         Count(DISTINCT ( CASE
                            WHEN claim_source = 'RX' THEN master_claim_id
                          end )) AS RX_PAC_Count
  FROM   assign_1
  GROUP  BY level_1
  UNION
  SELECT level_2                 AS EPISODE,
         '2'                     AS Level_Assignment,
         Count(DISTINCT ( CASE
                            WHEN claim_source = 'IP' THEN master_claim_id
                          end )) AS IP_PAC_Count,
         Count(DISTINCT ( CASE
                            WHEN claim_source = 'OP' THEN master_claim_id
                          end )) AS OP_PAC_Count,
         Count(DISTINCT ( CASE
                            WHEN claim_source = 'PB' THEN master_claim_id
                          end )) AS PB_PAC_Count,
         Count(DISTINCT ( CASE
                            WHEN claim_source = 'RX' THEN master_claim_id
                          end )) AS RX_PAC_Count
  FROM   assign_1
  GROUP  BY level_2
  UNION
  SELECT level_3                 AS EPISODE,
         '3'                     AS Level_Assignment,
         Count(DISTINCT ( CASE
                            WHEN claim_source = 'IP' THEN master_claim_id
                          end )) AS IP_PAC_Count,
         Count(DISTINCT ( CASE
                            WHEN claim_source = 'OP' THEN master_claim_id
                          end )) AS OP_PAC_Count,
         Count(DISTINCT ( CASE
                            WHEN claim_source = 'PB' THEN master_claim_id
                          end )) AS PB_PAC_Count,
         Count(DISTINCT ( CASE
                            WHEN claim_source = 'RX' THEN master_claim_id
                          end )) AS RX_PAC_Count
  FROM   assign_1
  GROUP  BY level_3
  UNION
  SELECT level_4                 AS EPISODE,
         '4'                     AS Level_Assignment,
         Count(DISTINCT ( CASE
                            WHEN claim_source = 'IP' THEN master_claim_id
                          end )) AS IP_PAC_Count,
         Count(DISTINCT ( CASE
                            WHEN claim_source = 'OP' THEN master_claim_id
                          end )) AS OP_PAC_Count,
         Count(DISTINCT ( CASE
                            WHEN claim_source = 'PB' THEN master_claim_id
                          end )) AS PB_PAC_Count,
         Count(DISTINCT ( CASE
                            WHEN claim_source = 'RX' THEN master_claim_id
                          end )) AS RX_PAC_Count
  FROM   assign_1
  GROUP  BY level_4
  UNION
  SELECT level_5                 AS EPISODE,
         '5'                     AS Level_Assignment,
         Count(DISTINCT ( CASE
                            WHEN claim_source = 'IP' THEN master_claim_id
                          end )) AS IP_PAC_Count,
         Count(DISTINCT ( CASE
                            WHEN claim_source = 'OP' THEN master_claim_id
                          end )) AS OP_PAC_Count,
         Count(DISTINCT ( CASE
                            WHEN claim_source = 'PB' THEN master_claim_id
                          end )) AS PB_PAC_Count,
         Count(DISTINCT ( CASE
                            WHEN claim_source = 'RX' THEN master_claim_id
                          end )) AS RX_PAC_Count
  FROM   assign_1
  GROUP  BY level_5; 



/* level tables no longer needed */
DROP TABLE IF EXISTS level_2;
DROP TABLE IF EXISTS level_3;
DROP TABLE IF EXISTS level_4; 
DROP TABLE IF EXISTS level_5; 



//CREATE INDEX redpac3 ON report_episode_detail(master_episode_id);  // <-- duplicate key

//CREATE INDEX redpac2 ON report_episode_detail(level);  // <-- duplicate key

//CREATE INDEX aptpac ON assign_pac_totals(episode);   // <-- duplicate key

//CREATE INDEX aptpac2 ON assign_pac_totals(level_assignment);  // <-- duplicate key



CREATE INDEX redpac3 ON report_episode_detail(master_episode_id);

CREATE INDEX redpac2 ON report_episode_detail(level);

CREATE INDEX aptpac ON assign_pac_totals(episode);

CREATE INDEX aptpac2 ON assign_pac_totals(level_assignment);

UPDATE report_episode_detail red
       JOIN assign_pac_totals p
         ON p.episode = red.master_episode_id
            AND p.level_assignment = red.level
SET    red.ip_pac_count = p.ip_pac_count,
       red.op_pac_count = p.op_pac_count,
       red.pb_pac_count = p.pb_pac_count,
       red.rx_pac_count = p.rx_pac_count; 




//add expected costs to report_episode_detail table for level 1 and the level at which the episode is complete

//add level 1 expected costs for chronic and other condition episodes to report_episode_detail

CREATE temporary TABLE IF NOT EXISTS x AS
  SELECT rec.epi_id AS master_episode_id,
         red.level,
         rec.total_exp_cost_sa_l1,
         rec.exp_cost_sa_typ_l1,
         rec.exp_cost_sa_comp_l1,
         rec.total_exp_cost_ra_l1,
         rec.exp_cost_ra_typ_l1,
         rec.exp_cost_ra_comp_l1
  FROM   ra_exp_cost rec
         LEFT JOIN report_episode_detail red
                ON rec.epi_id = red.master_episode_id
  WHERE  rec.exp_cost_sa_typ_l1 IS NOT NULL
         AND red.level = 1;

UPDATE report_episode_detail red
       JOIN x
         ON x.master_episode_id = red.master_episode_id
            AND x.level = red.level
SET    red.split_expected_total_cost = x.total_exp_cost_sa_l1,
       red.split_expected_typical_other_cost = x.exp_cost_sa_typ_l1,
       red.split_expected_pac_cost = x.exp_cost_sa_comp_l1,
       red.unsplit_expected_total_cost = x.total_exp_cost_ra_l1,
       red.unsplit_expected_typical_other_cost = x.exp_cost_ra_typ_l1,
       red.unsplit_expected_pac_cost=x.exp_cost_ra_comp_l1;

DROP TABLE x;


#add level 5 expected costs for chronic and other condition episodes to report_episode_detail

CREATE temporary TABLE IF NOT EXISTS x AS
  SELECT rec.epi_id AS master_episode_id,
         red.level,
         rec.total_exp_cost_sa_l5,
         rec.exp_cost_sa_typ_l5,
         rec.exp_cost_sa_comp_l5,
         rec.total_exp_cost_ra_l5,
         rec.exp_cost_ra_typ_l5,
         rec.exp_cost_ra_comp_l5
  FROM   ra_exp_cost rec
         LEFT JOIN report_episode_detail red
                ON rec.epi_id = red.master_episode_id
  WHERE  rec.exp_cost_sa_typ_l5 IS NOT NULL
         AND red.level = 5;

UPDATE report_episode_detail red
       JOIN x
         ON x.master_episode_id = red.master_episode_id
            AND x.level = red.level
SET    red.split_expected_total_cost = x.total_exp_cost_sa_l5,
       red.split_expected_typical_other_cost = x.exp_cost_sa_typ_l5,
       red.split_expected_pac_cost = x.exp_cost_sa_comp_l5,
       red.unsplit_expected_total_cost = x.total_exp_cost_ra_l5,
       red.unsplit_expected_typical_other_cost = x.exp_cost_ra_typ_l5,
       red.unsplit_expected_pac_cost = x.exp_cost_ra_comp_l5;

DROP TABLE x; 



//add level 1 expected costs for acute and procedural episodes to report_episode_detail

CREATE temporary TABLE IF NOT EXISTS x AS
  SELECT rec.epi_id AS master_episode_id,
         red.level,
         rec.total_exp_cost_sa_l1,
         rec.exp_cost_sa_typ_ip_l1,
         rec.exp_cost_sa_typ_other_l1,
         rec.exp_cost_sa_comp_other_l1,
         rec.total_exp_cost_ra_l1,
         rec.exp_cost_ra_typ_ip_l1,
         rec.exp_cost_ra_typ_other_l1,
         rec.exp_cost_ra_comp_other_l1
  FROM   ra_exp_cost rec
         LEFT JOIN report_episode_detail red
                ON rec.epi_id = red.master_episode_id
  WHERE  rec.exp_cost_sa_typ_ip_l1 IS NOT NULL
         AND red.level = 1; 

UPDATE report_episode_detail red
       JOIN x
         ON x.master_episode_id = red.master_episode_id
            AND x.level = red.level
SET    red.split_expected_total_cost = x.total_exp_cost_sa_l1,
       red.split_expected_typical_ip_cost = x.exp_cost_sa_typ_ip_l1,
       red.split_expected_typical_other_cost = x.exp_cost_sa_typ_other_l1,
       red.split_expected_pac_cost = x.exp_cost_sa_comp_other_l1,
       red.unsplit_expected_total_cost = x.total_exp_cost_ra_l1,
       red.unsplit_expected_typical_ip_cost = x.exp_cost_ra_typ_ip_l1,
       red.unsplit_expected_typical_other_cost = x.exp_cost_ra_typ_other_l1,
       red.unsplit_expected_pac_cost = x.exp_cost_ra_comp_other_l1; 



DROP TABLE x;

//add level 3 expected costs for procedural episodes to report_episode_detail

CREATE temporary TABLE IF NOT EXISTS x AS
  SELECT rec.epi_id AS master_episode_id,
         red.level,
         rec.total_exp_cost_sa_l3,
         rec.exp_cost_sa_typ_ip_l3,
         rec.exp_cost_sa_typ_other_l3,
         rec.exp_cost_sa_comp_other_l3,
         rec.total_exp_cost_ra_l3,
         rec.exp_cost_ra_typ_ip_l3,
         rec.exp_cost_ra_typ_other_l3,
         rec.exp_cost_ra_comp_other_l3
  FROM   ra_exp_cost rec
         LEFT JOIN report_episode_detail red
                ON rec.epi_id = red.master_episode_id
  WHERE  rec.exp_cost_sa_typ_ip_l3 IS NOT NULL
         AND red.level = 3; 



UPDATE report_episode_detail red
       JOIN x
         ON x.master_episode_id = red.master_episode_id
            AND x.level = red.level
SET    red.split_expected_total_cost = x.total_exp_cost_sa_l3,
       red.split_expected_typical_ip_cost = x.exp_cost_sa_typ_ip_l3,
       red.split_expected_typical_other_cost = x.exp_cost_sa_typ_other_l3,
       red.split_expected_pac_cost = x.exp_cost_sa_comp_other_l3,
       red.unsplit_expected_total_cost = x.total_exp_cost_ra_l3,
       red.unsplit_expected_typical_ip_cost = x.exp_cost_ra_typ_ip_l3,
       red.unsplit_expected_typical_other_cost = x.exp_cost_ra_typ_other_l3,
       red.unsplit_expected_pac_cost = x.exp_cost_ra_comp_other_l3; 



DROP TABLE x;


//add level 4 expected costs for acute episodes to report_episode_detail

CREATE temporary TABLE IF NOT EXISTS x AS
  SELECT rec.epi_id AS master_episode_id,
         red.level,
         rec.total_exp_cost_sa_l4,
         rec.exp_cost_sa_typ_ip_l4,
         rec.exp_cost_sa_typ_other_l4,
         rec.exp_cost_sa_comp_other_l4,
         rec.total_exp_cost_ra_l4,
         rec.exp_cost_ra_typ_ip_l4,
         rec.exp_cost_ra_typ_other_l4,
         rec.exp_cost_ra_comp_other_l4
  FROM   ra_exp_cost rec
         LEFT JOIN report_episode_detail red
                ON rec.epi_id = red.master_episode_id
  WHERE  rec.exp_cost_sa_typ_ip_l4 IS NOT NULL
         AND red.level = 4; 



UPDATE report_episode_detail red
       JOIN x
         ON x.master_episode_id = red.master_episode_id
            AND x.level = red.level
SET    red.split_expected_total_cost = x.total_exp_cost_sa_l4,
       red.split_expected_typical_ip_cost = x.exp_cost_sa_typ_ip_l4,
       red.split_expected_typical_other_cost = x.exp_cost_sa_typ_other_l4,
       red.split_expected_pac_cost = x.exp_cost_sa_comp_other_l4,
       red.unsplit_expected_total_cost = x.total_exp_cost_ra_l4,
       red.unsplit_expected_typical_ip_cost = x.exp_cost_ra_typ_ip_l4,
       red.unsplit_expected_typical_other_cost = x.exp_cost_ra_typ_other_l4,
       red.unsplit_expected_pac_cost = x.exp_cost_ra_comp_other_l4; 

DROP TABLE x;



//compare actual vs. expected

SELECT red.filter_id,
       red.episode_id,
       red.episode_name,
       red.episode_type,
       red.level,
       Count(*)                                            AS episode_volume,
       Sum(red.split_total_cost),
       Sum(red.annualized_split_total_cost),
       Sum(red.split_expected_total_cost),
       ( Sum(red.split_total_cost) ) / Count(*)            AS
       average_split_costs,
       ( Sum(red.annualized_split_total_cost) ) / Count(*) AS
       average_annualized_split_costs,
       ( Sum(red.split_expected_total_cost) ) / Count(*)   AS
       average_expected_split_costs
FROM   report_episode_detail red
WHERE  red.episode_type <> 'System Related Failure'
GROUP  BY red.filter_id,
          red.episode_id,
          red.level
ORDER  BY red.level,
          red.episode_id ;


