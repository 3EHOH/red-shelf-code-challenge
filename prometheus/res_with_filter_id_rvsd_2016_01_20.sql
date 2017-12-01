




# Get current metadata scehema and assign it to the eb variable
set @eb = (SELECT max(SCHEMA_NAME) AS `Database`
  FROM INFORMATION_SCHEMA.SCHEMATA
  where SCHEMA_NAME like 'episode_builder%');

drop table if exists sub_distinct;
create table sub_distinct as
select child_master_episode_id,
min(association_level) as association_level
from association
group by child_master_episode_id;

create index sd on sub_distinct(child_master_episode_id);
create index sdl on sub_distinct(association_level);

drop table report_episode_summary;

# Get current metadata scehema and assign it to the eb variable
set @eb = (SELECT max(SCHEMA_NAME) AS `Database`
  FROM INFORMATION_SCHEMA.SCHEMATA
  where SCHEMA_NAME like 'episode_builder%');

DROP TABLE IF EXISTS report_episode_summary;

CREATE TABLE `report_episode_summary` (
  `Filter_ID` int(11) DEFAULT NULL,
  `Episode_ID` varchar(6) DEFAULT NULL,
  `Episode_Name` varchar(6) DEFAULT NULL,
  `Episode_Description` varchar(255) DEFAULT NULL,
  `Episode_Type` varchar(50) DEFAULT NULL,
  `MDC` int(2) DEFAULT NULL,
  `MDC_Description` varchar(255) DEFAULT NULL,
  `Level` int(1) DEFAULT NULL,
  `Episode_Volume` int(11) DEFAULT NULL,
  `Split_Total_Cost` decimal(13,2) DEFAULT NULL,
  `Split_Average_Cost` decimal(13,2) DEFAULT NULL,
  `Split_1stPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Split_99thPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Split_Min_Cost` decimal(13,2) DEFAULT NULL,
  `Split_Max_Cost` decimal(13,2) DEFAULT NULL,
  `Split_STDEV` decimal(13,2) DEFAULT NULL,
  `Split_CV` decimal(13,2) DEFAULT NULL,
  `Split_Total_PAC_Cost` decimal(13,2) DEFAULT NULL,
  `Split_Average_PAC_Cost` decimal(13,2) DEFAULT NULL,
  `Split_PAC_Percent` decimal(5,2) DEFAULT NULL,
  `Split_Total_Typical_Cost` decimal(13,2) DEFAULT NULL,
  `Split_Average_Typical_Cost` decimal(13,2) DEFAULT NULL,
  `Split_Total_TypicalwPAC_Cost` decimal(13,2) DEFAULT NULL,
  `Split_Average_TypicalwPAC_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_Total_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_Average_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_1stPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_99thPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_Min_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_Max_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_STDEV` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_CV` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_Total_PAC_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_Average_PAC_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_PAC_Percent` decimal(5,2) DEFAULT NULL,
  `Annualized_Split_Total_Typical_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_Average_Typical_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_Total_TypicalwPAC_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Split_Average_TypicalwPAC_Cost` decimal(13,2) DEFAULT NULL,
  `Unsplit_Total_Cost` decimal(13,2) DEFAULT NULL,
  `Unsplit_Average_Cost` decimal(13,2) DEFAULT NULL,
  `Unsplit_1stPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Unsplit_99thPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Unsplit_Min_Cost` decimal(13,2) DEFAULT NULL,
  `Unsplit_Max_Cost` decimal(13,2) DEFAULT NULL,
  `Unsplit_STDEV` decimal(13,2) DEFAULT NULL,
  `Unsplit_CV` decimal(13,2) DEFAULT NULL,
  `Unsplit_Total_PAC_Cost` decimal(13,2) DEFAULT NULL,
  `Unsplit_Average_PAC_Cost` decimal(13,2) DEFAULT NULL,
  `Unsplit_PAC_Percent` decimal(5,2) DEFAULT NULL,
  `Unsplit_Total_Typical_Cost` decimal(13,2) DEFAULT NULL,
  `Unsplit_Average_Typical_Cost` decimal(13,2) DEFAULT NULL,
  `Unsplit_Total_TypicalwPAC_Cost` decimal(13,2) DEFAULT NULL,
  `Unsplit_Average_TypicalwPAC_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_Total_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_Average_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_1stPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_99thPercentile_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_Min_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_Max_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_STDEV` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_CV` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_Total_PAC_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_Average_PAC_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_PAC_Percent` decimal(5,2) DEFAULT NULL,
  `Annualized_Unsplit_Total_Typical_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_Average_Typical_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_Total_TypicalwPAC_Cost` decimal(13,2) DEFAULT NULL,
  `Annualized_Unsplit_Average_TypicalwPAC_Cost` decimal(13,2) DEFAULT NULL,
  `Expected_Split_Average_Cost` decimal(13,2) DEFAULT NULL,
  `Expected_Split_Typical_IP_Average_Cost` decimal(13,2) DEFAULT NULL,
  `Expected_Split_Typical_Other_Average_Cost` decimal(13,2) DEFAULT NULL,
  `Expected_Split_PAC_Average_Cost` decimal(13,2) DEFAULT NULL,
  `Expected_Unsplit_Average_Cost` decimal(13,2) DEFAULT NULL,
  `Expected_Unsplit_Typical_IP_Average_Cost` decimal(13,2) DEFAULT NULL,
  `Expected_Unsplit_Typical_Other_Average_Cost` decimal(13,2) DEFAULT NULL,
  `Expected_Unsplit_PAC_Average_Cost` decimal(13,2) DEFAULT NULL,
  KEY `Episode_ID` (`Episode_ID`),
  KEY `Level` (`Level`)
) ;

#1
set @s = CONCAT('
insert into report_episode_summary
(Filter_ID, Episode_ID, Episode_Name, Episode_Description, Episode_Type, MDC, MDC_Description, Level, Episode_Volume, Split_Total_Cost, Split_Average_Cost, Split_Min_Cost, Split_Max_Cost, Split_STDEV, Split_CV, Split_Total_PAC_Cost, Split_Average_PAC_Cost, Split_PAC_Percent, Split_Total_Typical_Cost, Split_Average_Typical_Cost, Split_Total_TypicalwPAC_Cost, Split_Average_TypicalwPAC_Cost)

SELECT
master_epid_level.filter_id,
left(master_epid_level.master_episode_id,6),
eb.NAME,
eb.DESCRIPTION,
eb.TYPE,
eb.MDC_CATEGORY,
mdc_desc.mdc_description,
master_epid_level.level,
COUNT(*),
SUM(master_epid_level.cost),
SUM(master_epid_level.cost)/COUNT(*),
MIN(master_epid_level.cost),
MAX(master_epid_level.cost),
STD(master_epid_level.cost),
STD(master_epid_level.cost)/(SUM(master_epid_level.cost)/COUNT(*)),
SUM(master_epid_level.cost_c),
SUM(master_epid_level.cost_c)/COUNT(*),
(SUM(master_epid_level.cost_c)/SUM(master_epid_level.cost))*100,
SUM(master_epid_level.cost_t),
SUM(master_epid_level.cost_t)/COUNT(*),
SUM(master_epid_level.cost_tc),
SUM(master_epid_level.cost_tc)/COUNT(*)

FROM `',@eb,'`.episode eb
join master_epid_level
on left(master_epid_level.master_episode_id,6) = eb.EPISODE_ID
left join mdc_desc
on eb.MDC_CATEGORY=mdc_desc.mdc



left join sub_distinct
on master_epid_level.master_episode_id=sub_distinct.child_master_episode_id 

left join sub_distinct_filter
on master_epid_level.master_episode_id=sub_distinct_filter.child_master_episode_id 

where master_epid_level.level<sub_distinct.association_level and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=0

AND master_epid_level.level=1
or  sub_distinct.association_level is null  and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0  and master_epid_level.filter_id=0

AND master_epid_level.level=1

or master_epid_level.level<sub_distinct_filter.association_level and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1 
AND master_epid_level.level=1
or  sub_distinct_filter.association_level is null  and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1

AND master_epid_level.level=1

GROUP BY left(master_epid_level.master_episode_id,6), master_epid_level.level, master_epid_level.filter_id

');

PREPARE stmt1 FROM @s;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;


#2
set @s = CONCAT('
insert into report_episode_summary
(Filter_ID, Episode_ID, Episode_Name, Episode_Description, Episode_Type, MDC, MDC_Description, Level, Episode_Volume, Split_Total_Cost, Split_Average_Cost, Split_Min_Cost, Split_Max_Cost, Split_STDEV, Split_CV, Split_Total_PAC_Cost, Split_Average_PAC_Cost, Split_PAC_Percent, Split_Total_Typical_Cost, Split_Average_Typical_Cost, Split_Total_TypicalwPAC_Cost, Split_Average_TypicalwPAC_Cost)

SELECT
master_epid_level.filter_id,
left(master_epid_level.master_episode_id,6),
eb.NAME,
eb.DESCRIPTION,
eb.TYPE,
eb.MDC_CATEGORY,
mdc_desc.mdc_description,
master_epid_level.level,
COUNT(*),
SUM(master_epid_level.cost),
SUM(master_epid_level.cost)/COUNT(*),
MIN(master_epid_level.cost),
MAX(master_epid_level.cost),
STD(master_epid_level.cost),
STD(master_epid_level.cost)/(SUM(master_epid_level.cost)/COUNT(*)),
SUM(master_epid_level.cost_c),
SUM(master_epid_level.cost_c)/COUNT(*),
(SUM(master_epid_level.cost_c)/SUM(master_epid_level.cost))*100,
SUM(master_epid_level.cost_t),
SUM(master_epid_level.cost_t)/COUNT(*),
SUM(master_epid_level.cost_tc),
SUM(master_epid_level.cost_tc)/COUNT(*)

FROM `',@eb,'`.episode eb
join master_epid_level
on left(master_epid_level.master_episode_id,6) = eb.EPISODE_ID
left join mdc_desc
on eb.MDC_CATEGORY=mdc_desc.mdc
left join sub_distinct
on master_epid_level.master_episode_id=sub_distinct.child_master_episode_id 

left join sub_distinct_filter
on master_epid_level.master_episode_id=sub_distinct_filter.child_master_episode_id 

where master_epid_level.level<sub_distinct.association_level and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=0 

AND master_epid_level.level=2
or  sub_distinct.association_level is null  and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0  and master_epid_level.filter_id=0

AND master_epid_level.level=2

or master_epid_level.level<sub_distinct_filter.association_level and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1 
AND master_epid_level.level=2
or  sub_distinct_filter.association_level is null  and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1

AND master_epid_level.level=2
GROUP BY left(master_epid_level.master_episode_id,6), master_epid_level.level, master_epid_level.filter_id
');

PREPARE stmt1 FROM @s;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;


#3
set @s = CONCAT('
insert into report_episode_summary
(Filter_ID, Episode_ID, Episode_Name, Episode_Description, Episode_Type, MDC, MDC_Description, Level, Episode_Volume, Split_Total_Cost, Split_Average_Cost, Split_Min_Cost, Split_Max_Cost, Split_STDEV, Split_CV, Split_Total_PAC_Cost, Split_Average_PAC_Cost, Split_PAC_Percent, Split_Total_Typical_Cost, Split_Average_Typical_Cost, Split_Total_TypicalwPAC_Cost, Split_Average_TypicalwPAC_Cost)

SELECT
master_epid_level.filter_id,
left(master_epid_level.master_episode_id,6),
eb.NAME,
eb.DESCRIPTION,
eb.TYPE,
eb.MDC_CATEGORY,
mdc_desc.mdc_description,
master_epid_level.level,
COUNT(*),
SUM(master_epid_level.cost),
SUM(master_epid_level.cost)/COUNT(*),
MIN(master_epid_level.cost),
MAX(master_epid_level.cost),
STD(master_epid_level.cost),
STD(master_epid_level.cost)/(SUM(master_epid_level.cost)/COUNT(*)),
SUM(master_epid_level.cost_c),
SUM(master_epid_level.cost_c)/COUNT(*),
(SUM(master_epid_level.cost_c)/SUM(master_epid_level.cost))*100,
SUM(master_epid_level.cost_t),
SUM(master_epid_level.cost_t)/COUNT(*),
SUM(master_epid_level.cost_tc),
SUM(master_epid_level.cost_tc)/COUNT(*)

FROM `',@eb,'`.episode eb
join master_epid_level
on left(master_epid_level.master_episode_id,6) = eb.EPISODE_ID
left join mdc_desc
on eb.MDC_CATEGORY=mdc_desc.mdc
left join sub_distinct
on master_epid_level.master_episode_id=sub_distinct.child_master_episode_id 

left join sub_distinct_filter
on master_epid_level.master_episode_id=sub_distinct_filter.child_master_episode_id 

where master_epid_level.level<sub_distinct.association_level and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=0

AND master_epid_level.level=3
or  sub_distinct.association_level is null  and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0  and master_epid_level.filter_id=0

AND master_epid_level.level=3

or master_epid_level.level<sub_distinct_filter.association_level and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1 
AND master_epid_level.level=3
or  sub_distinct_filter.association_level is null  and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1

AND master_epid_level.level=3
GROUP BY left(master_epid_level.master_episode_id,6), master_epid_level.level, master_epid_level.filter_id
');

PREPARE stmt1 FROM @s;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

#4
set @s = CONCAT('
insert into report_episode_summary
(Filter_ID, Episode_ID, Episode_Name, Episode_Description, Episode_Type, MDC, MDC_Description, Level, Episode_Volume, Split_Total_Cost, Split_Average_Cost, Split_Min_Cost, Split_Max_Cost, Split_STDEV, Split_CV, Split_Total_PAC_Cost, Split_Average_PAC_Cost, Split_PAC_Percent, Split_Total_Typical_Cost, Split_Average_Typical_Cost, Split_Total_TypicalwPAC_Cost, Split_Average_TypicalwPAC_Cost)

SELECT
master_epid_level.filter_id,
left(master_epid_level.master_episode_id,6),
eb.NAME,
eb.DESCRIPTION,
eb.TYPE,
eb.MDC_CATEGORY,
mdc_desc.mdc_description,
master_epid_level.level,
COUNT(*),
SUM(master_epid_level.cost),
SUM(master_epid_level.cost)/COUNT(*),
MIN(master_epid_level.cost),
MAX(master_epid_level.cost),
STD(master_epid_level.cost),
STD(master_epid_level.cost)/(SUM(master_epid_level.cost)/COUNT(*)),
SUM(master_epid_level.cost_c),
SUM(master_epid_level.cost_c)/COUNT(*),
(SUM(master_epid_level.cost_c)/SUM(master_epid_level.cost))*100,
SUM(master_epid_level.cost_t),
SUM(master_epid_level.cost_t)/COUNT(*),
SUM(master_epid_level.cost_tc),
SUM(master_epid_level.cost_tc)/COUNT(*)


FROM `',@eb,'`.episode eb
join master_epid_level
on left(master_epid_level.master_episode_id,6) = eb.EPISODE_ID
left join mdc_desc
on eb.MDC_CATEGORY=mdc_desc.mdc
left join sub_distinct
on master_epid_level.master_episode_id=sub_distinct.child_master_episode_id 

left join sub_distinct_filter
on master_epid_level.master_episode_id=sub_distinct_filter.child_master_episode_id 

where master_epid_level.level<sub_distinct.association_level and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=0

AND master_epid_level.level=4
or  sub_distinct.association_level is null  and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0  and master_epid_level.filter_id=0

AND master_epid_level.level=4

or master_epid_level.level<sub_distinct_filter.association_level and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1 
AND master_epid_level.level=4
or  sub_distinct_filter.association_level is null  and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1

AND master_epid_level.level=4
GROUP BY left(master_epid_level.master_episode_id,6), master_epid_level.level, master_epid_level.filter_id

');

PREPARE stmt1 FROM @s;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;

#5
set @s = CONCAT('
insert into report_episode_summary
(Filter_ID, Episode_ID, Episode_Name, Episode_Description, Episode_Type, MDC, MDC_Description, Level, Episode_Volume, Split_Total_Cost, Split_Average_Cost, Split_Min_Cost, Split_Max_Cost, Split_STDEV, Split_CV, Split_Total_PAC_Cost, Split_Average_PAC_Cost, Split_PAC_Percent, Split_Total_Typical_Cost, Split_Average_Typical_Cost, Split_Total_TypicalwPAC_Cost, Split_Average_TypicalwPAC_Cost)

SELECT
master_epid_level.filter_id,
left(master_epid_level.master_episode_id,6),
eb.NAME,
eb.DESCRIPTION,
eb.TYPE,
eb.MDC_CATEGORY,
mdc_desc.mdc_description,
master_epid_level.level,
COUNT(*),
SUM(master_epid_level.cost),
SUM(master_epid_level.cost)/COUNT(*),
MIN(master_epid_level.cost),
MAX(master_epid_level.cost),
STD(master_epid_level.cost),
STD(master_epid_level.cost)/(SUM(master_epid_level.cost)/COUNT(*)),
SUM(master_epid_level.cost_c),
SUM(master_epid_level.cost_c)/COUNT(*),
(SUM(master_epid_level.cost_c)/SUM(master_epid_level.cost))*100,
SUM(master_epid_level.cost_t),
SUM(master_epid_level.cost_t)/COUNT(*),
SUM(master_epid_level.cost_tc),
SUM(master_epid_level.cost_tc)/COUNT(*)


FROM `',@eb,'`.episode eb
join master_epid_level
on left(master_epid_level.master_episode_id,6) = eb.EPISODE_ID
left join mdc_desc
on eb.MDC_CATEGORY=mdc_desc.mdc
left join sub_distinct
on master_epid_level.master_episode_id=sub_distinct.child_master_episode_id 

left join sub_distinct_filter
on master_epid_level.master_episode_id=sub_distinct_filter.child_master_episode_id 

where master_epid_level.level<sub_distinct.association_level and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=0

AND master_epid_level.level=5
or  sub_distinct.association_level is null  and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0  and master_epid_level.filter_id=0

AND master_epid_level.level=5

or master_epid_level.level<sub_distinct_filter.association_level and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1 
AND master_epid_level.level=5
or  sub_distinct_filter.association_level is null  and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1

AND master_epid_level.level=5
GROUP BY left(master_epid_level.master_episode_id,6), master_epid_level.level, master_epid_level.filter_id

');

PREPARE stmt1 FROM @s;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;


CREATE TEMPORARY TABLE IF NOT EXISTS melSum AS
SELECT
filter_id,
left(master_episode_id,6) as episode_id,
level,
SUM(cost) AS 'cost',
SUM(cost_t) AS cost_t,
SUM(cost_tc) AS cost_tc,
SUM(cost_c) AS cost_c,
MIN(cost) AS min_cost,
MAX(cost) AS max_cost,
STD(cost) AS std_cost,
count(*) AS episode_count
FROM master_epid_level
WHERE claim_type='CL'
AND split=0
AND annualized=0
GROUP BY filter_id, left(master_episode_id,6),  level;

UPDATE report_episode_summary res
JOIN melSum mel ON mel.episode_id=res.episode_id
AND mel.level=res.level
and mel.filter_id=res.filter_id
SET
res.Unsplit_Total_Cost=mel.cost,
res.Unsplit_Average_Cost=mel.cost/Episode_Volume,
res.Unsplit_Min_Cost=mel.min_cost,
res.Unsplit_Max_Cost=mel.max_cost,
res.Unsplit_STDEV=mel.std_cost,
res.Unsplit_CV=mel.std_cost/(mel.cost/Episode_Volume),
res.Unsplit_Total_PAC_Cost=mel.cost_c,
res.Unsplit_Average_PAC_Cost=mel.cost_c/Episode_Volume,
res.Unsplit_PAC_Percent=mel.cost_c/mel.cost*100,
res.Unsplit_Total_Typical_Cost=mel.cost_t,
res.Unsplit_Average_Typical_Cost=mel.cost_t/Episode_Volume,
res.Unsplit_Total_TypicalwPAC_Cost=mel.cost_tc,
res.Unsplit_Average_TypicalwPAC_Cost=mel.cost_tc/Episode_Volume;

DROP TABLE melSum;


CREATE TEMPORARY TABLE IF NOT EXISTS melSum AS
SELECT
filter_id,
left(master_episode_id,6) as episode_id,
level,
SUM(cost) AS 'cost',
SUM(cost_t) AS cost_t,
SUM(cost_tc) AS cost_tc,
SUM(cost_c) AS cost_c,
MIN(cost) AS min_cost,
MAX(cost) AS max_cost,
STD(cost) AS std_cost,
count(*) AS episode_count
FROM master_epid_level
WHERE claim_type='CL'
AND split=1
AND annualized=1 and left(master_episode_id,2)='EC'

or  claim_type='CL'
AND split=1
AND annualized=1 and left(master_episode_id,6)='EX9901'
GROUP BY filter_id, left(master_episode_id,6),  level;

UPDATE report_episode_summary res
JOIN melSum mel ON mel.episode_id=res.episode_id
AND mel.level=res.level
and mel.filter_id=res.filter_id
SET
res.Annualized_Split_Total_Cost=mel.cost,
res.Annualized_Split_Average_Cost=mel.cost/Episode_Volume,
res.Annualized_Split_Min_Cost=mel.min_cost,
res.Annualized_Split_Max_Cost=mel.max_cost,
res.Annualized_Split_STDEV=mel.std_cost,
res.Annualized_Split_CV=mel.std_cost/(mel.cost/Episode_Volume),
res.Annualized_Split_Total_PAC_Cost=mel.cost_c,
res.Annualized_Split_Average_PAC_Cost=mel.cost_c/Episode_Volume,
res.Annualized_Split_PAC_Percent=mel.cost_c/mel.cost*100,
res.Annualized_Split_Total_Typical_Cost=mel.cost_t,
res.Annualized_Split_Average_Typical_Cost=mel.cost_t/Episode_Volume,
res.Annualized_Split_Total_TypicalwPAC_Cost=mel.cost_tc,
res.Annualized_Split_Average_TypicalwPAC_Cost=mel.cost_tc/Episode_Volume;

DROP TABLE melSum;


CREATE TEMPORARY TABLE IF NOT EXISTS melSum AS
SELECT
filter_id,
left(master_episode_id,6) as episode_id,
level,
SUM(cost) AS 'cost',
SUM(cost_t) AS cost_t,
SUM(cost_tc) AS cost_tc,
SUM(cost_c) AS cost_c,
MIN(cost) AS min_cost,
MAX(cost) AS max_cost,
STD(cost) AS std_cost,
count(*) AS episode_count
FROM master_epid_level
WHERE claim_type='CL'
AND split=0
AND annualized=1 and left(master_episode_id,2)='EC'

or  claim_type='CL'
AND split=0
AND annualized=1 and left(master_episode_id,6)='EX9901'
GROUP BY filter_id, left(master_episode_id,6),  level;

UPDATE report_episode_summary res
JOIN melSum mel ON mel.episode_id=res.episode_id
AND mel.level=res.level
and mel.filter_id=res.filter_id
SET
res.Annualized_Unsplit_Total_Cost=mel.cost,
res.Annualized_Unsplit_Average_Cost=mel.cost/Episode_Volume,
res.Annualized_Unsplit_Min_Cost=mel.min_cost,
res.Annualized_Unsplit_Max_Cost=mel.max_cost,
res.Annualized_Unsplit_STDEV=mel.std_cost,
res.Annualized_Unsplit_CV=mel.std_cost/(mel.cost/Episode_Volume),
res.Annualized_Unsplit_Total_PAC_Cost=mel.cost_c,
res.Annualized_Unsplit_Average_PAC_Cost=mel.cost_c/Episode_Volume,
res.Annualized_Unsplit_PAC_Percent=mel.cost_c/mel.cost*100,
res.Annualized_Unsplit_Total_Typical_Cost=mel.cost_t,
res.Annualized_Unsplit_Average_Typical_Cost=mel.cost_t/Episode_Volume,
res.Annualized_Unsplit_Total_TypicalwPAC_Cost=mel.cost_tc,
res.Annualized_Unsplit_Average_TypicalwPAC_Cost=mel.cost_tc/Episode_Volume;

DROP TABLE melSum;


SELECT *
FROM percentiles p;

UPDATE report_episode_summary res
JOIN percentiles p ON p.episode_id=res.episode_id
AND p.level=res.level
AND p.filter_id=res.filter_id
SET
res.Split_1stPercentile_Cost=p.Split_1stPercentile_Cost,
res.Split_99thPercentile_Cost=p.Split_99thPercentile_Cost,
res.Annualized_Split_1stPercentile_Cost=p.Annualized_Split_1stPercentile_Cost,
res.Annualized_Split_99thPercentile_Cost=p.Annualized_Split_99thPercentile_Cost,
res.Unsplit_1stPercentile_Cost=p.Unsplit_1stPercentile_Cost,
res.Unsplit_99thPercentile_Cost=p.Unsplit_99thPercentile_Cost,
res.Annualized_Unsplit_1stPercentile_Cost=p.Annualized_Unsplit_1stPercentile_Cost,
res.Annualized_Unsplit_99thPercentile_Cost=p.Annualized_Unsplit_99thPercentile_Cost;

##########NEED TO TEST THIS SECTION###################

#add expected costs to report_episode_summary table for level 1 and the level at which the episode is complete

#add level 1 expected costs for chronic and other condition episodes to report_episode_summary

create temporary table if not exists x as

select
res.episode_id,
res.level,
res.filter_id,
count(*) as episode_count,
sum(rec.total_exp_cost_sa_l1) as total_split,
sum(rec.exp_cost_sa_typ_l1) as t_split,
sum(rec.exp_cost_sa_comp_l1) as c_split,
sum(rec.total_exp_cost_ra_l1) as total_unsplit,
sum(rec.exp_cost_ra_typ_l1) as t_unsplit,
sum(rec.exp_cost_ra_comp_l1) as c_unsplit

from ra_exp_cost rec
left join report_episode_summary res
on rec.epi_number=res.episode_id
where rec.exp_cost_sa_typ_l1 is not null
and res.level=1
group by res.filter_id, rec.epi_number;

update report_episode_summary res
join x
on x.episode_id=res.episode_id
and x.level=res.level
and x.filter_id=res.filter_id
set
res.Expected_Split_Average_Cost=x.total_split/Episode_Volume,
res.Expected_Split_Typical_Other_Average_Cost=x.t_split/Episode_Volume,
res.Expected_Split_PAC_Average_Cost=x.c_split/Episode_Volume,
res.Expected_Unsplit_Average_Cost=x.total_unsplit/Episode_Volume,
res.Expected_Unsplit_Typical_Other_Average_Cost=x.t_unsplit/Episode_Volume,
res.Expected_Unsplit_PAC_Average_Cost=x.c_unsplit/Episode_Volume;

drop table x;

#add level 5 expected costs for chronic and other condition episodes to report_episode_summary

create temporary table if not exists x as

select
res.episode_id,
res.level,
res.filter_id,
count(*) as episode_count,
sum(rec.total_exp_cost_sa_l5) as total_split,
sum(rec.exp_cost_sa_typ_l5) as t_split,
sum(rec.exp_cost_sa_comp_l5) as c_split,
sum(rec.total_exp_cost_ra_l5) as total_unsplit,
sum(rec.exp_cost_ra_typ_l5) as t_unsplit,
sum(rec.exp_cost_ra_comp_l5) as c_unsplit

from ra_exp_cost rec
left join report_episode_summary res
on rec.epi_number=res.episode_id
where rec.exp_cost_sa_typ_l5 is not null
and res.level=5
group by res.filter_id, rec.epi_number;

update report_episode_summary res
join x
on x.episode_id=res.episode_id
and x.level=res.level
and x.filter_id=res.filter_id
set
res.Expected_Split_Average_Cost=x.total_split/Episode_Volume,
res.Expected_Split_Typical_Other_Average_Cost=x.t_split/Episode_Volume,
res.Expected_Split_PAC_Average_Cost=x.c_split/Episode_Volume,
res.Expected_Unsplit_Average_Cost=x.total_unsplit/Episode_Volume,
res.Expected_Unsplit_Typical_Other_Average_Cost=x.t_unsplit/Episode_Volume,
res.Expected_Unsplit_PAC_Average_Cost=x.c_unsplit/Episode_Volume;


drop table x;

#add level 1 expected costs for acute and procedural episodes to report_episode_summary

create temporary table if not exists x as

select
res.episode_id,
res.level,
res.filter_id,
count(*) as episode_count,
sum(rec.total_exp_cost_sa_l1) as total_split,
sum(rec.exp_cost_sa_typ_ip_l1) as t_ip_split,
sum(rec.exp_cost_sa_typ_other_l1) as t_o_split,
sum(rec.exp_cost_sa_comp_other_l1) as c_split,
sum(rec.total_exp_cost_ra_l1) as total_unsplit,
sum(rec.exp_cost_ra_typ_ip_l1) as t_ip_unsplit,
sum(rec.exp_cost_ra_typ_other_l1) as t_o_unsplit,
sum(rec.exp_cost_ra_comp_other_l1) as c_unsplit

from ra_exp_cost rec
left join report_episode_summary res
on rec.epi_number=res.episode_id
where rec.exp_cost_sa_typ_ip_l1 is not null
and res.level=1
group by res.filter_id, rec.epi_number;

update report_episode_summary res
join x
on x.episode_id=res.episode_id
and x.level=res.level
and x.filter_id=res.filter_id
set
res.Expected_Split_Average_Cost=x.total_split/Episode_Volume,
res.Expected_Split_Typical_IP_Average_Cost=x.t_ip_split/Episode_Volume,
res.Expected_Split_Typical_Other_Average_Cost=x.t_o_split/Episode_Volume,
res.Expected_Split_PAC_Average_Cost=x.c_split/Episode_Volume,
res.Expected_Unsplit_Average_Cost=x.total_unsplit/Episode_Volume,
res.Expected_Unsplit_Typical_IP_Average_Cost=x.t_ip_unsplit/Episode_Volume,
res.Expected_Unsplit_Typical_Other_Average_Cost=x.t_o_unsplit/Episode_Volume,
res.Expected_Unsplit_PAC_Average_Cost=x.c_unsplit/Episode_Volume;

drop table x;

#add level 3 expected costs for procedural episodes to report_episode_summary

create temporary table if not exists x as

select
res.episode_id,
res.level,
res.filter_id,
count(*) as episode_count,
sum(rec.total_exp_cost_sa_l3) as total_split,
sum(rec.exp_cost_sa_typ_ip_l3) as t_ip_split,
sum(rec.exp_cost_sa_typ_other_l3) as t_o_split,
sum(rec.exp_cost_sa_comp_other_l3) as c_split,
sum(rec.total_exp_cost_ra_l3) as total_unsplit,
sum(rec.exp_cost_ra_typ_ip_l3) as t_ip_unsplit,
sum(rec.exp_cost_ra_typ_other_l3) as t_o_unsplit,
sum(rec.exp_cost_ra_comp_other_l3) as c_unsplit

from ra_exp_cost rec
left join report_episode_summary res
on rec.epi_number=res.episode_id
where rec.exp_cost_sa_typ_ip_l3 is not null
and res.level=3
group by res.filter_id, rec.epi_number;

update report_episode_summary res
join x
on x.episode_id=res.episode_id
and x.level=res.level
and x.filter_id=res.filter_id
set
res.Expected_Split_Average_Cost=x.total_split/Episode_Volume,
res.Expected_Split_Typical_IP_Average_Cost=x.t_ip_split/Episode_Volume,
res.Expected_Split_Typical_Other_Average_Cost=x.t_o_split/Episode_Volume,
res.Expected_Split_PAC_Average_Cost=x.c_split/Episode_Volume,
res.Expected_Unsplit_Average_Cost=x.total_unsplit/Episode_Volume,
res.Expected_Unsplit_Typical_IP_Average_Cost=x.t_ip_unsplit/Episode_Volume,
res.Expected_Unsplit_Typical_Other_Average_Cost=x.t_o_unsplit/Episode_Volume,
res.Expected_Unsplit_PAC_Average_Cost=x.c_unsplit/Episode_Volume;

drop table x;

#add level 4 expected costs for acute episodes to report_episode_summary

create temporary table if not exists x as

select
res.episode_id,
res.level,
res.filter_id,
count(*) as episode_count,
sum(rec.total_exp_cost_sa_l4) as total_split,
sum(rec.exp_cost_sa_typ_ip_l4) as t_ip_split,
sum(rec.exp_cost_sa_typ_other_l4) as t_o_split,
sum(rec.exp_cost_sa_comp_other_l4) as c_split,
sum(rec.total_exp_cost_ra_l4) as total_unsplit,
sum(rec.exp_cost_ra_typ_ip_l4) as t_ip_unsplit,
sum(rec.exp_cost_ra_typ_other_l4) as t_o_unsplit,
sum(rec.exp_cost_ra_comp_other_l4) as c_unsplit

from ra_exp_cost rec
left join report_episode_summary res
on rec.epi_number=res.episode_id
where rec.exp_cost_sa_typ_ip_l4 is not null
and res.level=4
group by res.filter_id, rec.epi_number;

update report_episode_summary res
join x
on x.episode_id=res.episode_id
and x.level=res.level
and x.filter_id=res.filter_id
set
res.Expected_Split_Average_Cost=x.total_split/Episode_Volume,
res.Expected_Split_Typical_IP_Average_Cost=x.t_ip_split/Episode_Volume,
res.Expected_Split_Typical_Other_Average_Cost=x.t_o_split/Episode_Volume,
res.Expected_Split_PAC_Average_Cost=x.c_split/Episode_Volume,
res.Expected_Unsplit_Average_Cost=x.total_unsplit/Episode_Volume,
res.Expected_Unsplit_Typical_IP_Average_Cost=x.t_ip_unsplit/Episode_Volume,
res.Expected_Unsplit_Typical_Other_Average_Cost=x.t_o_unsplit/Episode_Volume,
res.Expected_Unsplit_PAC_Average_Cost=x.c_unsplit/Episode_Volume;

drop table x;
