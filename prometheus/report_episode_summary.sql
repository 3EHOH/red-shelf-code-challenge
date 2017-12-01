set search_path=epbuilder;
                                                                                                                                                                          
select 'Step: report_episode_summary.sql' as curstep, current_timestamp as datetime;                                                                                       
\set AUTOCOMMIT on      

DROP TABLE IF EXISTS report_episode_summary;

CREATE TABLE report_episode_summary
(
    Filter_ID int DEFAULT NULL,
    Episode_ID varchar(6) NOT NULL,
    Episode_Name varchar(6) DEFAULT NULL,
    Episode_Description varchar(255) DEFAULT NULL,
    Episode_Type varchar(50) DEFAULT NULL,
    MDC varchar(2) DEFAULT NULL,
    MDC_Description varchar(255) DEFAULT NULL,
    Level int NOT NULL DEFAULT 0,
    Episode_Volume int DEFAULT NULL,
    Split_Total_Cost float DEFAULT NULL,
    Split_Average_Cost float DEFAULT NULL,
    Split_1stPercentile_Cost float DEFAULT NULL,
    Split_99thPercentile_Cost float DEFAULT NULL,
    Split_Min_Cost float DEFAULT NULL,
    Split_Max_Cost float DEFAULT NULL,
    Split_STDEV float DEFAULT NULL,
    Split_CV float DEFAULT NULL,
    Split_Total_PAC_Cost float DEFAULT NULL,
    Split_Average_PAC_Cost float DEFAULT NULL,
    Split_PAC_Percent numeric(5,2) DEFAULT NULL,
    Split_Total_Typical_Cost float DEFAULT NULL,
    Split_Average_Typical_Cost float DEFAULT NULL,
    Split_Total_TypicalwPAC_Cost float DEFAULT NULL,
    Split_Average_TypicalwPAC_Cost float DEFAULT NULL,
    Annualized_Split_Total_Cost float DEFAULT NULL,
    Annualized_Split_Average_Cost float DEFAULT NULL,
    Annualized_Split_1stPercentile_Cost float DEFAULT NULL,
    Annualized_Split_99thPercentile_Cost float DEFAULT NULL,
    Annualized_Split_Min_Cost float DEFAULT NULL,
    Annualized_Split_Max_Cost float DEFAULT NULL,
    Annualized_Split_STDEV float DEFAULT NULL,
    Annualized_Split_CV float DEFAULT NULL,
    Annualized_Split_Total_PAC_Cost float DEFAULT NULL,
    Annualized_Split_Average_PAC_Cost float DEFAULT NULL,
    Annualized_Split_PAC_Percent numeric(5,2) DEFAULT NULL,
    Annualized_Split_Total_Typical_Cost float DEFAULT NULL,
    Annualized_Split_Average_Typical_Cost float DEFAULT NULL,
    Annualized_Split_Total_TypicalwPAC_Cost float DEFAULT NULL,
    Annualized_Split_Average_TypicalwPAC_Cost float DEFAULT NULL,
    Unsplit_Total_Cost float DEFAULT NULL,
    Unsplit_Average_Cost float DEFAULT NULL,
    Unsplit_1stPercentile_Cost float DEFAULT NULL,
    Unsplit_99thPercentile_Cost float DEFAULT NULL,
    Unsplit_Min_Cost float DEFAULT NULL,
    Unsplit_Max_Cost float DEFAULT NULL,
    Unsplit_STDEV float DEFAULT NULL,
    Unsplit_CV float DEFAULT NULL,
    Unsplit_Total_PAC_Cost float DEFAULT NULL,
    Unsplit_Average_PAC_Cost float DEFAULT NULL,
    Unsplit_PAC_Percent numeric(5,2) DEFAULT NULL,
    Unsplit_Total_Typical_Cost float DEFAULT NULL,
    Unsplit_Average_Typical_Cost float DEFAULT NULL,
    Unsplit_Total_TypicalwPAC_Cost float DEFAULT NULL,
    Unsplit_Average_TypicalwPAC_Cost float DEFAULT NULL,
    Annualized_Unsplit_Total_Cost float DEFAULT NULL,
    Annualized_Unsplit_Average_Cost float DEFAULT NULL,
    Annualized_Unsplit_1stPercentile_Cost float DEFAULT NULL,
    Annualized_Unsplit_99thPercentile_Cost float DEFAULT NULL,
    Annualized_Unsplit_Min_Cost float DEFAULT NULL,
    Annualized_Unsplit_Max_Cost float DEFAULT NULL,
    Annualized_Unsplit_STDEV float DEFAULT NULL,
    Annualized_Unsplit_CV float DEFAULT NULL,
    Annualized_Unsplit_Total_PAC_Cost float DEFAULT NULL,
    Annualized_Unsplit_Average_PAC_Cost float DEFAULT NULL,
    Annualized_Unsplit_PAC_Percent numeric(5,2) DEFAULT NULL,
    Annualized_Unsplit_Total_Typical_Cost float DEFAULT NULL,
    Annualized_Unsplit_Average_Typical_Cost float DEFAULT NULL,
    Annualized_Unsplit_Total_TypicalwPAC_Cost float DEFAULT NULL,
    Annualized_Unsplit_Average_TypicalwPAC_Cost float DEFAULT NULL,
    Expected_Split_Average_Cost float DEFAULT NULL,
    Expected_Split_Typical_IP_Average_Cost float DEFAULT NULL,
    Expected_Split_Typical_Other_Average_Cost float DEFAULT NULL,
    Expected_Split_PAC_Average_Cost float DEFAULT NULL,
    Expected_Unsplit_Average_Cost float DEFAULT NULL,
    Expected_Unsplit_Typical_IP_Average_Cost float DEFAULT NULL,
    Expected_Unsplit_Typical_Other_Average_Cost float DEFAULT NULL,
    Expected_Unsplit_PAC_Average_Cost float DEFAULT NULL
);





INSERT /*+ direct */  into report_episode_summary
(Filter_ID, Episode_ID, Episode_Name, Episode_Description, Episode_Type, MDC, MDC_Description, Level, Episode_Volume, Split_Total_Cost, Split_Average_Cost, Split_Min_Cost, Split_Max_Cost, Split_STDEV, Split_CV, Split_Total_PAC_Cost, Split_Average_PAC_Cost, Split_PAC_Percent, Split_Total_Typical_Cost, Split_Average_Typical_Cost, Split_Total_TypicalwPAC_Cost, Split_Average_TypicalwPAC_Cost)

SELECT
master_epid_level.filter_id,
left(master_epid_level.master_episode_id,6),
eb.NAME,
eb.DESCRIPTION,
eb.TYPE,
eb.MDC_CATEGORY,
null as mdc_description,
master_epid_level.level,
COUNT(*),
cast(SUM(master_epid_level.cost) as numeric(13,2)),
cast(SUM(master_epid_level.cost)/COUNT(*) as numeric(13,2)),
cast(MIN(master_epid_level.cost) as numeric(13,2)),
cast(MAX(master_epid_level.cost) as numeric(13,2)),
round(STDDEV(master_epid_level.cost),2),
round(STDDEV(master_epid_level.cost)/(SUM(master_epid_level.cost)/COUNT(*)),2),
cast(SUM(master_epid_level.cost_c) as numeric(13,2)),
cast(SUM(master_epid_level.cost_c)/COUNT(*) as numeric(13,2)),
cast((SUM(master_epid_level.cost_c)/SUM(master_epid_level.cost))*100 as numeric(13,2)),
cast(SUM(master_epid_level.cost_t) as numeric(13,2)),
cast(SUM(master_epid_level.cost_t)/COUNT(*) as numeric(13,2)),
cast(SUM(master_epid_level.cost_tc) as numeric(13,2)),
cast(SUM(master_epid_level.cost_tc)/COUNT(*) as numeric(13,2))


FROM episode_builder_5_4_003.episode eb
join master_epid_level
on left(master_epid_level.master_episode_id,6) = eb.EPISODE_ID
left join mdc_desc
on eb.MDC_CATEGORY=mdc_desc.mdc



left join sub_distinct
on master_epid_level.master_episode_id=sub_distinct.child_master_episode_id 

left join sub_distinct_filter
on master_epid_level.master_episode_id=sub_distinct_filter.child_master_episode_id 

where master_epid_level.level<sub_distinct.association_level and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=0

AND master_epid_level.level=1
or  sub_distinct.association_level is null  and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0  and master_epid_level.filter_id=0

AND master_epid_level.level=1

or master_epid_level.level<sub_distinct_filter.association_level and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1 
AND master_epid_level.level=1
or  sub_distinct_filter.association_level is null  and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1

AND master_epid_level.level=1

GROUP BY eb.name, eb.DESCRIPTION,
eb.TYPE,
eb.MDC_CATEGORY, left(master_epid_level.master_episode_id,6), master_epid_level.level, master_epid_level.filter_id
;



INSERT /*+ direct */  into report_episode_summary
(Filter_ID, Episode_ID, Episode_Name, Episode_Description, Episode_Type, MDC, MDC_Description, Level, Episode_Volume, Split_Total_Cost, Split_Average_Cost, Split_Min_Cost, Split_Max_Cost, Split_STDEV, Split_CV, Split_Total_PAC_Cost, Split_Average_PAC_Cost, Split_PAC_Percent, Split_Total_Typical_Cost, Split_Average_Typical_Cost, Split_Total_TypicalwPAC_Cost, Split_Average_TypicalwPAC_Cost)
SELECT
master_epid_level.filter_id,
left(master_epid_level.master_episode_id,6),
eb.NAME,
eb.DESCRIPTION,
eb.TYPE,
eb.MDC_CATEGORY,
null as mdc_description,
master_epid_level.level,
COUNT(*),
SUM(master_epid_level.cost),
SUM(master_epid_level.cost)/COUNT(*),
MIN(master_epid_level.cost),
MAX(master_epid_level.cost),
STDDEV(master_epid_level.cost),
STDDEV(master_epid_level.cost)/(SUM(master_epid_level.cost)/COUNT(*)),
SUM(master_epid_level.cost_c),
SUM(master_epid_level.cost_c)/COUNT(*),
(SUM(master_epid_level.cost_c)/SUM(master_epid_level.cost))*100,
SUM(master_epid_level.cost_t),
SUM(master_epid_level.cost_t)/COUNT(*),
SUM(master_epid_level.cost_tc),
SUM(master_epid_level.cost_tc)/COUNT(*)

FROM episode_builder_5_4_003.episode eb
join master_epid_level
on left(master_epid_level.master_episode_id,6) = eb.EPISODE_ID
left join mdc_desc
on eb.MDC_CATEGORY=mdc_desc.mdc



left join sub_distinct
on master_epid_level.master_episode_id=sub_distinct.child_master_episode_id 

left join sub_distinct_filter
on master_epid_level.master_episode_id=sub_distinct_filter.child_master_episode_id 

where master_epid_level.level<sub_distinct.association_level and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=0

AND master_epid_level.level=2
or  sub_distinct.association_level is null  and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0  and master_epid_level.filter_id=0

AND master_epid_level.level=2

or master_epid_level.level<sub_distinct_filter.association_level and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1 
AND master_epid_level.level=2
or  sub_distinct_filter.association_level is null  and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1

AND master_epid_level.level=2

GROUP BY eb.name, eb.DESCRIPTION,
eb.TYPE,
eb.MDC_CATEGORY, left(master_epid_level.master_episode_id,6), master_epid_level.level, master_epid_level.filter_id
;



INSERT /*+ direct */  into report_episode_summary
(Filter_ID, Episode_ID, Episode_Name, Episode_Description, Episode_Type, MDC, MDC_Description, Level, Episode_Volume, Split_Total_Cost, Split_Average_Cost, Split_Min_Cost, Split_Max_Cost, Split_STDEV, Split_CV, Split_Total_PAC_Cost, Split_Average_PAC_Cost, Split_PAC_Percent, Split_Total_Typical_Cost, Split_Average_Typical_Cost, Split_Total_TypicalwPAC_Cost, Split_Average_TypicalwPAC_Cost)
SELECT
master_epid_level.filter_id,
left(master_epid_level.master_episode_id,6),
eb.NAME,
eb.DESCRIPTION,
eb.TYPE,
eb.MDC_CATEGORY,
null as mdc_description,
master_epid_level.level,
COUNT(*),
SUM(master_epid_level.cost),
SUM(master_epid_level.cost)/COUNT(*),
MIN(master_epid_level.cost),
MAX(master_epid_level.cost),
STDDEV(master_epid_level.cost),
STDDEV(master_epid_level.cost)/(SUM(master_epid_level.cost)/COUNT(*)),
SUM(master_epid_level.cost_c),
SUM(master_epid_level.cost_c)/COUNT(*),
(SUM(master_epid_level.cost_c)/SUM(master_epid_level.cost))*100,
SUM(master_epid_level.cost_t),
SUM(master_epid_level.cost_t)/COUNT(*),
SUM(master_epid_level.cost_tc),
SUM(master_epid_level.cost_tc)/COUNT(*)

FROM episode_builder_5_4_003.episode eb
join master_epid_level
on left(master_epid_level.master_episode_id,6) = eb.EPISODE_ID
left join mdc_desc
on eb.MDC_CATEGORY=mdc_desc.mdc



left join sub_distinct
on master_epid_level.master_episode_id=sub_distinct.child_master_episode_id 

left join sub_distinct_filter
on master_epid_level.master_episode_id=sub_distinct_filter.child_master_episode_id 

where master_epid_level.level<sub_distinct.association_level and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=0

AND master_epid_level.level=3
or  sub_distinct.association_level is null  and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0  and master_epid_level.filter_id=0

AND master_epid_level.level=3

or master_epid_level.level<sub_distinct_filter.association_level and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1 
AND master_epid_level.level=3
or  sub_distinct_filter.association_level is null  and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1

AND master_epid_level.level=3

GROUP BY eb.name, eb.DESCRIPTION,
eb.TYPE,
eb.MDC_CATEGORY, left(master_epid_level.master_episode_id,6), master_epid_level.level, master_epid_level.filter_id
;
;


INSERT /*+ direct */  into report_episode_summary
(Filter_ID, Episode_ID, Episode_Name, Episode_Description, Episode_Type, MDC, MDC_Description, Level, Episode_Volume, Split_Total_Cost, Split_Average_Cost, Split_Min_Cost, Split_Max_Cost, Split_STDEV, Split_CV, Split_Total_PAC_Cost, Split_Average_PAC_Cost, Split_PAC_Percent, Split_Total_Typical_Cost, Split_Average_Typical_Cost, Split_Total_TypicalwPAC_Cost, Split_Average_TypicalwPAC_Cost)
SELECT
master_epid_level.filter_id,
left(master_epid_level.master_episode_id,6),
eb.NAME,
eb.DESCRIPTION,
eb.TYPE,
eb.MDC_CATEGORY,
null as mdc_description,
master_epid_level.level,
COUNT(*),
SUM(master_epid_level.cost),
SUM(master_epid_level.cost)/COUNT(*),
MIN(master_epid_level.cost),
MAX(master_epid_level.cost),
STDDEV(master_epid_level.cost),
STDDEV(master_epid_level.cost)/(SUM(master_epid_level.cost)/COUNT(*)),
SUM(master_epid_level.cost_c),
SUM(master_epid_level.cost_c)/COUNT(*),
(SUM(master_epid_level.cost_c)/SUM(master_epid_level.cost))*100,
SUM(master_epid_level.cost_t),
SUM(master_epid_level.cost_t)/COUNT(*),
SUM(master_epid_level.cost_tc),
SUM(master_epid_level.cost_tc)/COUNT(*)

FROM episode_builder_5_4_003.episode eb
join master_epid_level
on left(master_epid_level.master_episode_id,6) = eb.EPISODE_ID
left join mdc_desc
on eb.MDC_CATEGORY=mdc_desc.mdc



left join sub_distinct
on master_epid_level.master_episode_id=sub_distinct.child_master_episode_id 

left join sub_distinct_filter
on master_epid_level.master_episode_id=sub_distinct_filter.child_master_episode_id 

where master_epid_level.level<sub_distinct.association_level and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=0

AND master_epid_level.level=4
or  sub_distinct.association_level is null  and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0  and master_epid_level.filter_id=0

AND master_epid_level.level=4

or master_epid_level.level<sub_distinct_filter.association_level and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1 
AND master_epid_level.level=4
or  sub_distinct_filter.association_level is null  and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1

AND master_epid_level.level=4

GROUP BY eb.name, eb.DESCRIPTION,
eb.TYPE,
eb.MDC_CATEGORY, left(master_epid_level.master_episode_id,6), master_epid_level.level, master_epid_level.filter_id
;
;

INSERT /*+ direct */  into report_episode_summary
(Filter_ID, Episode_ID, Episode_Name, Episode_Description, Episode_Type, MDC, MDC_Description, Level, Episode_Volume, Split_Total_Cost, Split_Average_Cost, Split_Min_Cost, Split_Max_Cost, Split_STDEV, Split_CV, Split_Total_PAC_Cost, Split_Average_PAC_Cost, Split_PAC_Percent, Split_Total_Typical_Cost, Split_Average_Typical_Cost, Split_Total_TypicalwPAC_Cost, Split_Average_TypicalwPAC_Cost)
SELECT
master_epid_level.filter_id,
left(master_epid_level.master_episode_id,6),
eb.NAME,
eb.DESCRIPTION,
eb.TYPE,
eb.MDC_CATEGORY,
null as mdc_description,
master_epid_level.level,
COUNT(*),
SUM(master_epid_level.cost),
SUM(master_epid_level.cost)/COUNT(*),
MIN(master_epid_level.cost),
MAX(master_epid_level.cost),
STDDEV(master_epid_level.cost),
STDDEV(master_epid_level.cost)/(SUM(master_epid_level.cost)/COUNT(*)),
SUM(master_epid_level.cost_c),
SUM(master_epid_level.cost_c)/COUNT(*),
(SUM(master_epid_level.cost_c)/SUM(master_epid_level.cost))*100,
SUM(master_epid_level.cost_t),
SUM(master_epid_level.cost_t)/COUNT(*),
SUM(master_epid_level.cost_tc),
SUM(master_epid_level.cost_tc)/COUNT(*)

FROM episode_builder_5_4_003.episode eb
join master_epid_level
on left(master_epid_level.master_episode_id,6) = eb.EPISODE_ID
left join mdc_desc
on eb.MDC_CATEGORY=mdc_desc.mdc



left join sub_distinct
on master_epid_level.master_episode_id=sub_distinct.child_master_episode_id 

left join sub_distinct_filter
on master_epid_level.master_episode_id=sub_distinct_filter.child_master_episode_id 

where master_epid_level.level<sub_distinct.association_level and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=0

AND master_epid_level.level=5
or  sub_distinct.association_level is null  and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0  and master_epid_level.filter_id=0

AND master_epid_level.level=5

or master_epid_level.level<sub_distinct_filter.association_level and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1 
AND master_epid_level.level=5
or  sub_distinct_filter.association_level is null  and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1

AND master_epid_level.level=5

GROUP BY eb.name, eb.DESCRIPTION,
eb.TYPE,
eb.MDC_CATEGORY, left(master_epid_level.master_episode_id,6), master_epid_level.level, master_epid_level.filter_id
;
;


drop table IF EXISTS melSum;
CREATE TEMPORARY TABLE melSum  ON COMMIT PRESERVE ROWS AS
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
STDDEV(cost) AS std_cost,
count(*) AS episode_count
FROM master_epid_level
WHERE claim_type='CL'
AND split=0
AND annualized=0
GROUP BY filter_id, left(master_episode_id,6),  level;



update /*+direct*/  report_episode_summary
SET
Unsplit_Total_Cost=mel.cost,
Unsplit_Average_Cost=mel.cost/Episode_Volume,
Unsplit_Min_Cost=mel.min_cost,
Unsplit_Max_Cost=mel.max_cost,
Unsplit_STDEV=mel.std_cost,
Unsplit_CV=mel.std_cost/(mel.cost/Episode_Volume),
Unsplit_Total_PAC_Cost=mel.cost_c,
Unsplit_Average_PAC_Cost=mel.cost_c/Episode_Volume,
Unsplit_PAC_Percent=mel.cost_c/mel.cost*100,
Unsplit_Total_Typical_Cost=mel.cost_t,
Unsplit_Average_Typical_Cost=mel.cost_t/Episode_Volume,
Unsplit_Total_TypicalwPAC_Cost=mel.cost_tc,
Unsplit_Average_TypicalwPAC_Cost=mel.cost_tc/Episode_Volume
from melSum mel where mel.episode_id=report_episode_summary.episode_id
AND mel.level=report_episode_summary.level
and mel.filter_id=report_episode_summary.filter_id;



drop table if exists melSum;
CREATE TEMPORARY TABLE melSum ON COMMIT PRESERVE ROWS AS
SELECT
filter_id,
left(master_episode_id,6) as episode_id,
level,
SUM(cost) AS cost,
SUM(cost_t) AS cost_t,
SUM(cost_tc) AS cost_tc,
SUM(cost_c) AS cost_c,
MIN(cost) AS min_cost,
MAX(cost) AS max_cost,
STDDEV(cost) AS std_cost,
count(*) AS episode_count
FROM master_epid_level
WHERE claim_type='CL'
AND split=1
AND annualized=1
GROUP BY filter_id, left(master_episode_id,6),  level;

update /*+direct*/  report_episode_summary  set
 Annualized_Split_Total_Cost=mel.cost,
 Annualized_Split_Average_Cost=mel.cost/Episode_Volume,
 Annualized_Split_Min_Cost=mel.min_cost,
 Annualized_Split_Max_Cost=mel.max_cost,
 Annualized_Split_STDEV=mel.std_cost,
 Annualized_Split_CV=mel.std_cost/(mel.cost/Episode_Volume),
 Annualized_Split_Total_PAC_Cost=mel.cost_c,
 Annualized_Split_Average_PAC_Cost=mel.cost_c/Episode_Volume,
 Annualized_Split_PAC_Percent=mel.cost_c/mel.cost*100,
 Annualized_Split_Total_Typical_Cost=mel.cost_t,
 Annualized_Split_Average_Typical_Cost=mel.cost_t/Episode_Volume,
 Annualized_Split_Total_TypicalwPAC_Cost=mel.cost_tc,
 Annualized_Split_Average_TypicalwPAC_Cost=mel.cost_tc/Episode_Volume
from melSum mel where mel.episode_id= report_episode_summary.episode_id
AND mel.level= report_episode_summary.level
and mel.filter_id= report_episode_summary.filter_id;


drop table if exists melSum;
CREATE TEMPORARY TABLE melSum ON COMMIT PRESERVE ROWS AS
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
STDDEV(cost) AS std_cost,
count(*) AS episode_count
FROM master_epid_level
WHERE  claim_type='CL'
AND  split=0
AND annualized=1
GROUP BY filter_id, left(master_episode_id,6),  level;


update /*+direct*/  report_episode_summary 
SET
 Annualized_Unsplit_Total_Cost=mel.cost,
 Annualized_Unsplit_Average_Cost=mel.cost/Episode_Volume,
 Annualized_Unsplit_Min_Cost=mel.min_cost,
 Annualized_Unsplit_Max_Cost=mel.max_cost,
 Annualized_Unsplit_STDEV=mel.std_cost,
 Annualized_Unsplit_CV=mel.std_cost/(mel.cost/Episode_Volume),
 Annualized_Unsplit_Total_PAC_Cost=mel.cost_c,
 Annualized_Unsplit_Average_PAC_Cost=mel.cost_c/Episode_Volume,
 Annualized_Unsplit_PAC_Percent=mel.cost_c/mel.cost*100,
 Annualized_Unsplit_Total_Typical_Cost=mel.cost_t,
 Annualized_Unsplit_Average_Typical_Cost=mel.cost_t/Episode_Volume,
 Annualized_Unsplit_Total_TypicalwPAC_Cost=mel.cost_tc,
 Annualized_Unsplit_Average_TypicalwPAC_Cost=mel.cost_tc/Episode_Volume
from melSum mel where mel.episode_id= report_episode_summary.episode_id
AND mel.level= report_episode_summary.level
and mel.filter_id= report_episode_summary.filter_id;



DROP TABLE if exists melSum;


update /*+direct*/  report_episode_summary
SET
 Split_1stPercentile_Cost=p.Split_1stPercentile_Cost,
 Split_99thPercentile_Cost=p.Split_99thPercentile_Cost,
 Annualized_Split_1stPercentile_Cost=p.Annualized_Split_1stPercentile_Cost,
 Annualized_Split_99thPercentile_Cost=p.Annualized_Split_99thPercentile_Cost,
 Unsplit_1stPercentile_Cost=p.Unsplit_1stPercentile_Cost,
 Unsplit_99thPercentile_Cost=p.Unsplit_99thPercentile_Cost,
 Annualized_Unsplit_1stPercentile_Cost=p.Annualized_Unsplit_1stPercentile_Cost,
 Annualized_Unsplit_99thPercentile_Cost=p.Annualized_Unsplit_99thPercentile_Cost
from percentiles p where p.episode_id= report_episode_summary.episode_id
AND p.level= report_episode_summary.level
AND p.filter_id= report_episode_summary.filter_id;

/*##########NEED TO TEST THIS SECTION###################**/

/*#add expected costs to report_episode_summary table for level 1 and the level at which the episode is complete

#add level 1 expected costs for chronic and other condition episodes to report_episode_summary*/

drop table if exists x;
create temporary table x ON COMMIT PRESERVE ROWS as

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
group by res.filter_id, res.episode_id,res.level;

update /*+direct*/  report_episode_summary
set
Expected_Split_Average_Cost=x.total_split/Episode_Volume,
Expected_Split_Typical_Other_Average_Cost=x.t_split/Episode_Volume,
Expected_Split_PAC_Average_Cost=x.c_split/Episode_Volume,
Expected_Unsplit_Average_Cost=x.total_unsplit/Episode_Volume,
Expected_Unsplit_Typical_Other_Average_Cost=x.t_unsplit/Episode_Volume,
Expected_Unsplit_PAC_Average_Cost=x.c_unsplit/Episode_Volume
from x
where x.episode_id=report_episode_summary.episode_id
and x.level=report_episode_summary.level
and x.filter_id=report_episode_summary.filter_id;


/*#add level 5 expected costs for chronic and other condition episodes to report_episode_summary*/
drop table if exists x;
create temporary table x ON COMMIT PRESERVE ROWS as
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
group by res.filter_id, res.episode_id,res.level;


update /*+direct*/  report_episode_summary
set
 Expected_Split_Average_Cost=x.total_split/Episode_Volume,
 Expected_Split_Typical_Other_Average_Cost=x.t_split/Episode_Volume,
 Expected_Split_PAC_Average_Cost=x.c_split/Episode_Volume,
 Expected_Unsplit_Average_Cost=x.total_unsplit/Episode_Volume,
 Expected_Unsplit_Typical_Other_Average_Cost=x.t_unsplit/Episode_Volume,
 Expected_Unsplit_PAC_Average_Cost=x.c_unsplit/Episode_Volume
from x
where x.episode_id= report_episode_summary.episode_id
and x.level= report_episode_summary.level
and x.filter_id= report_episode_summary.filter_id;


/*#add level 1 expected costs for acute and procedural episodes to report_episode_summary*/
drop table if exists x;
create temporary table x ON COMMIT PRESERVE ROWS as
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
group by res.filter_id, res.episode_id,res.level;


update /*+direct*/  report_episode_summary
set
 Expected_Split_Average_Cost=x.total_split/Episode_Volume,
 Expected_Split_Typical_IP_Average_Cost=x.t_ip_split/Episode_Volume,
 Expected_Split_Typical_Other_Average_Cost=x.t_o_split/Episode_Volume,
 Expected_Split_PAC_Average_Cost=x.c_split/Episode_Volume,
 Expected_Unsplit_Average_Cost=x.total_unsplit/Episode_Volume,
 Expected_Unsplit_Typical_IP_Average_Cost=x.t_ip_unsplit/Episode_Volume,
 Expected_Unsplit_Typical_Other_Average_Cost=x.t_o_unsplit/Episode_Volume,
 Expected_Unsplit_PAC_Average_Cost=x.c_unsplit/Episode_Volume
from x
where x.episode_id= report_episode_summary.episode_id
and x.level= report_episode_summary.level
and x.filter_id= report_episode_summary.filter_id;


/*#add level 3 expected costs for procedural episodes to report_episode_summary*/

drop table if exists x;
create temporary table x ON COMMIT PRESERVE ROWS as
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
group by res.filter_id, res.episode_id,
res.level;


update /*+direct*/  report_episode_summary
set
 Expected_Split_Average_Cost=x.total_split/Episode_Volume,
 Expected_Split_Typical_IP_Average_Cost=x.t_ip_split/Episode_Volume,
 Expected_Split_Typical_Other_Average_Cost=x.t_o_split/Episode_Volume,
 Expected_Split_PAC_Average_Cost=x.c_split/Episode_Volume,
 Expected_Unsplit_Average_Cost=x.total_unsplit/Episode_Volume,
 Expected_Unsplit_Typical_IP_Average_Cost=x.t_ip_unsplit/Episode_Volume,
 Expected_Unsplit_Typical_Other_Average_Cost=x.t_o_unsplit/Episode_Volume,
 Expected_Unsplit_PAC_Average_Cost=x.c_unsplit/Episode_Volume
from x
where x.episode_id= report_episode_summary.episode_id
and x.level= report_episode_summary.level
and x.filter_id= report_episode_summary.filter_id;


/*#add level 4 expected costs for acute episodes to report_episode_summary*/

drop table if exists x;
create temporary table x ON COMMIT PRESERVE ROWS as

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
group by res.filter_id, res.episode_id,
res.level;

update /*+direct*/  report_episode_summary
set
 Expected_Split_Average_Cost=x.total_split/Episode_Volume,
 Expected_Split_Typical_IP_Average_Cost=x.t_ip_split/Episode_Volume,
 Expected_Split_Typical_Other_Average_Cost=x.t_o_split/Episode_Volume,
 Expected_Split_PAC_Average_Cost=x.c_split/Episode_Volume,
 Expected_Unsplit_Average_Cost=x.total_unsplit/Episode_Volume,
 Expected_Unsplit_Typical_IP_Average_Cost=x.t_ip_unsplit/Episode_Volume,
 Expected_Unsplit_Typical_Other_Average_Cost=x.t_o_unsplit/Episode_Volume,
 Expected_Unsplit_PAC_Average_Cost=x.c_unsplit/Episode_Volume
from x
where x.episode_id= report_episode_summary.episode_id
and x.level= report_episode_summary.level
and x.filter_id= report_episode_summary.filter_id;




drop table x;
