set search_path=epbuilder;

select 'Step: SQL-DefaultAll-NYS-del_filters_added.sql' as curstep, current_timestamp as datetime;
\set AUTOCOMMIT on



INSERT /*+ direct */  INTO filtered_episodes (filter_id, master_episode_id)
SELECT DISTINCT 0 AS filter_id, master_episode_id FROM episode;

INSERT /*+ direct */  INTO filtered_episodes (filter_id, master_episode_id)
SELECT DISTINCT 1 AS filter_id, master_episode_id FROM episode;

/* ==== BEGIN ==== 
DUAL Filtering 
*/

alter table filtered_episodes drop column dual_eligible;
alter table filtered_episodes add column dual_eligible numeric (1) default 0;

update /*+direct*/  filtered_episodes
set
filter_fail=0,
age_limit_lower=0,
age_limit_upper=0,
episode_cost_upper=0,
episode_cost_lower=0,
coverage_gap=0,
episode_complete=0,
drg=0,
proc_ep_orphan=0,
proc_ep_orph_triggered=0,
dual_eligible=0;


update /*+direct*/  filtered_episodes fe
set dual_eligible=1
FROM 
(SELECT 
e.master_episode_id FROM episode e
JOIN member m 
ON e.member_id=m.member_id
and m.dual_eligible=1
) AS sq
WHERE sq.master_episode_id=fe.master_episode_id
AND fe.filter_id=1;

/* ==== END ==== 
DUAL Filtering */

/* ==== BEGIN ==== 
Age Range Filtering 
*/

update /*+direct*/  filtered_episodes fe
SET age_limit_lower=1
FROM (SELECT e.master_episode_id FROM episode e
JOIN member m ON e.member_id=m.member_id
JOIN filter_params fp ON fp.episode_id=e.episode_id
AND ( year(e.episode_begin_date)-m.birth_year < fp.lower_age_limit )
) AS sq
WHERE sq.master_episode_id=fe.master_episode_id
AND fe.filter_id=1;

/*
update /*+direct*/  filtered_episodes fe
JOIN episode e ON fe.master_episode_id=e.master_episode_id
JOIN member m ON e.member_id=m.member_id
JOIN filter_params fp ON fp.episode_id=e.episode_id
AND ( year(e.episode_begin_date)-m.birth_year > fp.upper_age_limit )
AND fe.filter_id=1
SET fe.`age_limit_upper`=1;
*/

update /*+direct*/  filtered_episodes fe
SET age_limit_upper=1
FROM (SELECT e.master_episode_id FROM episode e
JOIN member m ON e.member_id=m.member_id
JOIN filter_params fp ON fp.episode_id=e.episode_id
AND ( year(e.episode_begin_date)-m.birth_year > fp.upper_age_limit )
) AS sq
WHERE sq.master_episode_id=fe.master_episode_id
AND fe.filter_id=1;

/* Filter out any members who do not have member records */


update /*+direct*/  filtered_episodes fe 
SET age_limit_lower=1, age_limit_upper=1
FROM (SELECT master_episode_id FROM episode
WHERE member_id NOT IN (SELECT member_id FROM member) ) AS sq
WHERE fe.master_episode_id=sq.master_episode_id
AND fe.filter_id=1;


/* ==== END ==== 
Age Range Filtering */


/* ==== BEGIN ==== 
Enrollment Gap Filtering */

/* need a query to find episodes with a gap within the time period of the episode...
// !!! isGap MUST BE 1 IN PRODUCTION!
*/

TRUNCATE TABLE tmp_enroll_gap;

INSERT /*+ direct */  INTO tmp_enroll_gap
( 
episode_begin_date, 
episode_end_date, 
gap_begin_date,
gap_end_date,
episode_length,
gap_length,
master_episode_id,
member_id
)
SELECT
e.episode_begin_date,
e.episode_end_date,
en.begin_date,
en.end_date,
DATEDIFF('d', e.episode_begin_date, e.episode_end_date) AS episode_length,
DATEDIFF('d', en.begin_date, en.end_date) AS gap_length,
e.master_episode_id,
e.member_id
FROM episode e
JOIN enrollment en ON e.member_id=en.member_id
AND en.isGap = 1 
AND en.begin_date <= e.episode_end_date
AND en.end_date >= e.episode_begin_date;

/* DROP TABLE tmp_epi */

CREATE TABLE tmp_epi AS 
SELECT
e.episode_begin_date,
e.episode_end_date,
DATEDIFF('d', e.episode_begin_date, e.episode_end_date) AS episode_length,
e.master_episode_id,
e.member_id
FROM episode e
WHERE e.master_episode_id NOT IN 
(SELECT master_episode_id FROM tmp_enroll_gap);

INSERT /*+ direct */  INTO tmp_enroll_gap
(
episode_begin_date, 
episode_end_date, 
episode_length,
master_episode_id,
member_id
)
SELECT 
episode_begin_date,
episode_end_date,
episode_length,
master_episode_id,
member_id
FROM tmp_epi;

DROP TABLE tmp_epi;

/* look for gaps wholly within the episode */
update /*+direct*/  tmp_enroll_gap
SET filter_fail=1
WHERE gap_end_date<=episode_end_date
AND gap_begin_date>=episode_begin_date
AND 
(
DATEDIFF('d', gap_begin_date, gap_end_date) >= 30
AND episode_length >= 90
)
OR
(
DATEDIFF('d', gap_begin_date, gap_end_date) >= 1
AND episode_length <= 89
);


/* first look for any gaps that overlap with the entire episode */
update /*+direct*/  tmp_enroll_gap
SET filter_fail=1
WHERE gap_begin_date < episode_begin_date
AND gap_end_date > episode_end_date;

/* look for any gaps that start before the beginning of the episode
// and find the amount of days of gap within the episode (begin episode to end gap)
// and find out if that amount is long enough to filter the episode 
*/
update /*+direct*/  tmp_enroll_gap
SET filter_fail=1
WHERE gap_begin_date<episode_begin_date
AND gap_end_date>=episode_begin_date
AND 
(
DATEDIFF('d', episode_begin_date, gap_end_date) >= 30
AND episode_length >= 90
)
OR
(
DATEDIFF('d', episode_begin_date, gap_end_date) >= 1
AND DATEDIFF('d', episode_begin_date, gap_end_date) <= 89
AND episode_length <= 89
);


/* look for any gaps that end after the end of the episode
// and find the amount of days of gap within the episode (begin gap to end episode)
// and find out if that amount is long enough to filter the episode 
*/
update /*+direct*/  tmp_enroll_gap
SET filter_fail=1
WHERE gap_begin_date<episode_end_date
AND gap_end_date>=episode_end_date
AND 
(
DATEDIFF('d', gap_begin_date, episode_end_date) >= 30
AND episode_length >= 90
)
OR
(
DATEDIFF('d', gap_begin_date, episode_end_date) >= 1
AND DATEDIFF('d', gap_begin_date, episode_end_date) <= 89
AND episode_length <= 89
);

/*
for annualizable episodes, need to set 1's back to 0's
if gaps do not overlap at all with final year ...
*/



/* Set all annualizable episodes back to zero */
/* need additional columns to avoid slowdowns from trying to join in one query...*/

ALTER TABLE tmp_enroll_gap ADD COLUMN data_latest_begin_date DATE;

ALTER TABLE tmp_enroll_gap ADD COLUMN annualize_begin_date DATE;

ALTER TABLE tmp_enroll_gap ADD COLUMN data_start_date DATE;

ALTER TABLE tmp_enroll_gap ADD COLUMN end_of_study INT DEFAULT 0;

ALTER TABLE tmp_enroll_gap ADD COLUMN annualizable INT DEFAULT 0;

/* mark episodes that are potentially annualizable because their end of study is 1*/
update /*+direct*/  tmp_enroll_gap teg
SET end_of_study=1
FROM (SELECT e.master_episode_id FROM episode e 
	JOIN episode_builder_5_4_003.episode eb ON eb.episode_id=e.episode_id
	AND eb.END_OF_STUDY=1) AS eos
WHERE eos.master_episode_id=teg.master_episode_id;

/* SELECT * FROM tmp_enroll_gap WHERE end_of_study=1 */

/* get the begin date of the annualization period... */
update /*+direct*/  tmp_enroll_gap 
SET data_latest_begin_date = (
SELECT data_latest_begin_date FROM run
);

/* get the begin date of the annualization period... */
update /*+direct*/  tmp_enroll_gap 
SET annualize_begin_date = TIMESTAMPADD('yyyy', -1, data_latest_begin_date);

update /*+direct*/  tmp_enroll_gap
SET data_start_date = (SELECT data_start_date FROM run);

/* annualizable episodes are defined as those whose trigger date is 
before the first day of the last year of the study period */
update /*+direct*/  tmp_enroll_gap
SET annualizable=1
WHERE end_of_study=1
AND episode_begin_date < annualize_begin_date
AND episode_begin_date >= data_start_date;

/* remove the fail flag from annualizable episodes
this way we know we've only filtered for gaps within the annualizable period
for annualizable episodes */
update /*+direct*/  tmp_enroll_gap 
SET filter_fail=0
WHERE annualizable=1; 

/* look for gaps larger than 30 days wholly within the annualizable period */
update /*+direct*/  tmp_enroll_gap
SET filter_fail=1
WHERE gap_end_date<=episode_end_date
AND gap_begin_date>=annualize_begin_date
AND DATEDIFF('d', gap_begin_date, gap_end_date) >= 30
AND annualizable=1;

/* look for gaps that overlap with the entire annualizable period 
for annualizable episodes */
update /*+direct*/  tmp_enroll_gap
SET filter_fail=1
WHERE gap_begin_date <= annualize_begin_date
AND gap_end_date >= episode_end_date
AND annualizable=1;

/* look for any gaps that start before the beginning of the annualizable episode
// and find the amount of days of gap within the episode (begin annualize period to end gap)
// and find out if that amount is long enough to filter the episode 
*/
update /*+direct*/  tmp_enroll_gap
SET filter_fail=1
WHERE gap_begin_date<episode_begin_date
AND gap_end_date>=episode_begin_date
AND DATEDIFF('d', annualize_begin_date, gap_end_date) >= 30
AND annualizable=1;

/* look for any gaps that end after the end of the episode
// and find the amount of days of gap within the episode (begin gap to end episode)
// and find out if that amount is long enough to filter the episode 
*/
update /*+direct*/  tmp_enroll_gap
SET filter_fail=1
WHERE gap_begin_date<episode_end_date
AND gap_end_date>=episode_end_date
AND DATEDIFF('d', gap_begin_date, episode_end_date) >= 30
AND annualizable=1;

/* finally, take all the gap flags we've found and put them in the master filtered_episodes table */
update /*+direct*/  filtered_episodes fe
SET coverage_gap=1
FROM (SELECT master_episode_id FROM tmp_enroll_gap 
WHERE filter_fail=1) as teg
WHERE teg.master_episode_id=fe.master_episode_id
AND fe.filter_id=1;

/* ==== END ==== 
Enrollment Gap Filtering */


/* ==== BEGIN ==== 
Procedural Orphan Filter Flagging 
need episode type filter
only procedures
*/




TRUNCATE TABLE tmp_proc_orph;

/* episodes triggered by PB */
INSERT /*+ direct */  INTO tmp_proc_orph
SELECT DISTINCT
e.master_episode_id, 
e.master_claim_id, 
cl.claim_line_type_code,  
cl.begin_date,
cl.end_date,
0 AS confirmed
FROM episode e
JOIN claim_line cl ON cl.master_claim_id=e.master_claim_id
AND e.episode_type='P'
AND cl.claim_line_type_code='PB';

/* DROP TABLE tmp_pro_conf; */

/* Get the confirmations for the IP/OP... */
CREATE TABLE tmp_po_conf AS (
SELECT 
DISTINCT tpo.master_episode_id
FROM tmp_proc_orph tpo
JOIN assignment sig ON sig.master_episode_id=tpo.master_episode_id
AND sig.master_claim_id<>tpo.master_claim_id
JOIN claim_line cl ON sig.master_claim_id=cl.master_claim_id
AND cl.claim_line_type_code IN ('IP','OP')
AND (
	(cl.begin_date BETWEEN TIMESTAMPADD('dd', -2, tpo.begin_date) AND TIMESTAMPADD('dd', 2, tpo.end_date))
	OR
	(cl.end_date BETWEEN TIMESTAMPADD('dd', -2, tpo.begin_date) AND TIMESTAMPADD('dd', 2, tpo.end_date))
	OR
	(cl.begin_date < TIMESTAMPADD('dd', -2, tpo.begin_date) AND cl.end_date> TIMESTAMPADD('dd', 2, tpo.end_date))
)
);

/* TIMESTAMPADD('dd', -2, tpo.begin_date) */

/* previous date comparison only found those claims starting between the begin and end
( cl.begin_date<=DATE_ADD(tpo.end_date, INTERVAL 2 DAY) AND cl.begin_date>=DATE_ADD(tpo.begin_date, INTERVAL -2 DAY)  )
*/

update /*+direct*/  tmp_proc_orph tpo
SET confirmed=1
FROM tmp_po_conf tpc 
WHERE tpo.master_episode_id=tpc.master_episode_id;

DROP TABLE tmp_po_conf;

CREATE TABLE tmp_po_conf AS (
SELECT 
tpo.master_episode_id,
tpo.begin_date,
TIMESTAMPADD('dd', -2, tpo.begin_date) AS negbegdate
FROM tmp_proc_orph tpo
JOIN assignment sig ON sig.master_episode_id=tpo.master_episode_id
AND sig.master_claim_id<>tpo.master_claim_id
JOIN claim_line cl ON sig.master_claim_id=cl.master_claim_id
AND tpo.claim_line_type_code='PB'
AND cl.claim_line_type_code IN ('IP','OP')
AND cl.begin_date>=TIMESTAMPADD('dd', -2, tpo.begin_date)
AND cl.end_date<=TIMESTAMPADD('dd', 2, tpo.end_date)
);

/* TIMESTAMPADD('dd', 2, tpo.end_date) */

update /*+direct*/  tmp_proc_orph tpo
SET confirmed=1
FROM tmp_po_conf tpc 
WHERE tpo.master_episode_id=tpc.master_episode_id;

update /*+direct*/  filtered_episodes fe
SET proc_ep_orphan=1
FROM tmp_proc_orph po 
WHERE fe.master_episode_id=po.master_episode_id
AND po.confirmed=0
AND fe.filter_id=1;

DROP TABLE tmp_po_conf;

TRUNCATE TABLE tmp_proc_orph;

/* Now find any episodes triggered by filtered procedurals and filter them…*/

/* gets the master_episode_id of all episodes triggered by 
a procedural orphan for marking as an orphan themselves */
/* DROP TABLE tmp_trigorph; */

CREATE TABLE tmp_trigorph AS (
SELECT 
t.master_episode_id
FROM filtered_episodes fe
JOIN episode e ON fe.master_episode_id=e.master_episode_id
AND fe.proc_ep_orphan=1
AND fe.filter_id=1
JOIN triggers t ON t.master_claim_id=e.master_claim_id
AND e.episode_id=t.trig_by_episode_id
AND t.dropped=0
);

update /*+direct*/  filtered_episodes fe
SET proc_ep_orphan=1
FROM tmp_trigorph tt 
WHERE fe.master_episode_id=tt.master_episode_id
AND fe.filter_id=1;

DROP TABLE tmp_trigorph;

TRUNCATE TABLE tmp_proc_orph;

TRUNCATE TABLE tmp_filt_proc_orp_trig;

/* filtering - condition episodes triggered off filtered p episodes: */
/* update  trigger table to have correct trig_by_master_episode_id */
update /*+direct*/  triggers
SET trig_by_master_episode_id = (trig_by_episode_id || '_' || member_id || '_' || claim_id || '_' || claim_line_id)
WHERE trig_by_episode_id <> '';

/* #get the master_episode_ids of the dropped Ps */
INSERT /*+ direct */  INTO tmp_filt_proc_orp_trig
SELECT
master_episode_id
FROM filtered_episodes
WHERE filter_id=1
AND proc_ep_orphan=1;

/* #mark the master_episode_ids of the episodes triggered by filtered Ps */
update /*+direct*/  filtered_episodes fe
SET proc_ep_orph_triggered=1
FROM (
	SELECT t.master_episode_id FROM triggers t
	JOIN tmp_filt_proc_orp_trig tt ON t.trig_by_master_episode_id=tt.master_episode_id
	) AS sq 
WHERE fe.master_episode_id=sq.master_episode_id
AND fe.filter_id=1;

/* ==== END ==== 
Procedural Orphan Code
 */

/* ==== BEGIN ==== 
Episode Completion Filtering 
*/

/* chronic episode completion */

/* changing this filter from chronic to annualizable...

update /*+direct*/  filtered_episodes fe
SET episode_complete=1
FROM episode e 
WHERE e.master_episode_id=fe.master_episode_id
AND e.episode_type='C'
AND (
episode_begin_date>(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run)
OR trig_begin_date<(SELECT data_start_date FROM run)
)
AND fe.filter_id=1;
*/

update /*+direct*/  filtered_episodes fe
SET episode_complete=1
FROM tmp_enroll_gap teg 
WHERE teg.master_episode_id=fe.master_episode_id
AND teg.end_of_study=1
AND teg.annualizable=0
AND fe.filter_id=1;

/* all other episode completion */

/*update /*+direct*/  filtered_episodes fe
JOIN episode e ON e.master_episode_id=fe.master_episode_id
AND e.episode_type<>'C'
AND
( 
episode_begin_date<(SELECT data_start_date FROM run)
OR episode_end_date>(SELECT data_latest_begin_date FROM run)
)
AND fe.filter_id=1
SET episode_complete=1;*/

update /*+direct*/  filtered_episodes fe
SET episode_complete=1
FROM tmp_enroll_gap teg 
WHERE teg.master_episode_id=fe.master_episode_id
AND teg.end_of_study=0
AND (teg.episode_begin_date < teg.data_start_date
OR teg.episode_end_date > teg.data_latest_begin_date)
AND fe.filter_id=1;

/* ==== END ==== 
Episode Completion Filtering 
*/



/* ==== BEGIN ==== 
DRG Filtering 
*/

/*
EP0807 - Hip Revision
EP0812 - Hip Replacement & Hip Revision
EP0806 - Knee Revision
EP0813 - Knee Replacement & Knee Revision
*/


/* Triggers which are IP and have matching DRG */

/*update /*+direct*/  filtered_episodes fe
JOIN episode e ON e.master_episode_id=fe.master_episode_id
JOIN claim_line cl ON e.master_claim_id=cl.master_claim_id
AND e.episode_id IN ('EP0807','EP0812','EP0806','EP0813')
AND cl.claim_line_type_code='IP'
AND cl.ms_drg_code IN ('461','462')
AND fe.filter_id=1
SET drg=1;*/

update /*+direct*/  filtered_episodes fe
SET drg=1 
FROM (
SELECT master_episode_id FROM episode e 
JOIN claim_line cl ON e.master_claim_id=cl.master_claim_id
AND e.episode_id IN ('EP0807','EP0812','EP0806','EP0813')
AND cl.claim_line_type_code='IP'
AND cl.ms_drg_code IN ('461','462')
) AS sq
WHERE fe.master_episode_id=sq.master_episode_id
AND fe.filter_id=1;

/* now find overlapping IP's */

/* DROP TABLE tmp_drg_trig; */

/* this gets the trigger claims */
CREATE TABLE tmp_drg_trig AS (
SELECT DISTINCT
e.master_episode_id,
cl.begin_date,
cl.end_date
FROM episode e
JOIN claim_line cl ON e.master_claim_id=cl.master_claim_id
AND e.episode_id IN ('EP0807','EP0812','EP0806','EP0813')
);


update /*+direct*/  filtered_episodes fe
SET drg=1
FROM (
SELECT tm.master_episode_id FROM tmp_drg_trig tm
JOIN assignment sig ON tm.master_episode_id=sig.master_episode_id
JOIN claim_line cl ON cl.master_claim_id=sig.master_claim_id
AND cl.claim_line_type_code='IP'
AND cl.ms_drg_code IN ('461','462')
/* overlap */
AND ( 
	(cl.begin_date BETWEEN tm.begin_date AND tm.end_date)
	OR
	(cl.end_date BETWEEN tm.begin_date AND tm.end_date)
	OR
	(cl.begin_date<tm.begin_date AND cl.end_date> tm.end_date)
)
) AS sq
WHERE fe.master_episode_id=sq.master_episode_id
AND fe.filter_id=1;

DROP TABLE tmp_drg_trig;

/* ==== END ==== 
DRG Filtering 
*/

/* Make sure the filter_fail line which lets us check one column for all filters is up to date */

update /*+direct*/  filtered_episodes
SET filter_fail=1 
WHERE 
filter_id=1
AND 
(
age_limit_lower=1
OR age_limit_upper=1
OR episode_cost_upper=1
OR episode_cost_lower=1
OR coverage_gap=1
OR episode_complete=1
OR drg=1
OR proc_ep_orphan=1
);


/* ==== BEGIN ==== 
Episode Cost Filtering 
*/

TRUNCATE TABLE tmp_hlow;

/* Need to remove zero (or lower) cost episodes first… */
update /*+direct*/  filtered_episodes fe
SET episode_cost_lower=1,
episode_cost_upper=1
FROM master_epid_level mel 
WHERE fe.master_episode_id=mel.master_episode_id
AND mel.filter_id = 0
AND mel.level = 1
AND mel.split = 1
AND mel.annualized = 0
AND mel.claim_type = 'CL'
AND mel.cost <= 0
AND fe.filter_id=1;

/* set group_concat_max_len = 10485760; */
/* set group_concat_max_len = 10000000000; */

/* do cost filters for unsplit non-annualized */ 
/*INSERT /*+ direct */  INTO tmp_hlow
SELECT
e.episode_id,
cast(substring_index(substring_index(
	group_concat(mel.cost order by mel.cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal(26,10)) AS 'low',
cast(substring_index(substring_index(
	group_concat(mel.cost order by mel.cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal(26,10)) AS 'high'
FROM master_epid_level mel
JOIN episode e ON e.master_episode_id = mel.master_episode_id
AND mel.filter_id = 0
AND mel.level=1
AND mel.split = 0
AND mel.annualized = 0
AND mel.claim_type='CL'
JOIN filtered_episodes fe ON fe.master_episode_id=mel.master_episode_id
AND fe.filter_id=1
AND fe.episode_cost_lower=0
AND fe.proc_ep_orphan=0
GROUP BY e.episode_id;*/

INSERT /*+ direct */  INTO tmp_hlow
SELECT
DISTINCT
e.episode_id,
PERCENTILE_DISC(.01) WITHIN GROUP (order by mel.cost)
OVER (PARTITION by left(e.episode_id,6) ) as low,
PERCENTILE_DISC(.99) WITHIN GROUP (order by mel.cost)
OVER (PARTITION by left(e.episode_id,6) ) as high
FROM master_epid_level mel
JOIN episode e ON e.master_episode_id = mel.master_episode_id
AND mel.filter_id = 0
AND mel.level=1
AND mel.split = 0
AND mel.annualized = 0
AND mel.claim_type='CL'
JOIN filtered_episodes fe ON fe.master_episode_id=mel.master_episode_id
AND fe.filter_id=1
AND fe.episode_cost_lower=0
AND fe.proc_ep_orphan=0;


/* update  episodes where the cost is below the 1% threshhold */


CREATE TABLE tmp_sq AS
SELECT e.master_episode_id FROM episode e
JOIN master_epid_level mel ON mel.master_episode_id = e.master_episode_id
JOIN tmp_hlow th ON th.episode_id=e.episode_id
AND mel.filter_id = 0
AND mel.level=1
AND mel.split = 0
AND mel.annualized = 0
AND mel.claim_type='CL'
AND mel.cost<=th.low;

update /*+direct*/  filtered_episodes fe
SET episode_cost_lower=1
FROM (SELECT master_episode_id FROM tmp_sq) AS sq
WHERE fe.master_episode_id=sq.master_episode_id
AND fe.filter_id=1;

DROP TABLE tmp_sq;




/* update episodes where the cost is above the 99% threshhold */


CREATE TABLE tmp_sq AS 
SELECT e.master_episode_id FROM episode e
JOIN master_epid_level mel ON mel.master_episode_id = e.master_episode_id
JOIN tmp_hlow th ON th.episode_id=e.episode_id
AND mel.filter_id = 0
AND mel.level=1
AND mel.split = 0
AND mel.annualized = 0
AND mel.claim_type='CL'
AND mel.cost>=th.high;

update /*+direct*/  filtered_episodes fe
SET episode_cost_upper=1
FROM (SELECT master_episode_id FROM tmp_sq) AS sq
WHERE fe.master_episode_id=sq.master_episode_id
AND fe.filter_id=1;

DROP TABLE tmp_sq;

/* Need to enter rows into the filtered_episodes table for annualized and split */



/* ==== END ==== 
Episode Cost Filtering 
*/


/*level 2 del filtering*/
drop table if exists LVL2_del_filter_1;
create table LVL2_del_filter_1 as
select
case when left(child.parent_master_episode_id,6)='EX1401' then episode.master_episode_id 
when parent.child_master_episode_id is null and child.parent_master_episode_id is null then episode.master_episode_id
when parent.child_master_episode_id =''  and child.parent_master_episode_id ='' then episode.master_episode_id
when parent.child_master_episode_id is null  then child.parent_master_episode_id 
when parent.child_master_episode_id =''  then child.parent_master_episode_id 

else episode.master_episode_id end as master_episode_id,
sum(case when master_epid_level.level=2 then master_epid_level.cost else 0 end) as cost,
parent.child_master_episode_id,
case when sum( filtered_episodes.age_limit_lower )<count(*) then 0 else 1 end as age_limit_lower	,
case when sum( filtered_episodes.age_limit_upper )<count(*)  then 0 else 1  end as age_limit_upper	,
case when sum(filtered_episodes.episode_cost_upper )<count(*)  then 0 else 1  end as episode_cost_upper	,
case when sum(filtered_episodes.episode_cost_lower)<count(*)  then 0 else 1  end as episode_cost_lower	,
case when sum( filtered_episodes.coverage_gap)=0 then 0 else 1  end as coverage_gap	,
case when sum( filtered_episodes.episode_complete )=0 then 0 else 1 end  as episode_complete	,
case when sum( filtered_episodes.drg )=0 then 0 else 1  end as drg	,
case when sum(  filtered_episodes.proc_ep_orphan )<count(*)  then 0 else 1  end as proc_ep_orphan	,
case when sum(filtered_episodes.proc_ep_orph_triggered )<count(*)  then 0 else 1  end as proc_ep_orph_triggered	

from episode
left join master_epid_level
on episode.master_episode_id=master_epid_level.master_episode_id
left join association parent
on episode.master_episode_id=parent.parent_master_episode_id  and   parent.association_level=2
left join filtered_episodes
on  episode.master_episode_id=filtered_episodes.master_episodE_id
left join association child
on episode.master_episode_id=child.child_master_episode_id and   child.association_level=2

where master_epid_level.split=0  and filtered_episodes.filter_id=1 and master_epid_level.filter_id=0 and episode.episode_id in ('EP1403','EP1404') 
group by episode.master_episode_id, child.parent_master_episode_id, parent.child_master_episode_id

;


drop table if exists LVL2_del_filter;
create table LVL2_del_filter as
select 
master_episode_id,
sum(cost),
tmp_hlow.low,
tmp_hlow.high,
case when sum(age_limit_lower)<2 then 0 else 1 end as age_limit_lower	,
case when sum(age_limit_upper)<2 then 0 else 1  end as age_limit_upper	,
case when sum(cost)>tmp_hlow.high then 1 else 0 end as episode_cost_upper,
case when sum(cost)<tmp_hlow.low then 1 else 0 end as episode_cost_lower,
case when sum(coverage_gap)=0 then 0 else 1  end as coverage_gap	,
case when sum(episode_complete)=0 then 0 else 1 end  as episode_complete	,
case when sum(drg)=0 then 0 else 1  end as drg	,
case when sum(proc_ep_orphan)<2 then 0 else 1  end as proc_ep_orphan	,
case when sum(proc_ep_orph_triggered)<2 then 0 else 1  end as proc_ep_orph_triggered	

from LVL2_del_filter_1
left join tmp_hlow on left(LVL2_del_filter_1.master_episode_id,6)=tmp_hlow.episode_id
group by master_episode_id, tmp_hlow.low,
tmp_hlow.high;





update /*+direct*/  filtered_episodes fe

SET age_limit_lower=e.age_limit_lower,
 age_limit_upper=e.age_limit_upper,
 episode_cost_upper=e.episode_cost_upper,
 episode_cost_lower=e.episode_cost_lower,
 coverage_gap=e.coverage_gap,
 episode_complete=e.episode_complete,
 drg=e.drg,
 proc_ep_orphan=e.proc_ep_orphan,
 proc_ep_orph_triggered=e.proc_ep_orph_triggered

from LVL2_del_filter e 
where fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
;


drop table if exists LVL2_del_filter_1;
drop table if exists LVL2_del_filter;




/* Make sure the filter_fail line which lets us check one column for all filters is up to date */

update /*+direct*/  filtered_episodes
SET filter_fail=1 
WHERE age_limit_lower=1
OR age_limit_upper=1
OR episode_cost_upper=1
OR episode_cost_lower=1
OR coverage_gap=1
OR episode_complete=1
OR drg=1
OR proc_ep_orphan=1
OR dual_eligible=1;


/* Flag by association - if the episode is a parent or grandparent to any filtered episode, it should gain an incomplete by association flag (which should currently impact removal of episode) 

Also need to make sure only Procedurals are being filtered in p orphans */

/* need to change PREGN and NBORN filters 
PREGN should only flag as filtered if flagged for being incomplete
NBORN should only flag as filtered if high or low cost is flagged
PREGN = EX1401
NBORN = EX1502
*/

/* Unfilter the PREGN's except where incomplete... */
update /*+direct*/  filtered_episodes fe
SET filter_fail=0
FROM (
SELECT master_episode_id FROM episode
WHERE episode_id='EX1401'
) AS sq
WHERE sq.master_episode_id=fe.master_episode_id
AND fe.episode_complete=0 
AND fe.dual_eligible=0;

/* unfilter the NBORN's except where outside of cost bounds... */
update /*+direct*/  filtered_episodes fe
SET filter_fail=0
FROM (
SELECT master_episode_id FROM episode
WHERE episode_id='EX1502'
) AS sq
WHERE sq.master_episode_id=fe.master_episode_id
AND fe.episode_cost_upper=0
AND fe.episode_cost_lower=0 
AND fe.dual_eligible=0;