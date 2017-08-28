/*

The following code is ONLY for default (filter_id=1) filtering!

*/

/*
!!!!!!!!!!!!!!!!!!===========================!!!!!!!!!!!!!!!!!!
!MUST HAVE!:

claims combined needs assigned count!



Version 2 - Only split cost episode filtering, only need
filter 0 all claims (CL) split/unsplit

BEFORE running filter flags for episode cost
MUST HAVE dates (required for annualization as well):
UPDATE run SET
data_start_date = (SELECT min(begin_date) FROM claim_line),
data_end_date = (SELECT max(end_date) FROM claim_line),
data_latest_begin_date = (SELECT max(begin_date) FROM claim_line);

OLD?Level 1 filter_id=0 (all claims) rollups for unsplit, split,
annualized unsplit, and annualized split ?
!!!!!!!!!!!!!!!!!!===========================!!!!!!!!!!!!!!!!!!


*/

/* Start by adding blank rows for all episodes */

/* TRUNCATE filtered_episodes; */

INSERT INTO filtered_episodes (filter_id, master_episode_id)
SELECT DISTINCT 0 AS filter_id, master_episode_id FROM episode;

INSERT INTO filtered_episodes (filter_id, master_episode_id)
SELECT DISTINCT 1 AS filter_id, master_episode_id FROM episode;

/* ==== BEGIN ====
Age Range Filtering
*/



UPDATE filtered_episodes fe
JOIN episode e ON fe.master_episode_id=e.master_episode_id
JOIN member m ON e.member_id=m.member_id
JOIN filter_params fp ON fp.episode_id=e.episode_id
AND ( year(e.episode_begin_date)-m.birth_year < fp.lower_age_limit )
AND fe.filter_id=1
SET fe.`age_limit_lower`=1;

UPDATE filtered_episodes fe
JOIN episode e ON fe.master_episode_id=e.master_episode_id
JOIN member m ON e.member_id=m.member_id
JOIN filter_params fp ON fp.episode_id=e.episode_id
AND ( year(e.episode_begin_date)-m.birth_year > fp.upper_age_limit )
AND fe.filter_id=1
SET fe.`age_limit_upper`=1;

/* Filter out any members who do not have member records */
UPDATE filtered_episodes fe
JOIN episode e ON fe.master_episode_id=e.master_episode_id
AND e.member_id NOT IN
(
	SELECT member_id FROM member
)
AND fe.filter_id=1
SET fe.age_limit_lower=1, fe.age_limit_upper=1;

/* ==== END ====
Age Range Filtering */


/* ==== BEGIN ====
Enrollment Gap Filtering

/* need a query to find episodes with a gap within the time period of the episode...
// !!! isGap MUST BE 1 IN PRODUCTION!
*/
drop table tmp_enroll_gap;

create table  tmp_enroll_gap as

SELECT DISTINCT
episode.episode_begin_date,
episode.episode_end_date, 
datediff(episode.episode_end_date, episode.episode_begin_date) AS episode_length,
sum(case when enrollment.begin_date<(case when build_episode_reference.END_OF_STUDY=1 then DATE_SUB(episode.episode_end_date, interval 365 day)else episode.episode_begin_date end) and enrollment.end_date<(case when build_episode_reference.END_OF_STUDY=1 then DATE_SUB(episode.episode_end_date, interval 365 day)else episode.episode_begin_date end) then 0 else 0 end )+
   sum(case when enrollment.begin_date>(case when build_episode_reference.END_OF_STUDY=1 then DATE_SUB(episode.episode_end_date, interval 365 day)else episode.episode_begin_date end) and enrollment.end_date<episode.episode_end_date then datediff(enrollment.end_date,enrollment.begin_date) else 0 end )+
   
   
   sum(case when enrollment.begin_date<(case when build_episode_reference.END_OF_STUDY=1 then DATE_SUB(episode.episode_end_date, interval 365 day)else episode.episode_begin_date end) and enrollment.end_date>episode.episode_end_date then datediff(episode.episode_end_date,(case when build_episode_reference.END_OF_STUDY=1 then DATE_SUB(episode.episode_end_date, interval 365 day)else episode.episode_begin_date end)) else 0 end )+
   
   sum(case when enrollment.begin_date=(case when build_episode_reference.END_OF_STUDY=1 then DATE_SUB(episode.episode_end_date, interval 365 day)else episode.episode_begin_date end) and enrollment.end_date=episode.episode_end_date then datediff(episode.episode_end_date,(case when build_episode_reference.END_OF_STUDY=1 then DATE_SUB(episode.episode_end_date, interval 365 day)else episode.episode_begin_date end)) else 0 end )+   
       
       sum(case when enrollment.begin_date=(case when build_episode_reference.END_OF_STUDY=1 then DATE_SUB(episode.episode_end_date, interval 365 day)else episode.episode_begin_date end) and enrollment.end_date<episode.episode_end_date then datediff(enrollment.end_date,(case when build_episode_reference.END_OF_STUDY=1 then DATE_SUB(episode.episode_end_date, interval 365 day)else episode.episode_begin_date end)) else 0 end )+
     
     sum(case when enrollment.begin_date>(case when build_episode_reference.END_OF_STUDY=1 then DATE_SUB(episode.episode_end_date, interval 365 day)else episode.episode_begin_date end) and enrollment.end_date=episode.episode_end_date then datediff(episode.episode_end_date,enrollment.begin_date) else 0 end )+
   
   sum(case when enrollment.begin_date>(case when build_episode_reference.END_OF_STUDY=1 then DATE_SUB(episode.episode_end_date, interval 365 day)else episode.episode_begin_date end) and enrollment.begin_date<episode.episode_end_date and enrollment.end_date>episode.episode_end_date then datediff(episode.episode_end_date,enrollment.begin_date) else 0 end )+
                
sum(case when enrollment.begin_date<(case when build_episode_reference.END_OF_STUDY=1 then DATE_SUB(episode.episode_end_date, interval 365 day)else episode.episode_begin_date end) and enrollment.end_date<episode.episode_end_date and enrollment.end_date>(case when build_episode_reference.END_OF_STUDY=1 then DATE_SUB(episode.episode_end_date, interval 365 day)else episode.episode_begin_date end) then datediff(enrollment.end_date,(case when build_episode_reference.END_OF_STUDY=1 then DATE_SUB(episode.episode_end_date, interval 365 day)else episode.episode_begin_date end)) else 0 end )

         

        AS gap_length,
episode.master_episode_id,
episode.member_id
FROM episode 
 
left join build_episode_reference
on episode.episode_id=build_episode_reference.episode_id
JOIN enrollment  ON episode.member_id=enrollment.member_id
where enrollment.isGap = 1 
group by build_episode_reference.END_OF_STUDY, episode.episode_begin_date,
episode.episode_end_date,  episode.master_episode_id,
episode.member_id;

/* look for any gaps that start before the beginning of the episode
// and find the amount of days of gap within the episode (begin episode to end gap)
// and find out if that amount is long enough to filter the episode
*/

drop table if exists tmp_enroll_gap1;
create table tmp_enroll_gap1 as
select
master_episode_id,
member_id,
episode_length,
gap_length,
case when left(master_episode_id,6)='EX1401' then 0
     when gap_length>=episode_length then 1
     when gap_length>=30 and episode_length >= 90 then 1
     when gap_length>=1 and episode_length <= 89 then 1
        else 0 end as filter_fail
from tmp_enroll_gap
;


select * from tmp_enroll_gap1 where filter_id=1 and coverage_gap=1;

UPDATE filtered_episodes fe
JOIN tmp_enroll_gap1 teg ON fe.master_episode_id=teg.master_episode_id
AND fe.filter_id=1
AND teg.filter_fail=1
SET fe.coverage_gap=1;

/* ==== END ====
Enrollment Gap Filtering */




/* ==== BEGIN ====
Procedural Orphan Filter Flagging

need episode type filter
only procedures
*/

TRUNCATE tmp_proc_orph;

INSERT INTO tmp_proc_orph
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

/* Get the confirmations for the IP/OP... */
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_po_conf AS (
SELECT
DISTINCT tpo.master_episode_id
FROM tmp_proc_orph tpo
JOIN assignment sig ON sig.master_episode_id=tpo.master_episode_id
AND sig.master_claim_id<>tpo.master_claim_id
JOIN claim_line cl ON sig.master_claim_id=cl.master_claim_id
AND cl.claim_line_type_code IN ('IP','OP')
AND
( cl.begin_date<=DATE_ADD(tpo.end_date, INTERVAL 2 DAY) AND cl.begin_date>=DATE_ADD(tpo.begin_date, INTERVAL -2 DAY)  )
);

UPDATE tmp_proc_orph tpo
JOIN tmp_po_conf tpc ON tpo.master_episode_id=tpc.master_episode_id
SET tpo.confirmed=1;

DROP TABLE tmp_po_conf;

CREATE TEMPORARY TABLE IF NOT EXISTS tmp_po_conf AS (
SELECT
tpo.master_episode_id,
tpo.begin_date,
DATE_ADD(tpo.begin_date, INTERVAL -2 DAY) AS negbegdate
FROM tmp_proc_orph tpo
JOIN assignment sig ON sig.master_episode_id=tpo.master_episode_id
AND sig.master_claim_id<>tpo.master_claim_id
JOIN claim_line cl ON sig.master_claim_id=cl.master_claim_id
AND tpo.claim_line_type_code='PB'
AND cl.claim_line_type_code IN ('IP','OP')


AND cl.begin_date>=DATE_ADD(tpo.begin_date, INTERVAL -2 DAY)
AND cl.end_date<=DATE_ADD(tpo.end_date, INTERVAL 2 DAY)
);

UPDATE tmp_proc_orph tpo
JOIN tmp_po_conf tpc ON tpo.master_episode_id=tpc.master_episode_id
SET tpo.confirmed=1;

UPDATE filtered_episodes fe
JOIN tmp_proc_orph po ON fe.master_episode_id=po.master_episode_id
AND po.confirmed=0
AND fe.filter_id=1
SET fe.proc_ep_orphan=1;

DROP TABLE tmp_po_conf;

TRUNCATE tmp_proc_orph;

/* Now find any episodes triggered by filtered procedurals and filter them?*/

/* gets the master_episode_id of all episodes triggered by
a procedural orphan for marking as an orphan themselves */
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_trigorph AS (
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

UPDATE filtered_episodes fe
JOIN tmp_trigorph tt ON fe.master_episode_id=tt.master_episode_id
AND fe.filter_id=1
SET fe.proc_ep_orphan=1;

DROP TABLE tmp_trigorph;

TRUNCATE tmp_proc_orph;

TRUNCATE tmp_filt_proc_orp_trig;

/* filtering - condition episodes triggered off filtered p episodes: */
# update trigger table to have correct trig_by_master_episode_id
UPDATE triggers
SET trig_by_master_episode_id = CONCAT(trig_by_episode_id, '_', member_id, '_', claim_id, '_', claim_line_id)
WHERE trig_by_episode_id <> '';

#get the master_episode_ids of the dropped Ps
INSERT INTO tmp_filt_proc_orp_trig
SELECT
master_episode_id
FROM filtered_episodes
WHERE filter_id=1
AND proc_ep_orphan=1;

#mark the master_episode_ids of the episodes triggered by filtered Ps
UPDATE filtered_episodes fe
JOIN triggers t ON t.master_episode_id=fe.master_episode_id
JOIN tmp_filt_proc_orp_trig tt ON t.trig_by_master_episode_id=tt.master_episode_id
AND fe.filter_id=0
SET proc_ep_orph_triggered=1;

/* ==== END ====
Procedural Orphan Code
 */

/* ==== BEGIN ====
Episode Completion Filtering
*/

UPDATE filtered_episodes fe
JOIN episode e ON e.master_episode_id=fe.master_episode_id
AND (
trig_begin_date>(SELECT DATE_ADD(data_latest_begin_date,INTERVAL -1 YEAR) FROM run)
OR trig_begin_date<(SELECT data_start_date FROM run)
)
AND fe.filter_id=1
JOIN `episode_builder-5.4.005`.episode ebe ON e.episode_id=ebe.episode_id
AND ebe.END_OF_STUDY=1
SET episode_complete=1;


UPDATE filtered_episodes fe
JOIN episode e ON e.master_episode_id=fe.master_episode_id
AND
(
episode_begin_date<(SELECT data_start_date FROM run)
OR episode_end_date>(SELECT data_latest_begin_date FROM run)
)
AND fe.filter_id=1
JOIN `episode_builder-5.4.005`.episode ebe ON e.episode_id=ebe.episode_id
AND ebe.END_OF_STUDY<>1
SET episode_complete=1;

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

UPDATE filtered_episodes fe
JOIN episode e ON e.master_episode_id=fe.master_episode_id
JOIN claim_line cl ON e.master_claim_id=cl.master_claim_id
AND e.episode_id IN ('EP0807','EP0812','EP0806','EP0813')
AND cl.claim_line_type_code='IP'
AND cl.apr_drg_code IN ('461','462')
AND fe.filter_id=1
SET drg=1;

/* now find overlapping IP's */

/* DELETE TABLE tmp_drg_trig; */

/* this gets the trigger claims */
CREATE TEMPORARY TABLE IF NOT EXISTS tmp_drg_trig AS (
SELECT DISTINCT
e.master_episode_id,
cl.begin_date,
cl.end_date
FROM episode e
JOIN claim_line cl ON e.master_claim_id=cl.master_claim_id
AND e.episode_id IN ('EP0807','EP0812','EP0806','EP0813')
);

UPDATE filtered_episodes fe
JOIN tmp_drg_trig tm ON fe.master_episode_id=tm.master_episode_id
JOIN assignment sig ON tm.master_episode_id=sig.master_episode_id
JOIN claim_line cl ON cl.master_claim_id=sig.master_claim_id
AND cl.claim_line_type_code='IP'
AND cl.apr_drg_code IN ('461','462')
AND cl.begin_date<=tm.end_date
AND cl.end_date>=tm.begin_date
AND fe.filter_id=1
SET drg=1;

/* ==== END ====
DRG Filtering
*/

/* Make sure the filter_fail line which lets us check one column for all filters is up to date */



/* ==== BEGIN ====
Episode Cost Filtering
*/

TRUNCATE tmp_hlow;

/* Need to remove zero (or lower) cost episodes first? */
UPDATE filtered_episodes fe
JOIN master_epid_level mel ON fe.master_episode_id=mel.master_episode_id
AND mel.filter_id = 0
AND mel.level = 1
AND mel.split = 1
AND mel.annualized = 0
AND mel.claim_type = 'CL'
AND mel.cost <= 0
AND fe.filter_id=1
SET fe.episode_cost_lower=1,
fe.episode_cost_upper=1;

/* set group_concat_max_len = 10485760; */
set group_concat_max_len = 10000000000;

/* do cost filters for split non-annualized */
INSERT INTO tmp_hlow
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
AND mel.split = 1
AND mel.annualized = 0
AND mel.claim_type='CL'
JOIN filtered_episodes fe ON fe.master_episode_id=mel.master_episode_id
AND fe.filter_id=1
AND fe.episode_cost_lower=0
AND fe.proc_ep_orphan=0
GROUP BY e.episode_id;


/* Update episodes where the cost is below the 1% threshhold */
UPDATE filtered_episodes fe
JOIN episode e ON e.master_episode_id=fe.master_episode_id
JOIN master_epid_level mel ON mel.master_episode_id = e.master_episode_id
JOIN tmp_hlow th ON th.episode_id=e.episode_id
AND mel.filter_id = 0
AND mel.level=1
AND mel.split = 1
AND mel.annualized = 0
AND mel.claim_type='CL'
AND mel.cost<=th.low
AND fe.filter_id=1
SET fe.episode_cost_lower=1;

/* Update episodes where the cost is above the 99% threshhold */
UPDATE filtered_episodes fe
JOIN episode e ON e.master_episode_id=fe.master_episode_id
JOIN master_epid_level mel ON mel.master_episode_id = e.master_episode_id
JOIN tmp_hlow th ON th.episode_id=e.episode_id
AND mel.filter_id = 0
AND mel.level=1
AND mel.split = 1
AND mel.annualized = 0
AND mel.claim_type='CL'
AND mel.cost>=th.high
AND fe.filter_id=1
SET fe.episode_cost_upper=1;

/* Need to enter rows into the filtered_episodes table for annualized and split */

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
group by episode.master_episode_id;

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
group by master_episode_id;

create index flt on LVL2_del_filter(master_episode_id);


UPDATE filtered_episodes fe
JOIN LVL2_del_filter e ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
SET fe.`age_limit_lower`=e.age_limit_lower,
 fe.`age_limit_upper`=e.age_limit_upper,
 fe.`episode_cost_upper`=e.episode_cost_upper,
 fe.`episode_cost_lower`=e.episode_cost_lower,
 fe.`coverage_gap`=e.coverage_gap,
 fe.`episode_complete`=e.episode_complete,
 fe.`drg`=e.drg,
 fe.`proc_ep_orphan`=e.proc_ep_orphan,
 fe.`proc_ep_orph_triggered`=e.proc_ep_orph_triggered
;


drop table if exists LVL2_del_filter_1;
drop table if exists LVL2_del_filter;

/* ==== END ====
Episode Cost Filtering
*/

/* Make sure the filter_fail line which lets us check one column for all filters is up to date */

UPDATE filtered_episodes
SET filter_fail=1
WHERE left(master_episode_id,6) <> 'EX1401' and age_limit_lower=1
OR age_limit_upper=1
OR episode_cost_upper=1
OR episode_cost_lower=1
OR coverage_gap=1
OR episode_complete=1
OR drg=1
OR proc_ep_orphan=1;

UPDATE filtered_episodes
SET filter_fail=1
WHERE left(master_episode_id,6) = 'EX1401' and age_limit_lower=1
OR age_limit_upper=1
OR episode_cost_upper=1
OR episode_cost_lower=1
OR episode_complete=1
OR drg=1
OR proc_ep_orphan=1;

/* Flag by association - if the episode is a parent or grandparent to any filtered episode, it should gain an incomplete by association flag (which should currently impact removal of episode)

Also need to make sure only Procedurals are being filtered in p orphans */