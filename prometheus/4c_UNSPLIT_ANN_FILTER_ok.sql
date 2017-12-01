set search_path=epbuilder;

/*IP CLAIM ROLL UP*/
TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;



/*
========================================
Begin of finding annualizable episodes and all children
========================================
*/

/* first find all parent episodes (the ones we will actually annualize costs on) */
insert /*+ direct */  INTO tmp_annids 
SELECT 
e.master_episode_id,
'P' AS parent_child_id
FROM episode e 
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN episode_builder_5_4_002.episode eb ON eb.episode_id=e.episode_id
WHERE eb.END_OF_STUDY=1
AND eb.version=(SELECT max(version) FROM episode_builder_5_4_002.episode)
AND trig_begin_date<
(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run)
AND e.member_id NOT IN (SELECT member_id FROM tmp_member_purge);



/* get first generational children of parents… */
insert /*+ direct */  INTO tmp_annids_c
SELECT  distinct
soc.child_master_episode_id AS master_episode_id,
2 as lvl
FROM association soc
JOIN tmp_annids t ON soc.parent_master_episode_id = t.master_episode_id
JOIN episode e ON soc.child_master_episode_id = e.master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;


insert /*+ direct */  INTO tmp_annids
SELECT DISTINCT
master_episode_id,
'C' AS parent_child_id
FROM tmp_annids_c;

TRUNCATE TABLE tmp_annids_c;
 
/*
CREATE TEMPORARY TABLE tmp_ids AS 
SELECT DISTINCT * FROM tmp_annids;

TRUNCATE TABLE tmp_annids;

insert /*+ direct */  INTO tmp_annids
SELECT 
master_episode_id,
MIN(parent_child_id)
FROM tmp_ids
GROUP BY master_episode_id;

DROP TABLE tmp_ids;

*/
/*
========================================
END of finding annualizable episodes and all children
========================================
*/


/*
========================================
BEGIN finding in-level association of annualizables
========================================
*/

/* The following code sets up the tmp_sub_association table
to get all the costs of episodes that have sub-association within the same level (chains of parent>child>grandchild etc within the same level */

/* DROP TABLE tmp_uber; */
 
TRUNCATE TABLE tmp_uber;
TRUNCATE TABLE tmp_sub_association;

/* this gets all episode who do not have a parent at their level of association */



insert /*+ direct */  INTO tmp_uber
SELECT DISTINCT
a1.*
FROM association a1
JOIN tmp_annids ta1 ON ta1.master_episode_id=a1.parent_master_episode_id
JOIN tmp_annids ta2 ON ta2.master_episode_id=a1.child_master_episode_id
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=2
)
AND a1.association_level=2;

insert /*+ direct */  INTO tmp_uber
SELECT DISTINCT
a1.*
FROM association a1
JOIN tmp_annids ta1 ON ta1.master_episode_id=a1.parent_master_episode_id
JOIN tmp_annids ta2 ON ta2.master_episode_id=a1.child_master_episode_id
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=3
)
AND a1.association_level=3;

insert /*+ direct */  INTO tmp_uber
SELECT DISTINCT
a1.*
FROM association a1
JOIN tmp_annids ta1 ON ta1.master_episode_id=a1.parent_master_episode_id
JOIN tmp_annids ta2 ON ta2.master_episode_id=a1.child_master_episode_id
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=4
)
AND a1.association_level=4;

insert /*+ direct */  INTO tmp_uber
SELECT DISTINCT
a1.*
FROM association a1
JOIN tmp_annids ta1 ON ta1.master_episode_id=a1.parent_master_episode_id
JOIN tmp_annids ta2 ON ta2.master_episode_id=a1.child_master_episode_id
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=5
)
AND a1.association_level=5;

/* this should get the grandchildren*/
insert /*+ direct */  INTO tmp_sub_association 
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
1 AS association_sub_level
FROM tmp_uber u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
2 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;


insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
3 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=2
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
4 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=3
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
5 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=4
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
6 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=5
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
7 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=6
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
8 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=7
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
9 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=8
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
10 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=9
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
11 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=10
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
12 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=11
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
13 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=12
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
14 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=13
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
15 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=14
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
16 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=15
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
17 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=16
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
18 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=17
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
19 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=18
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
20 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=19
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
21 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=20
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
22 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=21
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
23 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=22
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
24 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=23
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
25 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=24
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
26 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=25
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
27 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=26
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
28 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=27
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
29 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=28
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
30 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=29
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
31 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=30
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

/*SET @maxLvl = (SELECT max(association_sub_level) FROM tmp_sub_association);*/

update /*+ direct */  tmp_sub_association
SET association_sub_level_inv=
ABS(association_sub_level-(1+ (SELECT max(association_sub_level) FROM tmp_sub_association) ));


/*
========================================
END finding in-level association of annualizables
========================================
*/

/*
========================================
BEGIN level 1 costs and blank row setup in MEL
========================================
*/
/*
SETUP level 1 costs and blank rows for successive levels
*/
CREATE TABLE IF NOT EXISTS tmp_melfil AS (
SELECT 
mel.*
FROM master_epid_level_claim_type mel
JOIN tmp_annids ta ON ta.master_episode_id=mel.master_episode_id
AND mel.filter_id=1
AND mel.claim_type='CL'
AND mel.level=1
AND mel.split=0
AND mel.annualized=0
);

/*
insert /*+ direct */  INTO master_epid_level
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'IP' AS claim_type,
1 AS 'level',
0 AS split,
1 AS annualized,
cost,
cost_t,
cost_tc,
cost_c,
risk_factor_count,
sub_type_count,
probability_of_complications,
IP_stay_count,
IP_stay_facility_costs,
IP_stay_prof_costs,
IP_stay_total_costs,
IP_stay_bed_days,
IP_stay_avg_length
FROM tmp_melfil;

*/

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
e.master_episode_id,
'IP' AS claim_type,
1 AS 'level',
0 AS split,
1 AS annualized,
SUM(CASE
	WHEN allowed_amt IS NULL or cc.claim_line_type_code<>'IP' THEN 0.0000 ELSE allowed_amt
END) AS cost,
SUM(CASE
	WHEN sig.assigned_type='T' THEN 
	CASE WHEN cc.allowed_amt IS NULL  or cc.claim_line_type_code<>'IP'  THEN 0 ELSE cc.allowed_amt END 
	ELSE 0 
END) AS cost_t,
SUM(CASE
	WHEN sig.assigned_type='TC' THEN 
	CASE WHEN cc.allowed_amt IS NULL  or cc.claim_line_type_code<>'IP' THEN 0 ELSE cc.allowed_amt END 
	ELSE 0 
END) AS cost_tc,
SUM(CASE
	WHEN sig.assigned_type='C' THEN 
	CASE WHEN cc.allowed_amt IS NULL  or cc.claim_line_type_code<>'IP' THEN 0 ELSE cc.allowed_amt END 
	ELSE 0 
END) AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length 
FROM tmp_melfil e
LEFT OUTER JOIN assignment sig ON e.master_episode_id = sig.master_episode_id
LEFT OUTER JOIN claims_combined cc
ON sig.master_claim_id=cc.master_claim_id 
WHERE cc.begin_date>(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run) 
GROUP BY e.master_episode_id;


insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'IP' AS claim_type,
2 AS 'level',
0 AS split,
1 AS annualized,
0 AS cost,
0 AS cost_t,
0 AS cost_tc,
0 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length
FROM tmp_melfil;

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'IP' AS claim_type,
3 AS 'level',
0 AS split,
1 AS annualized,
0 AS cost,
0 AS cost_t,
0 AS cost_tc,
0 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length
FROM tmp_melfil;

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'IP' AS claim_type,
4 AS 'level',
0 AS split,
1 AS annualized,
0 AS cost,
0 AS cost_t,
0 AS cost_tc,
0 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length
FROM tmp_melfil;

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'IP' AS claim_type,
5 AS 'level',
0 AS split,
1 AS annualized,
0 AS cost,
0 AS cost_t,
0 AS cost_tc,
0 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length
FROM tmp_melfil;

DROP TABLE tmp_melfil;


/*
========================================
END level 1 costs and blank row setup in MEL
========================================
*/

/*
========================================
BEGIN Level II Cost Rollup
========================================
*/

/* LEVEL 2 SURVIVING (NON-CHILD) EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM master_epid_level_claim_type mel
	WHERE master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=2
	)
	AND mel.level = 1
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP'
);

update /*+ direct */  master_epid_level_claim_type
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP') AS t2
WHERE master_epid_level_claim_type.master_episode_id=t2.master_episode_id
AND level = 2
AND split = 0
AND annualized = 1
AND filter_id = 1
AND claim_type = 'IP';

TRUNCATE TABLE tmp_mel1;

/* MUST RUN the SAME LEVEL SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL II SUB ROLLUP */

/* at a given mel level, all the children can take on their own costs from mel */
update /*+ direct */  tmp_sub_association
SET cost=mel.cost,
cost_t=(mel.cost_t + mel.cost_tc),
cost_tc=0.0000,
cost_c=mel.cost_c
FROM master_epid_level_claim_type mel 
WHERE child_master_episode_id=mel.master_episode_id
	AND mel.level = 1
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP'
	AND association_level=2;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other query… */

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=32
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 31
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 31
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=31
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 30
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 30
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=30
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 29
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 29
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=29
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 28
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 28
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=28
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 27
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 27
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=27
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 26
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 26
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=26
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 25
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 25
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=25
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 24
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 24
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=24
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 23
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 23
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=23
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 22
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 22
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=22
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 21
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 21
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=21
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 20
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 20
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=20
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 19
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 19
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=19
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 19
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 18
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=18
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 17
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 17
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=17
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 16
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 16
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=16
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 15
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 15
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=15
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 14
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 14
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=14
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 13
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 13
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=13
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 12
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 12
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=12
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 11
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 11
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=11
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 10
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 10
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=10
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 9
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 9
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=9
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 8
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 8
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=8
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 7
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 7
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=7
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 6
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 6
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=6
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 5
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 5
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=5
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 4
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 4
AND s1.association_level = 2;


TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=4
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 3
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 3
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=3
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 2
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 2
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=2
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 1
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 1
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* <END> THIS WHOLE THING IS CURRENT JUST LEVEL II SUB ROLLUP */

/* LVL2 CHILD EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
e.master_episode_id,
SUM(mel.cost) as 'cost',
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN (mel.cost_t + mel.cost_tc)
	ELSE 0.0000
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN mel.cost_c
	ELSE (mel.cost_t + mel.cost_tc + mel.cost_c) 
END) AS cost_c
FROM episode e 
JOIN association soc ON e.master_episode_id = soc.parent_master_episode_id
AND soc.association_level=2 
JOIN filtered_episodes fe ON fe.master_episode_id=soc.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 1
AND mel.split = 0
AND mel.annualized = 1
AND mel.filter_id = 1
AND mel.claim_type = 'IP'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=2)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 2
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'IP';

TRUNCATE TABLE tmp_mel1;

update /*+ direct */  master_epid_level_claim_type 
SET cost=st.cost, 
cost_t = st.cost_t, 
cost_c = st.cost_c
FROM (
	SELECT sa.uber_master_episode_id, 
	m.cost+sa.cost AS cost, 
	m.cost_t+sa.cost_t AS cost_t, 
	m.cost_c+sa.cost_c AS cost_c
	FROM (
		SELECT 
		uber_master_episode_id,
		SUM(cost) AS cost,
		SUM(cost_t) AS cost_t,
		SUM(cost_c) AS cost_c
		FROM tmp_sub_association
		WHERE association_level=2
		AND association_sub_level=1
		GROUP BY uber_master_episode_id
	) AS sa 
	JOIN master_epid_level_claim_type m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 2
	AND m.split = 0
	AND m.annualized = 1
	AND m.filter_id = 1
	AND m.claim_type = 'IP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 2
AND split = 0
AND annualized = 1
AND filter_id = 1
AND claim_type = 'IP';


/*==============================*/
/*==============================*/
/* LEVEL III LEVEL 3 LEVEL III LEVEL 3 */
/*==============================*/
/*==============================*/
/* LEVEL 3 SURVIVING (NON-CHILD) EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM master_epid_level_claim_type mel
	WHERE master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=3
	)
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP'
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'IP';

TRUNCATE TABLE tmp_mel1;

/* MUST RUN the LEVEL III SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL 3 SUB ROLLUP */

/* at a given mel level, all the children can take on their own costs from mel */
update /*+ direct */  tmp_sub_association
SET cost=mel.cost,
cost_t=(mel.cost_t + mel.cost_tc),
cost_tc=0.0000,
cost_c=mel.cost_c
FROM master_epid_level_claim_type mel 
WHERE child_master_episode_id=mel.master_episode_id
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP'
	AND association_level=3;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other query… */

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=32
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 31
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 31
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=31
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 30
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 30
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=30
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 29
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 29
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=29
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 28
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 28
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=28
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 27
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 27
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=27
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 26
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 26
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=26
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 25
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 25
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=25
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 24
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 24
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=24
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 23
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 23
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=23
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 22
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 22
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=22
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 21
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 21
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=21
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 20
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 20
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=20
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 19
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 19
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=19
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 18
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 18
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=18
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 17
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 17
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=17
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 16
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 16
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=16
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 15
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 15
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=15
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 14
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 14
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=14
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 13
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 13
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=13
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 12
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 12
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=12
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 11
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 11
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=11
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 10
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 10
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=10
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 9
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 9
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=9
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 8
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 8
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=8
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 7
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 7
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=7
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 6
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 6
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=6
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 5
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 5
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=5
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 4
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 4
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=4
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 3
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 3
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=3
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 2
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 2
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=2
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 1
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 1
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* <END> THIS WHOLE THING IS CURRENT JUST LEVEL 3 SUB ROLLUP */


/* LVL3 CHILD EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
e.master_episode_id,
SUM(mel.cost) as 'cost',
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN (mel.cost_t + mel.cost_tc)
	ELSE 0.0000
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN mel.cost_c
	ELSE (mel.cost_t + mel.cost_tc + mel.cost_c) 
END) AS cost_c
FROM episode e 
JOIN association soc ON e.master_episode_id = soc.parent_master_episode_id
AND soc.association_level=3 
JOIN filtered_episodes fe ON fe.master_episode_id=soc.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 1
AND mel.filter_id = 1
AND mel.claim_type = 'IP'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=3)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'IP';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
update /*+ direct */  master_epid_level_claim_type 
SET cost=st.cost, 
cost_t = st.cost_t, 
cost_c = st.cost_c
FROM (
	SELECT sa.uber_master_episode_id, 
	m.cost+sa.cost AS cost, 
	m.cost_t+sa.cost_t AS cost_t, 
	m.cost_c+sa.cost_c AS cost_c
	FROM (
		SELECT 
		uber_master_episode_id,
		SUM(cost) AS cost,
		SUM(cost_t) AS cost_t,
		SUM(cost_c) AS cost_c
		FROM tmp_sub_association
		WHERE association_level=3
		AND association_sub_level=1
		GROUP BY uber_master_episode_id
	) AS sa 
	JOIN master_epid_level_claim_type m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 3
	AND m.split = 0
	AND m.annualized = 1
	AND m.filter_id = 1
	AND m.claim_type = 'IP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 3
AND split = 0
AND annualized = 1
AND filter_id = 1
AND claim_type = 'IP';

/*==============================*/
/*==============================*/
/* LEVEL IV LEVEL 4 LEVEL IV LEVEL 4 */
/*==============================*/
/*==============================*/
/* LEVEL 4 SURVIVING (NON-CHILD) EPISODE COSTS... */

insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM master_epid_level_claim_type mel
	WHERE master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=4
	)
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP'
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'IP';

TRUNCATE TABLE tmp_mel1;

/* MUST RUN the LEVEL 4 SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL 4 SUB ROLLUP */

/* at a given mel level, all the children can take on their own costs from mel */
update /*+ direct */  tmp_sub_association
SET cost=mel.cost,
cost_t=(mel.cost_t + mel.cost_tc),
cost_tc=0.0000,
cost_c=mel.cost_c
FROM master_epid_level_claim_type mel 
WHERE child_master_episode_id=mel.master_episode_id
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP'
	AND association_level=4;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other query… */

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=32
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 31
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 31
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=31
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 30
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 30
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=30
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 29
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 29
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=29
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 28
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 28
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=28
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 27
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 27
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=27
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 26
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 26
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=26
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 25
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 25
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=25
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 24
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 24
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=24
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 23
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 23
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=23
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 22
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 22
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=22
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 21
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 21
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=21
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 20
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 20
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=20
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 19
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 19
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=19
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 18
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 18
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=18
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 17
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 17
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=17
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 16
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 16
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=16
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 15
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 15
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=15
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 14
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 14
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=14
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 13
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 13
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=13
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 12
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 12
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=12
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 11
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 11
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=11
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 10
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 10
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=10
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 9
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 9
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=9
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 8
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 8
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=8
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 7
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 7
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=7
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 6
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 6
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=6
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 5
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 5
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=5
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 4
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 4
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=4
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 3
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 3
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=3
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 2
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 2
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=2
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 1
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 1
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* <END> THIS WHOLE THING IS CURRENT JUST LEVEL 4 SUB ROLLUP */


/* LVL4 CHILD EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
e.master_episode_id,
SUM(mel.cost) as 'cost',
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN (mel.cost_t + mel.cost_tc)
	ELSE 0.0000
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN mel.cost_c
	ELSE (mel.cost_t + mel.cost_tc + mel.cost_c) 
END) AS cost_c
FROM episode e 
JOIN association soc ON e.master_episode_id = soc.parent_master_episode_id
AND soc.association_level=4 
JOIN filtered_episodes fe ON fe.master_episode_id=soc.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 3
AND mel.split = 0
AND mel.annualized = 1
AND mel.filter_id = 1
AND mel.claim_type = 'IP'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=2)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'IP';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
update /*+ direct */  master_epid_level_claim_type 
SET cost=st.cost, 
cost_t = st.cost_t, 
cost_c = st.cost_c
FROM (
	SELECT sa.uber_master_episode_id, 
	m.cost+sa.cost AS cost, 
	m.cost_t+sa.cost_t AS cost_t, 
	m.cost_c+sa.cost_c AS cost_c
	FROM (
		SELECT 
		uber_master_episode_id,
		SUM(cost) AS cost,
		SUM(cost_t) AS cost_t,
		SUM(cost_c) AS cost_c
		FROM tmp_sub_association
		WHERE association_level=4
		AND association_sub_level=1
		GROUP BY uber_master_episode_id
	) AS sa 
	JOIN master_epid_level_claim_type m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 4
	AND m.split = 0
	AND m.annualized = 1
	AND m.filter_id = 1
	AND m.claim_type = 'IP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 4
AND split = 0
AND annualized = 1
AND filter_id = 1
AND claim_type = 'IP';

/*==============================*/
/*==============================*/
/* LEVEL V LEVEL 5 LEVEL V LEVEL 5  */
/*==============================*/
/*==============================*/
/* LEVEL 5 SURVIVING (NON-CHILD) EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM master_epid_level_claim_type mel
	WHERE master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=5
	)
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP'
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 5
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'IP';

TRUNCATE TABLE tmp_mel1;

/* MUST RUN the LEVEL 5 SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL 5 SUB ROLLUP */

/* at a given mel level, all the children can take on their own costs from mel */
update /*+ direct */  tmp_sub_association
SET cost=mel.cost,
cost_t=(mel.cost_t + mel.cost_tc),
cost_tc=0.0000,
cost_c=mel.cost_c
FROM master_epid_level_claim_type mel 
WHERE child_master_episode_id=mel.master_episode_id
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP'
	AND association_level=5;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other query… */

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=32
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 31
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 31
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=31
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 30
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 30
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=30
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 29
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 29
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=29
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 28
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 28
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=28
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 27
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 27
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=27
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 26
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 26
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=26
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 25
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 25
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=25
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 24
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 24
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=24
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 24
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 24
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=23
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 22
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 22
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=22
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 21
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 21
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=21
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 20
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 20
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=20
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 19
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 19
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=19
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 18
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 18
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=18
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 17
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 17
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=17
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 16
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 16
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=16
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 15
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 15
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=15
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 14
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 14
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=14
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 13
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 13
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=13
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 12
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 12
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=12
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 11
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 11
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=11
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 10
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 10
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=10
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 9
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 9
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=9
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 8
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 8
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=8
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 7
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 7
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=7
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 6
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 6
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=6
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 5
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 5
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=5
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 4
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 4
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=4
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 3
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 3
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=3
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 2
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 2
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=2
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 1
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 1
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* <END> THIS WHOLE THING IS CURRENT JUST LEVEL 5 SUB ROLLUP */


/* LVL5 CHILD EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
e.master_episode_id,
SUM(mel.cost) as 'cost',
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN (mel.cost_t + mel.cost_tc)
	ELSE 0.0000
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN mel.cost_c
	ELSE (mel.cost_t + mel.cost_tc + mel.cost_c) 
END) AS cost_c
FROM episode e 
JOIN association soc ON e.master_episode_id = soc.parent_master_episode_id
AND soc.association_level=5 
JOIN filtered_episodes fe ON fe.master_episode_id=soc.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 4
AND mel.split = 0
AND mel.annualized = 1
AND mel.filter_id = 1
AND mel.claim_type = 'IP'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=2)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 5
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'IP';

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
update /*+ direct */  master_epid_level_claim_type 
SET cost=st.cost, 
cost_t = st.cost_t, 
cost_c = st.cost_c
FROM (
	SELECT sa.uber_master_episode_id, 
	m.cost+sa.cost AS cost, 
	m.cost_t+sa.cost_t AS cost_t, 
	m.cost_c+sa.cost_c AS cost_c
	FROM (
		SELECT 
		uber_master_episode_id,
		SUM(cost) AS cost,
		SUM(cost_t) AS cost_t,
		SUM(cost_c) AS cost_c
		FROM tmp_sub_association
		WHERE association_level=5
		AND association_sub_level=1
		GROUP BY uber_master_episode_id
	) AS sa 
	JOIN master_epid_level_claim_type m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 5
	AND m.split = 0
	AND m.annualized = 1
	AND m.filter_id = 1
	AND m.claim_type = 'IP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 5
AND split = 0
AND annualized = 1
AND filter_id = 1
AND claim_type = 'IP';

TRUNCATE TABLE tmp_mel1;
TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;

/*OP CLAIM ROLL UP*/
TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;

/* !!!NEED TO FIND MAX END DATE TO update /*+ direct */  RUN TABLE! 
IF NOT DONE ALREADY!!!

update /*+ direct */  run
SET data_latest_begin_date = 
(SELECT max(begin_date) FROM claim_line);
*/

/*
========================================
Begin of finding annualizable episodes and all children
========================================
*/

/* first find all parent episodes (the ones we will actually annualize costs on) */
insert /*+ direct */  INTO tmp_annids 
SELECT 
e.master_episode_id,
'P' AS parent_child_id
FROM episode e 
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN episode_builder_5_4_002.episode eb ON eb.episode_id=e.episode_id
WHERE eb.END_OF_STUDY=1
AND eb.version=(SELECT max(version) FROM episode_builder_5_4_002.episode)
AND trig_begin_date<
(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

/* 
DATE_ADD(data_latest_begin_date,INTERVAL -1 YEAR)
TIMESTAMPADD('yyyy', -1, data_latest_begin_date)
*/

/* get first generational children of parents… */
insert /*+ direct */  INTO tmp_annids_c
SELECT  distinct
soc.child_master_episode_id AS master_episode_id,
2 as lvl
FROM association soc
JOIN tmp_annids t ON soc.parent_master_episode_id = t.master_episode_id
JOIN episode e ON soc.child_master_episode_id = e.master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;


insert /*+ direct */  INTO tmp_annids
SELECT DISTINCT
master_episode_id,
'C' AS parent_child_id
FROM tmp_annids_c;

TRUNCATE TABLE tmp_annids_c;
 
/*
CREATE TEMPORARY TABLE tmp_ids AS 
SELECT DISTINCT * FROM tmp_annids;

TRUNCATE TABLE tmp_annids;

insert /*+ direct */  INTO tmp_annids
SELECT 
master_episode_id,
MIN(parent_child_id)
FROM tmp_ids
GROUP BY master_episode_id;

DROP TABLE tmp_ids;

*/
/*
========================================
END of finding annualizable episodes and all children
========================================
*/


/*
========================================
BEGIN finding in-level association of annualizables
========================================
*/

/* The following code sets up the tmp_sub_association table
to get all the costs of episodes that have sub-association within the same level (chains of parent>child>grandchild etc within the same level */

/* DROP TABLE tmp_uber; */
 
TRUNCATE TABLE tmp_uber;
TRUNCATE TABLE tmp_sub_association;

/* this gets all episode who do not have a parent at their level of association */

/*
insert /*+ direct */  INTO tmp_uber
SELECT DISTINCT
a1.*
FROM association a1 */
/* the following 2 lines restricts it to annualizable episodes AND non-filtered episodes */
/* JOIN tmp_annids ta1 ON ta1.master_episode_id=a1.parent_master_episode_id
JOIN tmp_annids ta2 ON ta2.master_episode_id=a1.child_master_episode_id
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=a1.association_level
); */

insert /*+ direct */  INTO tmp_uber
SELECT DISTINCT
a1.*
FROM association a1
JOIN tmp_annids ta1 ON ta1.master_episode_id=a1.parent_master_episode_id
JOIN tmp_annids ta2 ON ta2.master_episode_id=a1.child_master_episode_id
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=2
)
AND a1.association_level=2;

insert /*+ direct */  INTO tmp_uber
SELECT DISTINCT
a1.*
FROM association a1
JOIN tmp_annids ta1 ON ta1.master_episode_id=a1.parent_master_episode_id
JOIN tmp_annids ta2 ON ta2.master_episode_id=a1.child_master_episode_id
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=3
)
AND a1.association_level=3;

insert /*+ direct */  INTO tmp_uber
SELECT DISTINCT
a1.*
FROM association a1
JOIN tmp_annids ta1 ON ta1.master_episode_id=a1.parent_master_episode_id
JOIN tmp_annids ta2 ON ta2.master_episode_id=a1.child_master_episode_id
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=4
)
AND a1.association_level=4;

insert /*+ direct */  INTO tmp_uber
SELECT DISTINCT
a1.*
FROM association a1
JOIN tmp_annids ta1 ON ta1.master_episode_id=a1.parent_master_episode_id
JOIN tmp_annids ta2 ON ta2.master_episode_id=a1.child_master_episode_id
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=5
)
AND a1.association_level=5;

/* this should get the grandchildren*/
insert /*+ direct */  INTO tmp_sub_association 
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
1 AS association_sub_level
FROM tmp_uber u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
2 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;


insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
3 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=2
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
4 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=3
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
5 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=4
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
6 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=5
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
7 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=6
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
8 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=7
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
9 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=8
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
10 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=9
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
11 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=10
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
12 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=11
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
13 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=12
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
14 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=13
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
15 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=14
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
16 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=15
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
17 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=16
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
18 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=17
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
19 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=18
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
20 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=19
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
21 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=20
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
22 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=21
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
23 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=22
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
24 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=23
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
25 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=24
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
26 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=25
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
27 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=26
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
28 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=27
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
29 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=28
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
30 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=29
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
31 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=30
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

/*SET @maxLvl = (SELECT max(association_sub_level) FROM tmp_sub_association);*/

update /*+ direct */  tmp_sub_association
SET association_sub_level_inv=
ABS(association_sub_level-(1+ (SELECT max(association_sub_level) FROM tmp_sub_association) ));


/*
========================================
END finding in-level association of annualizables
========================================
*/

/*
========================================
BEGIN level 1 costs and blank row setup in MEL
========================================
*/
/*
SETUP level 1 costs and blank rows for successive levels
*/

CREATE TABLE IF NOT EXISTS tmp_melfil AS (
SELECT 
mel.*
FROM master_epid_level_claim_type mel
JOIN tmp_annids ta ON ta.master_episode_id=mel.master_episode_id
AND mel.filter_id=1
AND mel.claim_type='CL'
AND mel.level=1
AND mel.split=0
AND mel.annualized=0
);

/*
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'OP' AS claim_type,
1 AS 'level',
0 AS split,
1 AS annualized,
cost,
cost_t,
cost_tc,
cost_c,
risk_factor_count,
sub_type_count,
probability_of_complications,
IP_stay_count,
IP_stay_facility_costs,
IP_stay_prof_costs,
IP_stay_total_costs,
IP_stay_bed_days,
IP_stay_avg_length
FROM tmp_melfil;

*/

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
e.master_episode_id,
'OP' AS claim_type,
1 AS 'level',
0 AS split,
1 AS annualized,
SUM(CASE
	WHEN allowed_amt IS NULL  or cc.claim_line_type_code<>'OP' THEN 0.0000 ELSE allowed_amt
END) AS cost,
SUM(CASE
	WHEN sig.assigned_type='T' THEN 
	CASE WHEN cc.allowed_amt IS NULL   or cc.claim_line_type_code<>'OP'  THEN 0 ELSE cc.allowed_amt END 
	ELSE 0 
END) AS cost_t,
SUM(CASE
	WHEN sig.assigned_type='TC' THEN 
	CASE WHEN cc.allowed_amt IS NULL   or cc.claim_line_type_code<>'OP' THEN 0 ELSE cc.allowed_amt END 
	ELSE 0 
END) AS cost_tc,
SUM(CASE
	WHEN sig.assigned_type='C' THEN 
	CASE WHEN cc.allowed_amt IS NULL   or cc.claim_line_type_code<>'OP' THEN 0 ELSE cc.allowed_amt END 
	ELSE 0 
END) AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length 
FROM tmp_melfil e
LEFT OUTER JOIN assignment sig ON e.master_episode_id = sig.master_episode_id
LEFT OUTER JOIN claims_combined cc
ON sig.master_claim_id=cc.master_claim_id 
WHERE cc.begin_date>(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run) 
GROUP BY e.master_episode_id;


insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'OP' AS claim_type,
2 AS 'level',
0 AS split,
1 AS annualized,
0 AS cost,
0 AS cost_t,
0 AS cost_tc,
0 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length
FROM tmp_melfil;

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'OP' AS claim_type,
3 AS 'level',
0 AS split,
1 AS annualized,
0 AS cost,
0 AS cost_t,
0 AS cost_tc,
0 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length
FROM tmp_melfil;

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'OP' AS claim_type,
4 AS 'level',
0 AS split,
1 AS annualized,
0 AS cost,
0 AS cost_t,
0 AS cost_tc,
0 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length
FROM tmp_melfil;

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'OP' AS claim_type,
5 AS 'level',
0 AS split,
1 AS annualized,
0 AS cost,
0 AS cost_t,
0 AS cost_tc,
0 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length
FROM tmp_melfil;

DROP TABLE tmp_melfil;


/*
========================================
END level 1 costs and blank row setup in MEL
========================================
*/

/*
========================================
BEGIN Level II Cost Rollup
========================================
*/

/* LEVEL 2 SURVIVING (NON-CHILD) EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM master_epid_level_claim_type mel
	WHERE master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=2
	)
	AND mel.level = 1
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP'
);
/*START HERE*/
update /*+ direct */  master_epid_level_claim_type
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP') AS t2
WHERE master_epid_level_claim_type.master_episode_id=t2.master_episode_id
AND level = 2
AND split = 0
AND annualized = 1
AND filter_id = 1
AND claim_type = 'OP';

TRUNCATE TABLE tmp_mel1;

/* MUST RUN the SAME LEVEL SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL II SUB ROLLUP */

/* at a given mel level, all the children can take on their own costs from mel */
update /*+ direct */  tmp_sub_association
SET cost=mel.cost,
cost_t=(mel.cost_t + mel.cost_tc),
cost_tc=0.0000,
cost_c=mel.cost_c
FROM master_epid_level_claim_type mel 
WHERE child_master_episode_id=mel.master_episode_id
	AND mel.level = 1
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP'
	AND association_level=2;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other query… */

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=32
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 31
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 31
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=31
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 30
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 30
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=30
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 29
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 29
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=29
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 28
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 28
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=28
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 27
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 27
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=27
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 26
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 26
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=26
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 25
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 25
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=25
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 24
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 24
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=24
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 23
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 23
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=23
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 22
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 22
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=22
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 21
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 21
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=21
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 20
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 20
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=20
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 19
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 19
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=19
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 19
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 18
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=18
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 17
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 17
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=17
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 16
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 16
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=16
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 15
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 15
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=15
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 14
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 14
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=14
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 13
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 13
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=13
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 12
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 12
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=12
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 11
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 11
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=11
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 10
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 10
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=10
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 9
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 9
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=9
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 8
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 8
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=8
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 7
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 7
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=7
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 6
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 6
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=6
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 5
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 5
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=5
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 4
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 4
AND s1.association_level = 2;


TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=4
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 3
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 3
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=3
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 2
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 2
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=2
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 1
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 1
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* <END> THIS WHOLE THING IS CURRENT JUST LEVEL II SUB ROLLUP */

/* LVL2 CHILD EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
e.master_episode_id,
SUM(mel.cost) as 'cost',
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN (mel.cost_t + mel.cost_tc)
	ELSE 0.0000
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN mel.cost_c
	ELSE (mel.cost_t + mel.cost_tc + mel.cost_c) 
END) AS cost_c
FROM episode e 
JOIN association soc ON e.master_episode_id = soc.parent_master_episode_id
AND soc.association_level=2 
JOIN filtered_episodes fe ON fe.master_episode_id=soc.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 1
AND mel.split = 0
AND mel.annualized = 1
AND mel.filter_id = 1
AND mel.claim_type = 'OP'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=2)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 2
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'OP';

TRUNCATE TABLE tmp_mel1;

update /*+ direct */  master_epid_level_claim_type 
SET cost=st.cost, 
cost_t = st.cost_t, 
cost_c = st.cost_c
FROM (
	SELECT sa.uber_master_episode_id, 
	m.cost+sa.cost AS cost, 
	m.cost_t+sa.cost_t AS cost_t, 
	m.cost_c+sa.cost_c AS cost_c
	FROM (
		SELECT 
		uber_master_episode_id,
		SUM(cost) AS cost,
		SUM(cost_t) AS cost_t,
		SUM(cost_c) AS cost_c
		FROM tmp_sub_association
		WHERE association_level=2
		AND association_sub_level=1
		GROUP BY uber_master_episode_id
	) AS sa 
	JOIN master_epid_level_claim_type m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 2
	AND m.split = 0
	AND m.annualized = 1
	AND m.filter_id = 1
	AND m.claim_type = 'OP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 2
AND split = 0
AND annualized = 1
AND filter_id = 1
AND claim_type = 'OP';


/*==============================*/
/*==============================*/
/* LEVEL III LEVEL 3 LEVEL III LEVEL 3 */
/*==============================*/
/*==============================*/
/* LEVEL 3 SURVIVING (NON-CHILD) EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM master_epid_level_claim_type mel
	WHERE master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=3
	)
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP'
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'OP';

TRUNCATE TABLE tmp_mel1;

/* MUST RUN the LEVEL III SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL 3 SUB ROLLUP */

/* at a given mel level, all the children can take on their own costs from mel */
update /*+ direct */  tmp_sub_association
SET cost=mel.cost,
cost_t=(mel.cost_t + mel.cost_tc),
cost_tc=0.0000,
cost_c=mel.cost_c
FROM master_epid_level_claim_type mel 
WHERE child_master_episode_id=mel.master_episode_id
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP'
	AND association_level=3;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other query… */

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=32
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 31
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 31
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=31
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 30
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 30
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=30
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 29
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 29
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=29
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 28
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 28
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=28
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 27
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 27
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=27
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 26
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 26
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=26
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 25
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 25
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=25
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 24
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 24
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=24
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 23
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 23
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=23
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 22
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 22
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=22
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 21
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 21
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=21
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 20
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 20
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=20
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 19
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 19
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=19
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 18
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 18
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=18
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 17
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 17
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=17
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 16
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 16
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=16
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 15
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 15
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=15
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 14
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 14
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=14
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 13
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 13
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=13
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 12
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 12
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=12
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 11
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 11
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=11
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 10
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 10
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=10
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 9
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 9
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=9
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 8
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 8
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=8
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 7
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 7
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=7
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 6
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 6
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=6
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 5
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 5
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=5
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 4
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 4
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=4
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 3
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 3
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=3
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 2
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 2
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=2
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 1
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 1
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* <END> THIS WHOLE THING IS CURRENT JUST LEVEL 3 SUB ROLLUP */


/* LVL3 CHILD EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
e.master_episode_id,
SUM(mel.cost) as 'cost',
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN (mel.cost_t + mel.cost_tc)
	ELSE 0.0000
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN mel.cost_c
	ELSE (mel.cost_t + mel.cost_tc + mel.cost_c) 
END) AS cost_c
FROM episode e 
JOIN association soc ON e.master_episode_id = soc.parent_master_episode_id
AND soc.association_level=3 
JOIN filtered_episodes fe ON fe.master_episode_id=soc.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 1
AND mel.filter_id = 1
AND mel.claim_type = 'OP'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=3)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'OP';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
update /*+ direct */  master_epid_level_claim_type 
SET cost=st.cost, 
cost_t = st.cost_t, 
cost_c = st.cost_c
FROM (
	SELECT sa.uber_master_episode_id, 
	m.cost+sa.cost AS cost, 
	m.cost_t+sa.cost_t AS cost_t, 
	m.cost_c+sa.cost_c AS cost_c
	FROM (
		SELECT 
		uber_master_episode_id,
		SUM(cost) AS cost,
		SUM(cost_t) AS cost_t,
		SUM(cost_c) AS cost_c
		FROM tmp_sub_association
		WHERE association_level=3
		AND association_sub_level=1
		GROUP BY uber_master_episode_id
	) AS sa 
	JOIN master_epid_level_claim_type m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 3
	AND m.split = 0
	AND m.annualized = 1
	AND m.filter_id = 1
	AND m.claim_type = 'OP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 3
AND split = 0
AND annualized = 1
AND filter_id = 1
AND claim_type = 'OP';

/*==============================*/
/*==============================*/
/* LEVEL IV LEVEL 4 LEVEL IV LEVEL 4 */
/*==============================*/
/*==============================*/
/* LEVEL 4 SURVIVING (NON-CHILD) EPISODE COSTS... */

insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM master_epid_level_claim_type mel
	WHERE master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=4
	)
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP'
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'OP';

TRUNCATE TABLE tmp_mel1;

/* MUST RUN the LEVEL 4 SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL 4 SUB ROLLUP */

/* at a given mel level, all the children can take on their own costs from mel */
update /*+ direct */  tmp_sub_association
SET cost=mel.cost,
cost_t=(mel.cost_t + mel.cost_tc),
cost_tc=0.0000,
cost_c=mel.cost_c
FROM master_epid_level_claim_type mel 
WHERE child_master_episode_id=mel.master_episode_id
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP'
	AND association_level=4;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other query… */

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=32
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 31
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 31
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=31
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 30
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 30
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=30
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 29
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 29
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=29
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 28
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 28
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=28
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 27
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 27
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=27
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 26
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 26
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=26
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 25
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 25
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=25
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 24
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 24
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=24
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 23
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 23
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=23
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 22
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 22
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=22
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 21
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 21
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=21
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 20
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 20
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=20
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 19
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 19
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=19
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 18
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 18
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=18
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 17
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 17
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=17
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 16
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 16
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=16
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 15
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 15
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=15
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 14
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 14
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=14
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 13
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 13
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=13
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 12
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 12
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=12
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 11
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 11
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=11
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 10
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 10
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=10
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 9
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 9
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=9
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 8
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 8
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=8
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 7
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 7
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=7
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 6
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 6
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=6
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 5
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 5
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=5
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 4
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 4
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=4
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 3
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 3
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=3
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 2
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 2
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=2
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 1
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 1
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* <END> THIS WHOLE THING IS CURRENT JUST LEVEL 4 SUB ROLLUP */


/* LVL4 CHILD EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
e.master_episode_id,
SUM(mel.cost) as 'cost',
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN (mel.cost_t + mel.cost_tc)
	ELSE 0.0000
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN mel.cost_c
	ELSE (mel.cost_t + mel.cost_tc + mel.cost_c) 
END) AS cost_c
FROM episode e 
JOIN association soc ON e.master_episode_id = soc.parent_master_episode_id
AND soc.association_level=4 
JOIN filtered_episodes fe ON fe.master_episode_id=soc.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 3
AND mel.split = 0
AND mel.annualized = 1
AND mel.filter_id = 1
AND mel.claim_type = 'OP'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=2)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'OP';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
update /*+ direct */  master_epid_level_claim_type 
SET cost=st.cost, 
cost_t = st.cost_t, 
cost_c = st.cost_c
FROM (
	SELECT sa.uber_master_episode_id, 
	m.cost+sa.cost AS cost, 
	m.cost_t+sa.cost_t AS cost_t, 
	m.cost_c+sa.cost_c AS cost_c
	FROM (
		SELECT 
		uber_master_episode_id,
		SUM(cost) AS cost,
		SUM(cost_t) AS cost_t,
		SUM(cost_c) AS cost_c
		FROM tmp_sub_association
		WHERE association_level=4
		AND association_sub_level=1
		GROUP BY uber_master_episode_id
	) AS sa 
	JOIN master_epid_level_claim_type m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 4
	AND m.split = 0
	AND m.annualized = 1
	AND m.filter_id = 1
	AND m.claim_type = 'OP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 4
AND split = 0
AND annualized = 1
AND filter_id = 1
AND claim_type = 'OP';

/*==============================*/
/*==============================*/
/* LEVEL V LEVEL 5 LEVEL V LEVEL 5  */
/*==============================*/
/*==============================*/
/* LEVEL 5 SURVIVING (NON-CHILD) EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM master_epid_level_claim_type mel
	WHERE master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=5
	)
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP'
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 5
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'OP';

TRUNCATE TABLE tmp_mel1;

/* MUST RUN the LEVEL 5 SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL 5 SUB ROLLUP */

/* at a given mel level, all the children can take on their own costs from mel */
update /*+ direct */  tmp_sub_association
SET cost=mel.cost,
cost_t=(mel.cost_t + mel.cost_tc),
cost_tc=0.0000,
cost_c=mel.cost_c
FROM master_epid_level_claim_type mel 
WHERE child_master_episode_id=mel.master_episode_id
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP'
	AND association_level=5;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other query… */

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=32
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 31
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 31
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=31
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 30
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 30
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=30
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 29
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 29
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=29
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 28
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 28
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=28
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 27
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 27
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=27
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 26
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 26
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=26
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 25
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 25
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=25
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 24
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 24
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=24
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 24
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 24
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=23
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 22
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 22
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=22
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 21
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 21
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=21
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 20
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 20
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=20
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 19
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 19
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=19
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 18
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 18
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=18
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 17
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 17
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=17
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 16
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 16
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=16
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 15
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 15
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=15
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 14
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 14
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=14
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 13
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 13
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=13
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 12
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 12
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=12
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 11
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 11
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=11
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 10
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 10
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=10
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 9
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 9
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=9
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 8
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 8
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=8
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 7
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 7
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=7
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 6
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 6
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=6
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 5
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 5
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=5
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 4
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 4
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=4
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 3
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 3
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=3
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 2
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 2
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=2
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 1
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 1
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* <END> THIS WHOLE THING IS CURRENT JUST LEVEL 5 SUB ROLLUP */


/* LVL5 CHILD EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
e.master_episode_id,
SUM(mel.cost) as 'cost',
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN (mel.cost_t + mel.cost_tc)
	ELSE 0.0000
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN mel.cost_c
	ELSE (mel.cost_t + mel.cost_tc + mel.cost_c) 
END) AS cost_c
FROM episode e 
JOIN association soc ON e.master_episode_id = soc.parent_master_episode_id
AND soc.association_level=5 
JOIN filtered_episodes fe ON fe.master_episode_id=soc.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 4
AND mel.split = 0
AND mel.annualized = 1
AND mel.filter_id = 1
AND mel.claim_type = 'OP'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=2)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 5
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'OP';

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
update /*+ direct */  master_epid_level_claim_type 
SET cost=st.cost, 
cost_t = st.cost_t, 
cost_c = st.cost_c
FROM (
	SELECT sa.uber_master_episode_id, 
	m.cost+sa.cost AS cost, 
	m.cost_t+sa.cost_t AS cost_t, 
	m.cost_c+sa.cost_c AS cost_c
	FROM (
		SELECT 
		uber_master_episode_id,
		SUM(cost) AS cost,
		SUM(cost_t) AS cost_t,
		SUM(cost_c) AS cost_c
		FROM tmp_sub_association
		WHERE association_level=5
		AND association_sub_level=1
		GROUP BY uber_master_episode_id
	) AS sa 
	JOIN master_epid_level_claim_type m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 5
	AND m.split = 0
	AND m.annualized = 1
	AND m.filter_id = 1
	AND m.claim_type = 'OP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 5
AND split = 0
AND annualized = 1
AND filter_id = 1
AND claim_type = 'OP';

TRUNCATE TABLE tmp_mel1;
TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;

/*PB CLAIM ROLL UP*/
TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;

/* !!!NEED TO FIND MAX END DATE TO update /*+ direct */  RUN TABLE! 
IF NOT DONE ALREADY!!!

update /*+ direct */  run
SET data_latest_begin_date = 
(SELECT max(begin_date) FROM claim_line);
*/

/*
========================================
Begin of finding annualizable episodes and all children
========================================
*/

/* first find all parent episodes (the ones we will actually annualize costs on) */
insert /*+ direct */  INTO tmp_annids 
SELECT 
e.master_episode_id,
'P' AS parent_child_id
FROM episode e 
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN episode_builder_5_4_002.episode eb ON eb.episode_id=e.episode_id
WHERE eb.END_OF_STUDY=1
AND eb.version=(SELECT max(version) FROM episode_builder_5_4_002.episode)
AND trig_begin_date<
(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

/* 
DATE_ADD(data_latest_begin_date,INTERVAL -1 YEAR)
TIMESTAMPADD('yyyy', -1, data_latest_begin_date)
*/

/* get first generational children of parents… */
insert /*+ direct */  INTO tmp_annids_c
SELECT  distinct
soc.child_master_episode_id AS master_episode_id,
2 as lvl
FROM association soc
JOIN tmp_annids t ON soc.parent_master_episode_id = t.master_episode_id
JOIN episode e ON soc.child_master_episode_id = e.master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;


insert /*+ direct */  INTO tmp_annids
SELECT DISTINCT
master_episode_id,
'C' AS parent_child_id
FROM tmp_annids_c;

TRUNCATE TABLE tmp_annids_c;
 
/*
CREATE TEMPORARY TABLE tmp_ids AS 
SELECT DISTINCT * FROM tmp_annids;

TRUNCATE TABLE tmp_annids;

insert /*+ direct */  INTO tmp_annids
SELECT 
master_episode_id,
MIN(parent_child_id)
FROM tmp_ids
GROUP BY master_episode_id;

DROP TABLE tmp_ids;

*/
/*
========================================
END of finding annualizable episodes and all children
========================================
*/


/*
========================================
BEGIN finding in-level association of annualizables
========================================
*/

/* The following code sets up the tmp_sub_association table
to get all the costs of episodes that have sub-association within the same level (chains of parent>child>grandchild etc within the same level */

/* DROP TABLE tmp_uber; */
 
TRUNCATE TABLE tmp_uber;
TRUNCATE TABLE tmp_sub_association;

/* this gets all episode who do not have a parent at their level of association */

/*
insert /*+ direct */  INTO tmp_uber
SELECT DISTINCT
a1.*
FROM association a1 */
/* the following 2 lines restricts it to annualizable episodes AND non-filtered episodes */
/* JOIN tmp_annids ta1 ON ta1.master_episode_id=a1.parent_master_episode_id
JOIN tmp_annids ta2 ON ta2.master_episode_id=a1.child_master_episode_id
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=a1.association_level
); */

insert /*+ direct */  INTO tmp_uber
SELECT DISTINCT
a1.*
FROM association a1
JOIN tmp_annids ta1 ON ta1.master_episode_id=a1.parent_master_episode_id
JOIN tmp_annids ta2 ON ta2.master_episode_id=a1.child_master_episode_id
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=2
)
AND a1.association_level=2;

insert /*+ direct */  INTO tmp_uber
SELECT DISTINCT
a1.*
FROM association a1
JOIN tmp_annids ta1 ON ta1.master_episode_id=a1.parent_master_episode_id
JOIN tmp_annids ta2 ON ta2.master_episode_id=a1.child_master_episode_id
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=3
)
AND a1.association_level=3;

insert /*+ direct */  INTO tmp_uber
SELECT DISTINCT
a1.*
FROM association a1
JOIN tmp_annids ta1 ON ta1.master_episode_id=a1.parent_master_episode_id
JOIN tmp_annids ta2 ON ta2.master_episode_id=a1.child_master_episode_id
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=4
)
AND a1.association_level=4;

insert /*+ direct */  INTO tmp_uber
SELECT DISTINCT
a1.*
FROM association a1
JOIN tmp_annids ta1 ON ta1.master_episode_id=a1.parent_master_episode_id
JOIN tmp_annids ta2 ON ta2.master_episode_id=a1.child_master_episode_id
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=5
)
AND a1.association_level=5;

/* this should get the grandchildren*/
insert /*+ direct */  INTO tmp_sub_association 
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
1 AS association_sub_level
FROM tmp_uber u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
2 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;


insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
3 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=2
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
4 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=3
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
5 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=4
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
6 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=5
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
7 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=6
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
8 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=7
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
9 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=8
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
10 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=9
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
11 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=10
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
12 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=11
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
13 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=12
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
14 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=13
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
15 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=14
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
16 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=15
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
17 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=16
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
18 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=17
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
19 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=18
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
20 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=19
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
21 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=20
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
22 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=21
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
23 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=22
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
24 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=23
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
25 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=24
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
26 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=25
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
27 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=26
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
28 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=27
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
29 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=28
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
30 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=29
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
31 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=30
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

/*SET @maxLvl = (SELECT max(association_sub_level) FROM tmp_sub_association);*/

update /*+ direct */  tmp_sub_association
SET association_sub_level_inv=
ABS(association_sub_level-(1+ (SELECT max(association_sub_level) FROM tmp_sub_association) ));


/*
========================================
END finding in-level association of annualizables
========================================
*/

/*
========================================
BEGIN level 1 costs and blank row setup in MEL
========================================
*/
/*
SETUP level 1 costs and blank rows for successive levels
*/

CREATE TABLE IF NOT EXISTS tmp_melfil AS (
SELECT 
mel.*
FROM master_epid_level_claim_type mel
JOIN tmp_annids ta ON ta.master_episode_id=mel.master_episode_id
AND mel.filter_id=1
AND mel.claim_type='CL'
AND mel.level=1
AND mel.split=0
AND mel.annualized=0
);

/*
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'PB' AS claim_type,
1 AS 'level',
0 AS split,
1 AS annualized,
cost,
cost_t,
cost_tc,
cost_c,
risk_factor_count,
sub_type_count,
probability_of_complications,
IP_stay_count,
IP_stay_facility_costs,
IP_stay_prof_costs,
IP_stay_total_costs,
IP_stay_bed_days,
IP_stay_avg_length
FROM tmp_melfil;

*/

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
e.master_episode_id,
'PB' AS claim_type,
1 AS 'level',
0 AS split,
1 AS annualized,
SUM(CASE
	WHEN allowed_amt IS NULL   or cc.claim_line_type_code<>'PB' THEN 0.0000 ELSE allowed_amt
END) AS cost,
SUM(CASE
	WHEN sig.assigned_type='T' THEN 
	CASE WHEN cc.allowed_amt IS NULL    or cc.claim_line_type_code<>'PB' THEN 0 ELSE cc.allowed_amt END 
	ELSE 0 
END) AS cost_t,
SUM(CASE
	WHEN sig.assigned_type='TC' THEN 
	CASE WHEN cc.allowed_amt IS NULL    or cc.claim_line_type_code<>'PB' THEN 0 ELSE cc.allowed_amt END 
	ELSE 0 
END) AS cost_tc,
SUM(CASE
	WHEN sig.assigned_type='C' THEN 
	CASE WHEN cc.allowed_amt IS NULL    or cc.claim_line_type_code<>'PB' THEN 0 ELSE cc.allowed_amt END 
	ELSE 0 
END) AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length 
FROM tmp_melfil e
LEFT OUTER JOIN assignment sig ON e.master_episode_id = sig.master_episode_id
LEFT OUTER JOIN claims_combined cc
ON sig.master_claim_id=cc.master_claim_id
WHERE cc.begin_date>(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run) 
GROUP BY e.master_episode_id;


insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'PB' AS claim_type,
2 AS 'level',
0 AS split,
1 AS annualized,
0 AS cost,
0 AS cost_t,
0 AS cost_tc,
0 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length
FROM tmp_melfil;

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'PB' AS claim_type,
3 AS 'level',
0 AS split,
1 AS annualized,
0 AS cost,
0 AS cost_t,
0 AS cost_tc,
0 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length
FROM tmp_melfil;

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'PB' AS claim_type,
4 AS 'level',
0 AS split,
1 AS annualized,
0 AS cost,
0 AS cost_t,
0 AS cost_tc,
0 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length
FROM tmp_melfil;

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'PB' AS claim_type,
5 AS 'level',
0 AS split,
1 AS annualized,
0 AS cost,
0 AS cost_t,
0 AS cost_tc,
0 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length
FROM tmp_melfil;

DROP TABLE tmp_melfil;


/*
========================================
END level 1 costs and blank row setup in MEL
========================================
*/

/*
========================================
BEGIN Level II Cost Rollup
========================================
*/

/* LEVEL 2 SURVIVING (NON-CHILD) EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM master_epid_level_claim_type mel
	WHERE master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=2
	)
	AND mel.level = 1
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB'
);

update /*+ direct */  master_epid_level_claim_type
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB') AS t2
WHERE master_epid_level_claim_type.master_episode_id=t2.master_episode_id
AND level = 2
AND split = 0
AND annualized = 1
AND filter_id = 1
AND claim_type = 'PB';

TRUNCATE TABLE tmp_mel1;

/* MUST RUN the SAME LEVEL SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL II SUB ROLLUP */

/* at a given mel level, all the children can take on their own costs from mel */
update /*+ direct */  tmp_sub_association
SET cost=mel.cost,
cost_t=(mel.cost_t + mel.cost_tc),
cost_tc=0.0000,
cost_c=mel.cost_c
FROM master_epid_level_claim_type mel 
WHERE child_master_episode_id=mel.master_episode_id
	AND mel.level = 1
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB'
	AND association_level=2;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other query… */

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=32
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 31
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 31
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=31
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 30
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 30
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=30
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 29
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 29
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=29
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 28
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 28
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=28
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 27
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 27
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=27
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 26
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 26
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=26
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 25
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 25
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=25
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 24
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 24
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=24
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 23
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 23
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=23
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 22
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 22
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=22
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 21
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 21
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=21
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 20
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 20
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=20
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 19
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 19
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=19
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 19
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 18
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=18
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 17
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 17
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=17
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 16
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 16
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=16
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 15
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 15
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=15
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 14
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 14
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=14
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 13
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 13
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=13
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 12
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 12
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=12
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 11
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 11
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=11
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 10
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 10
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=10
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 9
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 9
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=9
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 8
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 8
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=8
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 7
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 7
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=7
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 6
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 6
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=6
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 5
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 5
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=5
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 4
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 4
AND s1.association_level = 2;


TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=4
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 3
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 3
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=3
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 2
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 2
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=2
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 1
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 1
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* <END> THIS WHOLE THING IS CURRENT JUST LEVEL II SUB ROLLUP */

/* LVL2 CHILD EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
e.master_episode_id,
SUM(mel.cost) as 'cost',
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN (mel.cost_t + mel.cost_tc)
	ELSE 0.0000
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN mel.cost_c
	ELSE (mel.cost_t + mel.cost_tc + mel.cost_c) 
END) AS cost_c
FROM episode e 
JOIN association soc ON e.master_episode_id = soc.parent_master_episode_id
AND soc.association_level=2 
JOIN filtered_episodes fe ON fe.master_episode_id=soc.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 1
AND mel.split = 0
AND mel.annualized = 1
AND mel.filter_id = 1
AND mel.claim_type = 'PB'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=2)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 2
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'PB';

TRUNCATE TABLE tmp_mel1;

update /*+ direct */  master_epid_level_claim_type 
SET cost=st.cost, 
cost_t = st.cost_t, 
cost_c = st.cost_c
FROM (
	SELECT sa.uber_master_episode_id, 
	m.cost+sa.cost AS cost, 
	m.cost_t+sa.cost_t AS cost_t, 
	m.cost_c+sa.cost_c AS cost_c
	FROM (
		SELECT 
		uber_master_episode_id,
		SUM(cost) AS cost,
		SUM(cost_t) AS cost_t,
		SUM(cost_c) AS cost_c
		FROM tmp_sub_association
		WHERE association_level=2
		AND association_sub_level=1
		GROUP BY uber_master_episode_id
	) AS sa 
	JOIN master_epid_level_claim_type m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 2
	AND m.split = 0
	AND m.annualized = 1
	AND m.filter_id = 1
	AND m.claim_type = 'PB'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 2
AND split = 0
AND annualized = 1
AND filter_id = 1
AND claim_type = 'PB';


/*==============================*/
/*==============================*/
/* LEVEL III LEVEL 3 LEVEL III LEVEL 3 */
/*==============================*/
/*==============================*/
/* LEVEL 3 SURVIVING (NON-CHILD) EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM master_epid_level_claim_type mel
	WHERE master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=3
	)
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB'
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'PB';

TRUNCATE TABLE tmp_mel1;

/* MUST RUN the LEVEL III SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL 3 SUB ROLLUP */

/* at a given mel level, all the children can take on their own costs from mel */
update /*+ direct */  tmp_sub_association
SET cost=mel.cost,
cost_t=(mel.cost_t + mel.cost_tc),
cost_tc=0.0000,
cost_c=mel.cost_c
FROM master_epid_level_claim_type mel 
WHERE child_master_episode_id=mel.master_episode_id
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB'
	AND association_level=3;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other query… */

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=32
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 31
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 31
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=31
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 30
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 30
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=30
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 29
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 29
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=29
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 28
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 28
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=28
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 27
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 27
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=27
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 26
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 26
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=26
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 25
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 25
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=25
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 24
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 24
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=24
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 23
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 23
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=23
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 22
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 22
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=22
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 21
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 21
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=21
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 20
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 20
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=20
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 19
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 19
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=19
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 18
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 18
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=18
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 17
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 17
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=17
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 16
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 16
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=16
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 15
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 15
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=15
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 14
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 14
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=14
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 13
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 13
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=13
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 12
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 12
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=12
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 11
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 11
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=11
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 10
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 10
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=10
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 9
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 9
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=9
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 8
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 8
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=8
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 7
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 7
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=7
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 6
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 6
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=6
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 5
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 5
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=5
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 4
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 4
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=4
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 3
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 3
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=3
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 2
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 2
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=2
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 1
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 1
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* <END> THIS WHOLE THING IS CURRENT JUST LEVEL 3 SUB ROLLUP */


/* LVL3 CHILD EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
e.master_episode_id,
SUM(mel.cost) as 'cost',
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN (mel.cost_t + mel.cost_tc)
	ELSE 0.0000
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN mel.cost_c
	ELSE (mel.cost_t + mel.cost_tc + mel.cost_c) 
END) AS cost_c
FROM episode e 
JOIN association soc ON e.master_episode_id = soc.parent_master_episode_id
AND soc.association_level=3 
JOIN filtered_episodes fe ON fe.master_episode_id=soc.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 1
AND mel.filter_id = 1
AND mel.claim_type = 'PB'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=3)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'PB';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
update /*+ direct */  master_epid_level_claim_type 
SET cost=st.cost, 
cost_t = st.cost_t, 
cost_c = st.cost_c
FROM (
	SELECT sa.uber_master_episode_id, 
	m.cost+sa.cost AS cost, 
	m.cost_t+sa.cost_t AS cost_t, 
	m.cost_c+sa.cost_c AS cost_c
	FROM (
		SELECT 
		uber_master_episode_id,
		SUM(cost) AS cost,
		SUM(cost_t) AS cost_t,
		SUM(cost_c) AS cost_c
		FROM tmp_sub_association
		WHERE association_level=3
		AND association_sub_level=1
		GROUP BY uber_master_episode_id
	) AS sa 
	JOIN master_epid_level_claim_type m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 3
	AND m.split = 0
	AND m.annualized = 1
	AND m.filter_id = 1
	AND m.claim_type = 'PB'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 3
AND split = 0
AND annualized = 1
AND filter_id = 1
AND claim_type = 'PB';

/*==============================*/
/*==============================*/
/* LEVEL IV LEVEL 4 LEVEL IV LEVEL 4 */
/*==============================*/
/*==============================*/
/* LEVEL 4 SURVIVING (NON-CHILD) EPISODE COSTS... */

insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM master_epid_level_claim_type mel
	WHERE master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=4
	)
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB'
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'PB';

TRUNCATE TABLE tmp_mel1;

/* MUST RUN the LEVEL 4 SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL 4 SUB ROLLUP */

/* at a given mel level, all the children can take on their own costs from mel */
update /*+ direct */  tmp_sub_association
SET cost=mel.cost,
cost_t=(mel.cost_t + mel.cost_tc),
cost_tc=0.0000,
cost_c=mel.cost_c
FROM master_epid_level_claim_type mel 
WHERE child_master_episode_id=mel.master_episode_id
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB'
	AND association_level=4;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other query… */

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=32
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 31
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 31
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=31
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 30
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 30
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=30
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 29
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 29
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=29
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 28
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 28
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=28
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 27
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 27
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=27
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 26
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 26
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=26
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 25
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 25
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=25
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 24
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 24
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=24
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 23
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 23
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=23
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 22
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 22
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=22
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 21
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 21
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=21
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 20
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 20
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=20
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 19
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 19
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=19
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 18
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 18
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=18
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 17
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 17
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=17
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 16
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 16
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=16
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 15
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 15
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=15
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 14
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 14
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=14
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 13
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 13
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=13
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 12
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 12
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=12
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 11
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 11
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=11
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 10
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 10
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=10
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 9
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 9
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=9
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 8
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 8
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=8
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 7
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 7
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=7
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 6
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 6
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=6
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 5
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 5
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=5
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 4
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 4
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=4
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 3
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 3
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=3
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 2
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 2
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=2
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 1
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 1
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* <END> THIS WHOLE THING IS CURRENT JUST LEVEL 4 SUB ROLLUP */


/* LVL4 CHILD EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
e.master_episode_id,
SUM(mel.cost) as 'cost',
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN (mel.cost_t + mel.cost_tc)
	ELSE 0.0000
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN mel.cost_c
	ELSE (mel.cost_t + mel.cost_tc + mel.cost_c) 
END) AS cost_c
FROM episode e 
JOIN association soc ON e.master_episode_id = soc.parent_master_episode_id
AND soc.association_level=4 
JOIN filtered_episodes fe ON fe.master_episode_id=soc.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 3
AND mel.split = 0
AND mel.annualized = 1
AND mel.filter_id = 1
AND mel.claim_type = 'PB'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=2)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'PB';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
update /*+ direct */  master_epid_level_claim_type 
SET cost=st.cost, 
cost_t = st.cost_t, 
cost_c = st.cost_c
FROM (
	SELECT sa.uber_master_episode_id, 
	m.cost+sa.cost AS cost, 
	m.cost_t+sa.cost_t AS cost_t, 
	m.cost_c+sa.cost_c AS cost_c
	FROM (
		SELECT 
		uber_master_episode_id,
		SUM(cost) AS cost,
		SUM(cost_t) AS cost_t,
		SUM(cost_c) AS cost_c
		FROM tmp_sub_association
		WHERE association_level=4
		AND association_sub_level=1
		GROUP BY uber_master_episode_id
	) AS sa 
	JOIN master_epid_level_claim_type m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 4
	AND m.split = 0
	AND m.annualized = 1
	AND m.filter_id = 1
	AND m.claim_type = 'PB'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 4
AND split = 0
AND annualized = 1
AND filter_id = 1
AND claim_type = 'PB';

/*==============================*/
/*==============================*/
/* LEVEL V LEVEL 5 LEVEL V LEVEL 5  */
/*==============================*/
/*==============================*/
/* LEVEL 5 SURVIVING (NON-CHILD) EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM master_epid_level_claim_type mel
	WHERE master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=5
	)
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB'
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 5
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'PB';

TRUNCATE TABLE tmp_mel1;

/* MUST RUN the LEVEL 5 SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL 5 SUB ROLLUP */

/* at a given mel level, all the children can take on their own costs from mel */
update /*+ direct */  tmp_sub_association
SET cost=mel.cost,
cost_t=(mel.cost_t + mel.cost_tc),
cost_tc=0.0000,
cost_c=mel.cost_c
FROM master_epid_level_claim_type mel 
WHERE child_master_episode_id=mel.master_episode_id
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB'
	AND association_level=5;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other query… */

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=32
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 31
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 31
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=31
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 30
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 30
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=30
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 29
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 29
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=29
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 28
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 28
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=28
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 27
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 27
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=27
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 26
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 26
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=26
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 25
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 25
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=25
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 24
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 24
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=24
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 24
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 24
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=23
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 22
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 22
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=22
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 21
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 21
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=21
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 20
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 20
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=20
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 19
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 19
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=19
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 18
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 18
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=18
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 17
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 17
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=17
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 16
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 16
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=16
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 15
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 15
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=15
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 14
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 14
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=14
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 13
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 13
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=13
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 12
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 12
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=12
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 11
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 11
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=11
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 10
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 10
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=10
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 9
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 9
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=9
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 8
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 8
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=8
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 7
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 7
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=7
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 6
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 6
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=6
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 5
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 5
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=5
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 4
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 4
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=4
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 3
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 3
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=3
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 2
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 2
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=2
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 1
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 1
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* <END> THIS WHOLE THING IS CURRENT JUST LEVEL 5 SUB ROLLUP */


/* LVL5 CHILD EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
e.master_episode_id,
SUM(mel.cost) as 'cost',
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN (mel.cost_t + mel.cost_tc)
	ELSE 0.0000
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN mel.cost_c
	ELSE (mel.cost_t + mel.cost_tc + mel.cost_c) 
END) AS cost_c
FROM episode e 
JOIN association soc ON e.master_episode_id = soc.parent_master_episode_id
AND soc.association_level=5 
JOIN filtered_episodes fe ON fe.master_episode_id=soc.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 4
AND mel.split = 0
AND mel.annualized = 1
AND mel.filter_id = 1
AND mel.claim_type = 'PB'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=2)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 5
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'PB';

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
update /*+ direct */  master_epid_level_claim_type 
SET cost=st.cost, 
cost_t = st.cost_t, 
cost_c = st.cost_c
FROM (
	SELECT sa.uber_master_episode_id, 
	m.cost+sa.cost AS cost, 
	m.cost_t+sa.cost_t AS cost_t, 
	m.cost_c+sa.cost_c AS cost_c
	FROM (
		SELECT 
		uber_master_episode_id,
		SUM(cost) AS cost,
		SUM(cost_t) AS cost_t,
		SUM(cost_c) AS cost_c
		FROM tmp_sub_association
		WHERE association_level=5
		AND association_sub_level=1
		GROUP BY uber_master_episode_id
	) AS sa 
	JOIN master_epid_level_claim_type m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 5
	AND m.split = 0
	AND m.annualized = 1
	AND m.filter_id = 1
	AND m.claim_type = 'PB'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 5
AND split = 0
AND annualized = 1
AND filter_id = 1
AND claim_type = 'PB';

TRUNCATE TABLE tmp_mel1;
TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;

/*RX CLAIM ROLL UP*/
TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;

/* !!!NEED TO FIND MAX END DATE TO update /*+ direct */  RUN TABLE! 
IF NOT DONE ALREADY!!!

update /*+ direct */  run
SET data_latest_begin_date = 
(SELECT max(begin_date) FROM claim_line);
*/

/*
========================================
Begin of finding annualizable episodes and all children
========================================
*/

/* first find all parent episodes (the ones we will actually annualize costs on) */
insert /*+ direct */  INTO tmp_annids 
SELECT 
e.master_episode_id,
'P' AS parent_child_id
FROM episode e 
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN episode_builder_5_4_002.episode eb ON eb.episode_id=e.episode_id
WHERE eb.END_OF_STUDY=1
AND eb.version=(SELECT max(version) FROM episode_builder_5_4_002.episode)
AND trig_begin_date<
(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

/* 
DATE_ADD(data_latest_begin_date,INTERVAL -1 YEAR)
TIMESTAMPADD('yyyy', -1, data_latest_begin_date)
*/

/* get first generational children of parents… */
insert /*+ direct */  INTO tmp_annids_c
SELECT  distinct
soc.child_master_episode_id AS master_episode_id,
2 as lvl
FROM association soc
JOIN tmp_annids t ON soc.parent_master_episode_id = t.master_episode_id
JOIN episode e ON soc.child_master_episode_id = e.master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;

insert /*+ direct */  INTO tmp_annids_c
SELECT distinct
soc.child_master_episode_id as master_episode_id,
2 AS lvl
FROM association soc 
JOIN tmp_annids_c c ON soc.parent_master_episode_id=c.master_episode_id
AND c.lvl=1
JOIN episode e ON e.master_episode_id=soc.child_master_episode_id
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE e.trig_begin_date>=(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run);

update /*+ direct */  tmp_annids_c
SET lvl=0 WHERE lvl=1;

update /*+ direct */  tmp_annids_c
SET lvl=1 WHERE lvl=2;


insert /*+ direct */  INTO tmp_annids
SELECT DISTINCT
master_episode_id,
'C' AS parent_child_id
FROM tmp_annids_c;

TRUNCATE TABLE tmp_annids_c;
 
/*
CREATE TEMPORARY TABLE tmp_ids AS 
SELECT DISTINCT * FROM tmp_annids;

TRUNCATE TABLE tmp_annids;

insert /*+ direct */  INTO tmp_annids
SELECT 
master_episode_id,
MIN(parent_child_id)
FROM tmp_ids
GROUP BY master_episode_id;

DROP TABLE tmp_ids;

*/
/*
========================================
END of finding annualizable episodes and all children
========================================
*/


/*
========================================
BEGIN finding in-level association of annualizables
========================================
*/

/* The following code sets up the tmp_sub_association table
to get all the costs of episodes that have sub-association within the same level (chains of parent>child>grandchild etc within the same level */

/* DROP TABLE tmp_uber; */
 
TRUNCATE TABLE tmp_uber;
TRUNCATE TABLE tmp_sub_association;

/* this gets all episode who do not have a parent at their level of association */

/*
insert /*+ direct */  INTO tmp_uber
SELECT DISTINCT
a1.*
FROM association a1 */
/* the following 2 lines restricts it to annualizable episodes AND non-filtered episodes */
/* JOIN tmp_annids ta1 ON ta1.master_episode_id=a1.parent_master_episode_id
JOIN tmp_annids ta2 ON ta2.master_episode_id=a1.child_master_episode_id
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=a1.association_level
); */

insert /*+ direct */  INTO tmp_uber
SELECT DISTINCT
a1.*
FROM association a1
JOIN tmp_annids ta1 ON ta1.master_episode_id=a1.parent_master_episode_id
JOIN tmp_annids ta2 ON ta2.master_episode_id=a1.child_master_episode_id
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=2
)
AND a1.association_level=2;

insert /*+ direct */  INTO tmp_uber
SELECT DISTINCT
a1.*
FROM association a1
JOIN tmp_annids ta1 ON ta1.master_episode_id=a1.parent_master_episode_id
JOIN tmp_annids ta2 ON ta2.master_episode_id=a1.child_master_episode_id
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=3
)
AND a1.association_level=3;

insert /*+ direct */  INTO tmp_uber
SELECT DISTINCT
a1.*
FROM association a1
JOIN tmp_annids ta1 ON ta1.master_episode_id=a1.parent_master_episode_id
JOIN tmp_annids ta2 ON ta2.master_episode_id=a1.child_master_episode_id
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=4
)
AND a1.association_level=4;

insert /*+ direct */  INTO tmp_uber
SELECT DISTINCT
a1.*
FROM association a1
JOIN tmp_annids ta1 ON ta1.master_episode_id=a1.parent_master_episode_id
JOIN tmp_annids ta2 ON ta2.master_episode_id=a1.child_master_episode_id
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=5
)
AND a1.association_level=5;

/* this should get the grandchildren*/
insert /*+ direct */  INTO tmp_sub_association 
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
1 AS association_sub_level
FROM tmp_uber u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
2 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;


insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
3 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=2
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
4 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=3
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
5 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=4
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
6 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=5
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
7 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=6
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
8 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=7
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
9 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=8
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
10 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=9
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
11 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=10
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
12 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=11
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
13 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=12
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
14 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=13
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
15 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=14
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
16 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=15
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
17 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=16
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
18 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=17
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
19 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=18
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
20 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=19
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
21 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=20
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
22 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=21
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
23 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=22
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
24 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=23
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
25 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=24
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
26 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=25
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
27 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=26
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
28 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=27
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
29 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=28
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
30 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=29
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
u.parent_master_episode_id AS uber_master_episode_id,
a.parent_master_episode_id,
a.child_master_episode_id,
a.association_type,
a.association_level,
a.association_count,
31 AS association_sub_level
FROM tmp_sub_association u
JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
AND u.association_sub_level=30
JOIN tmp_annids ta ON ta.master_episode_id=a.child_master_episode_id;

/*SET @maxLvl = (SELECT max(association_sub_level) FROM tmp_sub_association);*/

update /*+ direct */  tmp_sub_association
SET association_sub_level_inv=
ABS(association_sub_level-(1+ (SELECT max(association_sub_level) FROM tmp_sub_association) ));


/*
========================================
END finding in-level association of annualizables
========================================
*/

/*
========================================
BEGIN level 1 costs and blank row setup in MEL
========================================
*/
/*
SETUP level 1 costs and blank rows for successive levels
*/

CREATE TABLE IF NOT EXISTS tmp_melfil AS (
SELECT 
mel.*
FROM master_epid_level_claim_type mel
JOIN tmp_annids ta ON ta.master_episode_id=mel.master_episode_id
AND mel.filter_id=1
AND mel.claim_type='CL'
AND mel.level=1
AND mel.split=0
AND mel.annualized=0
);

/*
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'RX' AS claim_type,
1 AS 'level',
0 AS split,
1 AS annualized,
cost,
cost_t,
cost_tc,
cost_c,
risk_factor_count,
sub_type_count,
probability_of_complications,
IP_stay_count,
IP_stay_facility_costs,
IP_stay_prof_costs,
IP_stay_total_costs,
IP_stay_bed_days,
IP_stay_avg_length
FROM tmp_melfil;

*/

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
e.master_episode_id,
'RX' AS claim_type,
1 AS 'level',
0 AS split,
1 AS annualized,
SUM(CASE
	WHEN allowed_amt IS NULL    or cc.claim_line_type_code<>'RX' THEN 0.0000 ELSE allowed_amt
END) AS cost,
SUM(CASE
	WHEN sig.assigned_type='T' THEN 
	CASE WHEN cc.allowed_amt IS NULL    or cc.claim_line_type_code<>'RX' THEN 0 ELSE cc.allowed_amt END 
	ELSE 0 
END) AS cost_t,
SUM(CASE
	WHEN sig.assigned_type='TC' THEN 
	CASE WHEN cc.allowed_amt IS NULL    or cc.claim_line_type_code<>'RX' THEN 0 ELSE cc.allowed_amt END 
	ELSE 0 
END) AS cost_tc,
SUM(CASE
	WHEN sig.assigned_type='C' THEN 
	CASE WHEN cc.allowed_amt IS NULL    or cc.claim_line_type_code<>'RX' THEN 0 ELSE cc.allowed_amt END 
	ELSE 0 
END) AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length 
FROM tmp_melfil e
LEFT OUTER JOIN assignment sig ON e.master_episode_id = sig.master_episode_id
LEFT OUTER JOIN claims_combined cc
ON sig.master_claim_id=cc.master_claim_id 
WHERE cc.begin_date>(SELECT TIMESTAMPADD('yyyy', -1, data_latest_begin_date) FROM run) 
GROUP BY e.master_episode_id;


insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'RX' AS claim_type,
2 AS 'level',
0 AS split,
1 AS annualized,
0 AS cost,
0 AS cost_t,
0 AS cost_tc,
0 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length
FROM tmp_melfil;

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'RX' AS claim_type,
3 AS 'level',
0 AS split,
1 AS annualized,
0 AS cost,
0 AS cost_t,
0 AS cost_tc,
0 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length
FROM tmp_melfil;

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'RX' AS claim_type,
4 AS 'level',
0 AS split,
1 AS annualized,
0 AS cost,
0 AS cost_t,
0 AS cost_tc,
0 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length
FROM tmp_melfil;

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'RX' AS claim_type,
5 AS 'level',
0 AS split,
1 AS annualized,
0 AS cost,
0 AS cost_t,
0 AS cost_tc,
0 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length
FROM tmp_melfil;

DROP TABLE tmp_melfil;


/*
========================================
END level 1 costs and blank row setup in MEL
========================================
*/

/*
========================================
BEGIN Level II Cost Rollup
========================================
*/

/* LEVEL 2 SURVIVING (NON-CHILD) EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM master_epid_level_claim_type mel
	WHERE master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=2
	)
	AND mel.level = 1
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX'
);

update /*+ direct */  master_epid_level_claim_type
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX') AS t2
WHERE master_epid_level_claim_type.master_episode_id=t2.master_episode_id
AND level = 2
AND split = 0
AND annualized = 1
AND filter_id = 1
AND claim_type = 'RX';

TRUNCATE TABLE tmp_mel1;

/* MUST RUN the SAME LEVEL SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL II SUB ROLLUP */

/* at a given mel level, all the children can take on their own costs from mel */
update /*+ direct */  tmp_sub_association
SET cost=mel.cost,
cost_t=(mel.cost_t + mel.cost_tc),
cost_tc=0.0000,
cost_c=mel.cost_c
FROM master_epid_level_claim_type mel 
WHERE child_master_episode_id=mel.master_episode_id
	AND mel.level = 1
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX'
	AND association_level=2;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other query… */

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=32
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 31
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 31
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=31
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 30
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 30
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=30
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 29
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 29
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=29
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 28
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 28
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=28
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 27
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 27
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=27
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 26
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 26
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=26
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 25
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 25
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=25
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 24
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 24
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=24
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 23
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 23
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=23
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 22
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 22
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=22
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 21
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 21
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=21
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 20
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 20
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=20
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 19
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 19
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=19
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 19
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 18
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=18
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 17
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 17
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=17
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 16
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 16
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=16
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 15
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 15
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=15
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 14
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 14
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=14
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 13
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 13
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=13
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 12
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 12
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=12
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 11
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 11
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=11
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 10
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 10
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=10
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 9
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 9
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=9
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 8
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 8
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=8
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 7
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 7
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=7
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 6
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 6
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=6
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 5
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 5
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=5
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 4
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 4
AND s1.association_level = 2;


TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=4
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 3
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 3
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=3
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 2
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 2
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=2
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 1
AND sa.association_level = 2) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 1
AND s1.association_level = 2;

TRUNCATE TABLE tmp_mel1;

/* <END> THIS WHOLE THING IS CURRENT JUST LEVEL II SUB ROLLUP */

/* LVL2 CHILD EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
e.master_episode_id,
SUM(mel.cost) as 'cost',
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN (mel.cost_t + mel.cost_tc)
	ELSE 0.0000
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN mel.cost_c
	ELSE (mel.cost_t + mel.cost_tc + mel.cost_c) 
END) AS cost_c
FROM episode e 
JOIN association soc ON e.master_episode_id = soc.parent_master_episode_id
AND soc.association_level=2 
JOIN filtered_episodes fe ON fe.master_episode_id=soc.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 1
AND mel.split = 0
AND mel.annualized = 1
AND mel.filter_id = 1
AND mel.claim_type = 'RX'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=2)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 2
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'RX';

TRUNCATE TABLE tmp_mel1;

update /*+ direct */  master_epid_level_claim_type 
SET cost=st.cost, 
cost_t = st.cost_t, 
cost_c = st.cost_c
FROM (
	SELECT sa.uber_master_episode_id, 
	m.cost+sa.cost AS cost, 
	m.cost_t+sa.cost_t AS cost_t, 
	m.cost_c+sa.cost_c AS cost_c
	FROM (
		SELECT 
		uber_master_episode_id,
		SUM(cost) AS cost,
		SUM(cost_t) AS cost_t,
		SUM(cost_c) AS cost_c
		FROM tmp_sub_association
		WHERE association_level=2
		AND association_sub_level=1
		GROUP BY uber_master_episode_id
	) AS sa 
	JOIN master_epid_level_claim_type m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 2
	AND m.split = 0
	AND m.annualized = 1
	AND m.filter_id = 1
	AND m.claim_type = 'RX'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 2
AND split = 0
AND annualized = 1
AND filter_id = 1
AND claim_type = 'RX';


/*==============================*/
/*==============================*/
/* LEVEL III LEVEL 3 LEVEL III LEVEL 3 */
/*==============================*/
/*==============================*/
/* LEVEL 3 SURVIVING (NON-CHILD) EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM master_epid_level_claim_type mel
	WHERE master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=3
	)
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX'
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'RX';

TRUNCATE TABLE tmp_mel1;

/* MUST RUN the LEVEL III SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL 3 SUB ROLLUP */

/* at a given mel level, all the children can take on their own costs from mel */
update /*+ direct */  tmp_sub_association
SET cost=mel.cost,
cost_t=(mel.cost_t + mel.cost_tc),
cost_tc=0.0000,
cost_c=mel.cost_c
FROM master_epid_level_claim_type mel 
WHERE child_master_episode_id=mel.master_episode_id
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX'
	AND association_level=3;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other query… */

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=32
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 31
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 31
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=31
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 30
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 30
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=30
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 29
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 29
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=29
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 28
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 28
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=28
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 27
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 27
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=27
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 26
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 26
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=26
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 25
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 25
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=25
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 24
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 24
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=24
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 23
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 23
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=23
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 22
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 22
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=22
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 21
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 21
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=21
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 20
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 20
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=20
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 19
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 19
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=19
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 18
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 18
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=18
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 17
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 17
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=17
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 16
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 16
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=16
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 15
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 15
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=15
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 14
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 14
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=14
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 13
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 13
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=13
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 12
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 12
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=12
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 11
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 11
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=11
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 10
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 10
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=10
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 9
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 9
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=9
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 8
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 8
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=8
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 7
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 7
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=7
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 6
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 6
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=6
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 5
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 5
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=5
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 4
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 4
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=4
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 3
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 3
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=3
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 2
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 2
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=3 
AND sa.association_sub_level=2
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 1
AND sa.association_level = 3) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 1
AND s1.association_level = 3;

TRUNCATE TABLE tmp_mel1;

/* <END> THIS WHOLE THING IS CURRENT JUST LEVEL 3 SUB ROLLUP */


/* LVL3 CHILD EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
e.master_episode_id,
SUM(mel.cost) as 'cost',
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN (mel.cost_t + mel.cost_tc)
	ELSE 0.0000
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN mel.cost_c
	ELSE (mel.cost_t + mel.cost_tc + mel.cost_c) 
END) AS cost_c
FROM episode e 
JOIN association soc ON e.master_episode_id = soc.parent_master_episode_id
AND soc.association_level=3 
JOIN filtered_episodes fe ON fe.master_episode_id=soc.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 1
AND mel.filter_id = 1
AND mel.claim_type = 'RX'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=3)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'RX';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
update /*+ direct */  master_epid_level_claim_type 
SET cost=st.cost, 
cost_t = st.cost_t, 
cost_c = st.cost_c
FROM (
	SELECT sa.uber_master_episode_id, 
	m.cost+sa.cost AS cost, 
	m.cost_t+sa.cost_t AS cost_t, 
	m.cost_c+sa.cost_c AS cost_c
	FROM (
		SELECT 
		uber_master_episode_id,
		SUM(cost) AS cost,
		SUM(cost_t) AS cost_t,
		SUM(cost_c) AS cost_c
		FROM tmp_sub_association
		WHERE association_level=3
		AND association_sub_level=1
		GROUP BY uber_master_episode_id
	) AS sa 
	JOIN master_epid_level_claim_type m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 3
	AND m.split = 0
	AND m.annualized = 1
	AND m.filter_id = 1
	AND m.claim_type = 'RX'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 3
AND split = 0
AND annualized = 1
AND filter_id = 1
AND claim_type = 'RX';

/*==============================*/
/*==============================*/
/* LEVEL IV LEVEL 4 LEVEL IV LEVEL 4 */
/*==============================*/
/*==============================*/
/* LEVEL 4 SURVIVING (NON-CHILD) EPISODE COSTS... */

insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM master_epid_level_claim_type mel
	WHERE master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=4
	)
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX'
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'RX';

TRUNCATE TABLE tmp_mel1;

/* MUST RUN the LEVEL 4 SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL 4 SUB ROLLUP */

/* at a given mel level, all the children can take on their own costs from mel */
update /*+ direct */  tmp_sub_association
SET cost=mel.cost,
cost_t=(mel.cost_t + mel.cost_tc),
cost_tc=0.0000,
cost_c=mel.cost_c
FROM master_epid_level_claim_type mel 
WHERE child_master_episode_id=mel.master_episode_id
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX'
	AND association_level=4;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other query… */

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=32
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 31
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 31
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=31
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 30
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 30
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=30
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 29
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 29
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=29
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 28
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 28
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=28
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 27
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 27
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=27
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 26
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 26
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=26
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 25
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 25
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=25
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 24
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 24
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=24
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 23
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 23
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=23
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 22
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 22
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=22
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 21
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 21
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=21
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 20
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 20
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=20
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 19
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 19
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=19
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 18
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 18
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=18
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 17
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 17
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=17
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 16
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 16
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=16
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 15
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 15
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=15
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 14
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 14
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=14
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 13
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 13
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=13
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 12
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 12
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=12
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 11
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 11
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=11
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 10
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 10
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=10
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 9
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 9
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=9
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 8
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 8
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=8
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 7
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 7
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=7
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 6
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 6
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=6
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 5
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 5
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=5
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 4
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 4
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=4
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 3
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 3
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=3
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 2
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 2
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=4 
AND sa.association_sub_level=2
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 1
AND sa.association_level = 4) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 1
AND s1.association_level = 4;

TRUNCATE TABLE tmp_mel1;

/* <END> THIS WHOLE THING IS CURRENT JUST LEVEL 4 SUB ROLLUP */


/* LVL4 CHILD EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
e.master_episode_id,
SUM(mel.cost) as 'cost',
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN (mel.cost_t + mel.cost_tc)
	ELSE 0.0000
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN mel.cost_c
	ELSE (mel.cost_t + mel.cost_tc + mel.cost_c) 
END) AS cost_c
FROM episode e 
JOIN association soc ON e.master_episode_id = soc.parent_master_episode_id
AND soc.association_level=4 
JOIN filtered_episodes fe ON fe.master_episode_id=soc.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 3
AND mel.split = 0
AND mel.annualized = 1
AND mel.filter_id = 1
AND mel.claim_type = 'RX'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=2)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'RX';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
update /*+ direct */  master_epid_level_claim_type 
SET cost=st.cost, 
cost_t = st.cost_t, 
cost_c = st.cost_c
FROM (
	SELECT sa.uber_master_episode_id, 
	m.cost+sa.cost AS cost, 
	m.cost_t+sa.cost_t AS cost_t, 
	m.cost_c+sa.cost_c AS cost_c
	FROM (
		SELECT 
		uber_master_episode_id,
		SUM(cost) AS cost,
		SUM(cost_t) AS cost_t,
		SUM(cost_c) AS cost_c
		FROM tmp_sub_association
		WHERE association_level=4
		AND association_sub_level=1
		GROUP BY uber_master_episode_id
	) AS sa 
	JOIN master_epid_level_claim_type m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 4
	AND m.split = 0
	AND m.annualized = 1
	AND m.filter_id = 1
	AND m.claim_type = 'RX'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 4
AND split = 0
AND annualized = 1
AND filter_id = 1
AND claim_type = 'RX';

/*==============================*/
/*==============================*/
/* LEVEL V LEVEL 5 LEVEL V LEVEL 5  */
/*==============================*/
/*==============================*/
/* LEVEL 5 SURVIVING (NON-CHILD) EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM master_epid_level_claim_type mel
	WHERE master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=5
	)
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX'
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 5
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'RX';

TRUNCATE TABLE tmp_mel1;

/* MUST RUN the LEVEL 5 SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL 5 SUB ROLLUP */

/* at a given mel level, all the children can take on their own costs from mel */
update /*+ direct */  tmp_sub_association
SET cost=mel.cost,
cost_t=(mel.cost_t + mel.cost_tc),
cost_tc=0.0000,
cost_c=mel.cost_c
FROM master_epid_level_claim_type mel 
WHERE child_master_episode_id=mel.master_episode_id
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX'
	AND association_level=5;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other query… */

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=32
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 31
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 31
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=31
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 30
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 30
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=30
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 29
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 29
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=29
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 28
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 28
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=28
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 27
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 27
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=27
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 26
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 26
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=26
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 25
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 25
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=25
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 24
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 24
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=24
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 24
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 24
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=23
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 22
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 22
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=22
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 21
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 21
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=21
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 20
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 20
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=20
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 19
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 19
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=19
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 18
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 18
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=18
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 17
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 17
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=17
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 16
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 16
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=16
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 15
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 15
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=15
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 14
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 14
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=14
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 13
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 13
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=13
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 12
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 12
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=12
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 11
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 11
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=11
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 10
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 10
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=10
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 9
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 9
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=9
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 8
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 8
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=8
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 7
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 7
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=7
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 6
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 6
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=6
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 5
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 5
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=5
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 4
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 4
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=4
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 3
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 3
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=3
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 2
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 2
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN (sa.cost_t + sa.cost_tc)
	ELSE 0.0000
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
	THEN sa.cost_c
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=5 
AND sa.association_sub_level=2
);

update /*+ direct */  tmp_sub_association s1
SET cost=tt.cost, cost_t = tt.cost_t, cost_c = tt.cost_c
FROM 
(SELECT t.master_episode_id, t.cost+sa.cost AS cost, t.cost_t+sa.cost_t AS cost_t, t.cost_c+sa.cost_c AS cost_c
FROM (SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
JOIN tmp_sub_association sa ON sa.child_master_episode_id = t.master_episode_id
AND sa.association_sub_level = 1
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 1
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* <END> THIS WHOLE THING IS CURRENT JUST LEVEL 5 SUB ROLLUP */


/* LVL5 CHILD EPISODE COSTS... */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
e.master_episode_id,
SUM(mel.cost) as 'cost',
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN (mel.cost_t + mel.cost_tc)
	ELSE 0.0000
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
	THEN mel.cost_c
	ELSE (mel.cost_t + mel.cost_tc + mel.cost_c) 
END) AS cost_c
FROM episode e 
JOIN association soc ON e.master_episode_id = soc.parent_master_episode_id
AND soc.association_level=5 
JOIN filtered_episodes fe ON fe.master_episode_id=soc.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 4
AND mel.split = 0
AND mel.annualized = 1
AND mel.filter_id = 1
AND mel.claim_type = 'RX'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=2)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

update /*+ direct */  master_epid_level_claim_type AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level_claim_type mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 5
	AND mel.split = 0
	AND mel.annualized = 1
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 1
AND mel1.filter_id = 1
AND mel1.claim_type = 'RX';

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
update /*+ direct */  master_epid_level_claim_type 
SET cost=st.cost, 
cost_t = st.cost_t, 
cost_c = st.cost_c
FROM (
	SELECT sa.uber_master_episode_id, 
	m.cost+sa.cost AS cost, 
	m.cost_t+sa.cost_t AS cost_t, 
	m.cost_c+sa.cost_c AS cost_c
	FROM (
		SELECT 
		uber_master_episode_id,
		SUM(cost) AS cost,
		SUM(cost_t) AS cost_t,
		SUM(cost_c) AS cost_c
		FROM tmp_sub_association
		WHERE association_level=5
		AND association_sub_level=1
		GROUP BY uber_master_episode_id
	) AS sa 
	JOIN master_epid_level_claim_type m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 5
	AND m.split = 0
	AND m.annualized = 1
	AND m.filter_id = 1
	AND m.claim_type = 'RX'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 5
AND split = 0
AND annualized = 1
AND filter_id = 1
AND claim_type = 'RX';

TRUNCATE TABLE tmp_mel1;
TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;




drop table if exists QC;
create table QC as 
SELECT
master_episode_id,
filter_id,
level,
split,
annualized,
sum(case when claim_type='CL' then cost else 0 end) as CL_cost,
sum(case when claim_type in ('IP','OP','PB','RX') then cost else 0 end) as NON_CL_cost,
sum(case when claim_type='CL' then cost else 0 end) - sum(case when claim_type in ('IP','OP','PB','RX') then cost else 0 end) as diff
from master_epid_level_claim_type
where filter_id=1 and split=0 and annualized=1 
group by master_episode_id,
filter_id,
level,
split,
annualized;


