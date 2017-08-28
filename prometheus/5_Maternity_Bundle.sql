set search_path=epbuilder;

drop table if exists mom_baby;
create table mom_baby as
select
 MOTHER_MBR_ID as ENCRYPT_RECIP_ID_MOM,
 BABY_MBR_ID as ENCRYPT_RECIP_ID_BABY,
 BIRTH_YR as year
from MBR_MOTHER_BABY;

/*MATERNITY BUNDLE*/

/* TRUNCATE TABLE master_epid_level; */

TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;

/* The following code sets up the tmp_sub_association table
to get all the costs of episodes that have sub-associations within the same level (chains of parent>child>grandchild etc within the same level */

/* DROP TABLE tmp_uber; */

TRUNCATE TABLE tmp_uber;
TRUNCATE TABLE tmp_sub_association;

/* this gets all episode who do not have a parent at their level of association */
/*
insert /*+ direct */  INTO tmp_uber
SELECT
a1.*
FROM association a1
JOIN filtered_episodes fe ON fe.master_episode_id=a1.parent_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN filtered_episodes fe2 ON fe2.master_episode_id=a1.child_master_episode_id
AND fe2.filter_id=1
AND fe2.filter_fail=0 
AND parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=a1.association_level
);
*/

insert /*+ direct */  INTO tmp_uber
SELECT
a1.*
FROM association a1
JOIN filtered_episodes fe ON fe.master_episode_id=a1.parent_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN filtered_episodes fe2 ON fe2.master_episode_id=a1.child_master_episode_id
AND fe2.filter_id=1
AND fe2.filter_fail=0 
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=2
)
AND a1.association_level=2;

insert /*+ direct */  INTO tmp_uber
SELECT
a1.*
FROM association a1
JOIN filtered_episodes fe ON fe.master_episode_id=a1.parent_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN filtered_episodes fe2 ON fe2.master_episode_id=a1.child_master_episode_id
AND fe2.filter_id=1
AND fe2.filter_fail=0 
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=3
)
AND a1.association_level=3;

insert /*+ direct */  INTO tmp_uber
SELECT
a1.*
FROM association a1
JOIN filtered_episodes fe ON fe.master_episode_id=a1.parent_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN filtered_episodes fe2 ON fe2.master_episode_id=a1.child_master_episode_id
AND fe2.filter_id=1
AND fe2.filter_fail=0 
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=4
)
AND a1.association_level=4;

insert /*+ direct */  INTO tmp_uber
SELECT
a1.*
FROM association a1
JOIN filtered_episodes fe ON fe.master_episode_id=a1.parent_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
JOIN filtered_episodes fe2 ON fe2.master_episode_id=a1.child_master_episode_id
AND fe2.filter_id=1
AND fe2.filter_fail=0 
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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

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
JOIN filtered_episodes fe ON fe.master_episode_id=a.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;



drop table if exists sub_distinct;
create table sub_distinct as
select child_master_episode_id,
min(association_level) as association_level
from association
group by child_master_episode_id;

drop table if exists preg_cost_info;
create table preg_cost_info as
select distinct
mom_baby.ENCRYPT_RECIP_ID_MOM,
mom_baby.ENCRYPT_RECIP_ID_BABY,
episode.master_episode_id as PREG_master_episode_id,
episode.trig_begin_date,
mom_baby.year,
split.cost as SPLIT_TOTAL_PREGN_COST,
split.cost_t as SPLIT_TYPICAL_PREGN_COST,
split.cost_c as SPLIT_PAC_PREGN_COST,
unsplit.cost as UNSPLIT_TOTAL_PREGN_COST,
unsplit.cost_t as UNSPLIT_TYPICAL_PREGN_COST,
unsplit.cost_c as UNSPLIT_PAC_PREGN_COST
from mom_baby

left join episode
on mom_baby.ENCRYPT_RECIP_ID_MOM=episode.member_id
join (select mom_baby.ENCRYPT_RECIP_ID_MOM, year, count(*) as count from mom_baby group by mom_baby.ENCRYPT_RECIP_ID_MOM, mom_baby.year) as mb2
on mom_baby.ENCRYPT_RECIP_ID_MOM=mb2.ENCRYPT_RECIP_ID_MOM and  mb2.count=1 and extract(year from trig_begin_date)=mb2.year

join (select mom_baby.ENCRYPT_RECIP_ID_BABY,year, count(*) as count from mom_baby group by mom_baby.ENCRYPT_RECIP_ID_BABY, mom_baby.year) as mb3
on mom_baby.ENCRYPT_RECIP_ID_BABY=mb3.ENCRYPT_RECIP_ID_BABY and mb3.count=1 and extract(year from trig_begin_date)=mb3.year

join (select episode.member_id, count(*) as count from episode where episode_id='EX1502' group by member_id) as mb4
on mom_baby.ENCRYPT_RECIP_ID_BABY=mb4.member_id and mb4.count=1 

join master_epid_level split
on episode.master_episode_id=split.master_episode_id
join master_epid_level unsplit
on episode.master_episode_id=unsplit.master_episode_id
left join sub_distinct
on sub_distinct.child_master_episode_id=episode.master_episode_id 

where left(episode.master_episode_id,6)='EX1401' and left(split.master_episode_id,6)='EX1401' and split.claim_type='CL' and split.split=1 and split.level=4 and split.annualized=0 and split.filter_id=1
and left(unsplit.master_episode_id,6)='EX1401' and unsplit.claim_type='CL' and unsplit.split=0 and unsplit.level=4 and unsplit.annualized=0 and unsplit.filter_id=1 and extract(year from trig_begin_date)=mom_baby.year and sub_distinct.association_level is null

or left(episode.master_episode_id,6)='EX1401' and left(split.master_episode_id,6)='EX1401' and split.claim_type='CL' and split.split=1 and split.level=4 and split.annualized=0 and split.filter_id=1
and left(unsplit.master_episode_id,6)='EX1401' and unsplit.claim_type='CL' and unsplit.split=0 and unsplit.level=4 and unsplit.annualized=0 and unsplit.filter_id=1 and extract(year from trig_begin_date)=mom_baby.year and split.level<sub_distinct.association_level 
and unsplit.level<sub_distinct.association_level ;

drop table if exists drop_list;

create table drop_list as select ENCRYPT_RECIP_ID_MOM,year, count(*) as count from preg_cost_info group by ENCRYPT_RECIP_ID_MOM, year;

DELETE FROM preg_cost_info
WHERE  ENCRYPT_RECIP_ID_MOM  IN (SELECT ENCRYPT_RECIP_ID_MOM FROM drop_list WHERE count >1 and preg_cost_info.ENCRYPT_RECIP_ID_MOM=drop_list.ENCRYPT_RECIP_ID_MOM and preg_cost_info.year=drop_list.year);

drop table drop_list;




/*get actual Delivery Costs*/
drop table if exists del_cost_info;
create table del_cost_info as
select distinct
mom_baby.ENCRYPT_RECIP_ID_MOM,
mom_baby.ENCRYPT_RECIP_ID_BABY,
episode.master_episode_id as DEL_master_episode_id,
case when left(episode.master_episode_id,6) = 'EP1403' then 'VAGDEL' else 'CSECT' end as DELIVERY_TYPE,
episode.trig_begin_date,
mom_baby.year,
split.cost as SPLIT_TOTAL_DEL_COST,
split.cost_t as SPLIT_TYPICAL_DEL_COST,
split.cost_c as SPLIT_PAC_DEL_COST,
unsplit.cost as UNSPLIT_TOTAL_DEL_COST,
unsplit.cost_t as UNSPLIT_TYPICAL_DEL_COST,
unsplit.cost_c as UNSPLIT_PAC_DEL_COST
from mom_baby
left join episode
on mom_baby.ENCRYPT_RECIP_ID_MOM=episode.member_id
join (select mom_baby.ENCRYPT_RECIP_ID_MOM, year, count(*) as count from mom_baby group by mom_baby.ENCRYPT_RECIP_ID_MOM, mom_baby.year) as mb2
on mom_baby.ENCRYPT_RECIP_ID_MOM=mb2.ENCRYPT_RECIP_ID_MOM and  mb2.count=1 and extract(year from trig_begin_date)=mb2.year

join (select mom_baby.ENCRYPT_RECIP_ID_BABY,year, count(*) as count from mom_baby group by mom_baby.ENCRYPT_RECIP_ID_BABY, mom_baby.year) as mb3
on mom_baby.ENCRYPT_RECIP_ID_BABY=mb3.ENCRYPT_RECIP_ID_BABY and mb3.count=1 and extract(year from trig_begin_date)=mb3.year

join (select episode.member_id, count(*) as count from episode where episode_id='EX1502' group by member_id) as mb4
on mom_baby.ENCRYPT_RECIP_ID_BABY=mb4.member_id and mb4.count=1 


join master_epid_level split
on episode.master_episode_id=split.master_episode_id
join master_epid_level unsplit
on episode.master_episode_id=unsplit.master_episode_id
left join sub_distinct
on sub_distinct.child_master_episode_id=episode.master_episode_id 
where left(episode.master_episode_id,6) in ('EP1403','EP1404') and left(split.master_episode_id,6)in ('EP1403','EP1404') and split.claim_type='CL' and split.split=1 and split.level=3 and split.annualized=0
and left(unsplit.master_episode_id,6)in ('EP1403','EP1404') and unsplit.claim_type='CL' and unsplit.split=0 and unsplit.level=3 and unsplit.annualized=0 and split.filter_id=0 and unsplit.filter_id=0 and extract(year from trig_begin_date)=mom_baby.year and sub_distinct.association_level is null

or left(episode.master_episode_id,6) in ('EP1403','EP1404') and left(split.master_episode_id,6)in ('EP1403','EP1404') and split.claim_type='CL' and split.split=1 and split.level=3 and split.annualized=0
and left(unsplit.master_episode_id,6)in ('EP1403','EP1404') and unsplit.claim_type='CL' and unsplit.split=0 and unsplit.level=3 and unsplit.annualized=0 and split.filter_id=0 and unsplit.filter_id=0 and extract(year from trig_begin_date)=mom_baby.year and split.level<sub_distinct.association_level 
and unsplit.level<sub_distinct.association_level 
;

create table drop_list as select ENCRYPT_RECIP_ID_MOM,year, count(*) as count from del_cost_info group by ENCRYPT_RECIP_ID_MOM, year;

DELETE FROM del_cost_info
WHERE  ENCRYPT_RECIP_ID_MOM  IN (SELECT ENCRYPT_RECIP_ID_MOM FROM drop_list WHERE count >1 and del_cost_info.ENCRYPT_RECIP_ID_MOM=drop_list.ENCRYPT_RECIP_ID_MOM and del_cost_info.year=drop_list.year);

drop table drop_list;


/* add on expected delivery costs at level 3*/
/* cannot find delivery episodes in ra_exp_cost_table*/
drop table if exists del_cost_exp_info;
create table del_cost_exp_info as
select
del_cost_info.*,
rec.total_exp_cost_sa_l3 as split_expected_total_cost,
rec.exp_cost_sa_typ_ip_l3+rec.exp_cost_sa_typ_other_l3 as split_expected_typical_cost,
rec.exp_cost_sa_comp_other_l3 as split_expected_pac_cost,
rec.total_exp_cost_ra_l3 as unsplit_expected_total_cost,
rec.exp_cost_ra_typ_ip_l3+rec.exp_cost_ra_typ_other_l3 as unsplit_expected_typical_cost,
rec.exp_cost_ra_comp_other_l3 as unsplit_expected_pac_cost
from del_cost_info
left join ra_exp_cost rec
on del_cost_info.DEL_master_episode_id=rec.epi_id;


/*grab newborn costs and nursery level*/

drop table if exists BABY_LEVEL_NURSERY;

create table BABY_LEVEL_NURSERY as
select
claims_combined.member_id,
code.code_value
from code
join claims_combined
on code.u_c_id=claims_combined.id
where nomen='REV' and code_value in ('0174','0170','0171','0179','0172','0173')
;

drop table if exists BABY_NURSERY;

create table BABY_NURSERY as
select member_id,
max(case when code_value in ('0170','0171','0179')  then 1
when code_value = '0173' then 3
when code_value = '0172' then 2
when code_value = '0174' then 4 else 0 end) as NURSERY_LEVEL
from BABY_LEVEL_NURSERY
group by member_id;

drop table if exists baby_cost_info;
create table baby_cost_info as
select distinct
mom_baby.ENCRYPT_RECIP_ID_MOM,
mom_baby.ENCRYPT_RECIP_ID_BABY,
episode.master_episode_id as NEWBORN_master_episode_id,
episode.trig_begin_date,
mom_baby.year,
split.cost as SPLIT_TOTAL_NEWBORN_COST,
split.cost_t as SPLIT_TYPICAL_NEWBORN_COST,
split.cost_c as SPLIT_PAC_NEWBORN_COST,
unsplit.cost as UNSPLIT_TOTAL_NEWBORN_COST,
unsplit.cost_t as UNSPLIT_TYPICAL_NEWBORN_COST,
unsplit.cost_c as UNSPLIT_PAC_NEWBORN_COST,
NURSERY_LEVEL
from mom_baby

left join episode
on mom_baby.ENCRYPT_RECIP_ID_BABY=episode.member_id
join (select mom_baby.ENCRYPT_RECIP_ID_MOM, year, count(*) as count from mom_baby group by mom_baby.ENCRYPT_RECIP_ID_MOM, mom_baby.year) as mb2
on mom_baby.ENCRYPT_RECIP_ID_MOM=mb2.ENCRYPT_RECIP_ID_MOM and  mb2.count=1 and extract(year from trig_begin_date)=mb2.year

join (select mom_baby.ENCRYPT_RECIP_ID_BABY,year, count(*) as count from mom_baby group by mom_baby.ENCRYPT_RECIP_ID_BABY, mom_baby.year) as mb3
on mom_baby.ENCRYPT_RECIP_ID_BABY=mb3.ENCRYPT_RECIP_ID_BABY and mb3.count=1 and extract(year from trig_begin_date)=mb3.year

join (select episode.member_id, count(*) as count from episode where episode_id='EX1502' group by member_id) as mb4
on mom_baby.ENCRYPT_RECIP_ID_BABY=mb4.member_id and mb4.count=1 

left join BABY_NURSERY
on mom_baby.ENCRYPT_RECIP_ID_BABY=BABY_NURSERY.member_id
join master_epid_level split
on episode.master_episode_id=split.master_episode_id
join master_epid_level unsplit
on episode.master_episode_id=unsplit.master_episode_id
where left(episode.master_episode_id,6) ='EX1502' and left(split.master_episode_id,6)='EX1502' and split.claim_type='CL' and split.split=1 and split.level=3 and split.annualized=0
and left(unsplit.master_episode_id,6)='EX1502' and unsplit.claim_type='CL' and unsplit.split=0 and unsplit.level=3 and unsplit.annualized=0 and split.filter_id=0 and unsplit.filter_id=0 and extract(year from trig_begin_date)=mom_baby.year;



/*bring preg, del and baby costs together*/



drop table if exists MATERNITY_BUNDLE_COSTFIELDS;
create table MATERNITY_BUNDLE_COSTFIELDS as
select preg_cost_info.*,
del_cost_exp_info.DEL_master_episode_id,
del_cost_exp_info.DELIVERY_TYPE,
del_cost_exp_info.SPLIT_TOTAL_DEL_COST,
del_cost_exp_info.SPLIT_TYPICAL_DEL_COST,
del_cost_exp_info.SPLIT_PAC_DEL_COST,
del_cost_exp_info.UNSPLIT_TOTAL_DEL_COST,
del_cost_exp_info.UNSPLIT_TYPICAL_DEL_COST,
del_cost_exp_info.UNSPLIT_PAC_DEL_COST,
del_cost_exp_info.split_expected_total_cost as split_expected_del_total_cost,
del_cost_exp_info.split_expected_typical_cost as split_expected_del_typical_cost,
del_cost_exp_info.split_expected_pac_cost as split_expected_del_pac_cost,
del_cost_exp_info.unsplit_expected_total_cost as unsplit_expected_del_total_cost,
del_cost_exp_info.unsplit_expected_typical_cost as unsplit_expected_del_typical_cost,
del_cost_exp_info.unsplit_expected_pac_cost as  unsplit_expected_del_pac_cost,
baby_cost_info.NEWBORN_master_episode_id,
baby_cost_info.SPLIT_TOTAL_NEWBORN_COST,
baby_cost_info.SPLIT_TYPICAL_NEWBORN_COST,
baby_cost_info.SPLIT_PAC_NEWBORN_COST,
baby_cost_info.UNSPLIT_TOTAL_NEWBORN_COST,
baby_cost_info.UNSPLIT_TYPICAL_NEWBORN_COST,
baby_cost_info.UNSPLIT_PAC_NEWBORN_COST,
baby_cost_info.NURSERY_LEVEL


from preg_cost_info
join del_cost_exp_info
on preg_cost_info.ENCRYPT_RECIP_ID_MOM=del_cost_exp_info.ENCRYPT_RECIP_ID_MOM and preg_cost_info.trig_begin_date=del_cost_exp_info.trig_begin_date

join baby_cost_info
on preg_cost_info.ENCRYPT_RECIP_ID_BABY=baby_cost_info.ENCRYPT_RECIP_ID_BABY

group by preg_cost_info.ENCRYPT_RECIP_ID_MOM,
preg_cost_info.ENCRYPT_RECIP_ID_BABY,
preg_cost_info.PREG_master_episode_id,
preg_cost_info.trig_begin_date,
preg_cost_info.year,
preg_cost_info.SPLIT_TOTAL_PREGN_COST,
preg_cost_info.SPLIT_TYPICAL_PREGN_COST,
preg_cost_info.SPLIT_PAC_PREGN_COST,
preg_cost_info.UNSPLIT_TOTAL_PREGN_COST,
preg_cost_info.UNSPLIT_TYPICAL_PREGN_COST,
preg_cost_info.UNSPLIT_PAC_PREGN_COST,
del_cost_exp_info.DEL_master_episode_id,
del_cost_exp_info.DELIVERY_TYPE,
del_cost_exp_info.SPLIT_TOTAL_DEL_COST,
del_cost_exp_info.SPLIT_TYPICAL_DEL_COST,
del_cost_exp_info.SPLIT_PAC_DEL_COST,
del_cost_exp_info.UNSPLIT_TOTAL_DEL_COST,
del_cost_exp_info.UNSPLIT_TYPICAL_DEL_COST,
del_cost_exp_info.UNSPLIT_PAC_DEL_COST,
del_cost_exp_info.split_expected_total_cost ,
del_cost_exp_info.split_expected_typical_cost ,
del_cost_exp_info.split_expected_pac_cost ,
del_cost_exp_info.unsplit_expected_total_cost ,
del_cost_exp_info.unsplit_expected_typical_cost ,
del_cost_exp_info.unsplit_expected_pac_cost ,
baby_cost_info.NEWBORN_master_episode_id,
baby_cost_info.SPLIT_TOTAL_NEWBORN_COST,
baby_cost_info.SPLIT_TYPICAL_NEWBORN_COST,
baby_cost_info.SPLIT_PAC_NEWBORN_COST,
baby_cost_info.UNSPLIT_TOTAL_NEWBORN_COST,
baby_cost_info.UNSPLIT_TYPICAL_NEWBORN_COST,
baby_cost_info.UNSPLIT_PAC_NEWBORN_COST,
baby_cost_info.NURSERY_LEVEL
;



