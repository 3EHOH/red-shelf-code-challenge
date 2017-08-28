set search_path=epbuilder;

/* DROP TABLE tmp_uber; */

TRUNCATE TABLE tmp_uber;
TRUNCATE TABLE tmp_sub_association;

/* this gets all episode who do not have a parent at their level of association */



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
AND a1.association_level=2

;

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
u.parent_master_episode_id,
u.child_master_episode_id,
u.association_type,
u.association_level,
u.association_count,
0 AS association_sub_level
FROM tmp_uber u
left JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
JOIN filtered_episodes fe ON fe.master_episode_id=u.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0


;


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

insert /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT  distinct
a1.parent_master_episode_id as uber_master_episode_id,
a1.parent_master_episode_id,
a1.child_master_episode_id,
a1.association_type,
a1.association_level,
a1.association_count,
0 as association_sub_level 
from association a1
left join tmp_sub_association tr
on a1.parent_master_episode_id=tr.parent_master_episode_id and a1.child_master_episode_id=tr.child_master_episode_id and a1.association_level=tr.association_level
JOIN filtered_episodes fe ON fe.master_episode_id=a1.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;
;

/* TRUNCATE TABLE master_epid_level_claim_type; */

select * from tmp_sub_association left  join episode on tmp_sub_association.parent_master_episode_id=episode.master_episode_id where member_id='AB94731N' ;
select * from filtered_episodes where master_episode_id='EP0520_AE87020D_132690008702861_00';
/* reset tmp_sub_assocation for split costs… */

drop table if exists tmp_sub_association_rank;
create table tmp_sub_association_rank as
SELECT uber_master_episode_id, parent_master_episode_id, child_master_episode_id, association_type, association_level, association_count, association_sub_level,      
   RANK() OVER (PARTITION BY child_master_episode_id, association_type, association_level, association_count 
   ORDER BY uber_master_episode_id, parent_master_episode_id, child_master_episode_id, association_type, association_level, association_count) as rank
   FROM tmp_sub_association where association_count>1
;

insert /*+ direct */  into tmp_sub_association_rank
SELECT uber_master_episode_id, parent_master_episode_id, child_master_episode_id, association_type, association_level, association_count, association_sub_level, 1 as rank
from tmp_sub_association
where association_count=1;

drop table if exists tmp_sub_association_rank_dd;
create table tmp_sub_association_rank_dd as
select distinct *
from tmp_sub_association_rank;




drop table tmp_sub_association_rank;
alter table tmp_sub_association_rank_dd rename to tmp_sub_association_rank;

/*grab level 1 claims */
drop table if exists master_epid_level_claim_type_claims;
create table master_epid_level_claim_type_claims as
SELECT 
episode.master_episode_id,
'1' as level,
episode.episode_begin_date,
episode.episode_end_date,
episode.trig_begin_date,
episode.trig_end_date,
assignment.master_claim_id,
claim_line.physician_id,
claim_line.facility_id,
assignment.claim_source,
assignment.rule,
assignment.assigned_count,
assignment.assigned_type,
claims_combined.begin_date,
claims_combined.end_date,
claims_combined.allowed_amt,
claims_combined.allowed_amt/claims_combined.assigned_count as split_allowed_amt,

episode.master_episode_id as original_assignment
FROM episode
left join assignment
on episode.master_episode_id=assignment.master_episode_id
left join claims_combined
on assignment.master_claim_id=claims_combined.master_claim_id
left join claim_line
on claims_combined.master_claim_id=claim_line.master_claim_id
left join filtered_episodes
on episode.master_episode_id=filtered_episodes.master_episode_id
where filter_id=1 and filter_fail=0
;


/* LEVEL 2 SURVIVING (NON-CHILD) EPISODE COSTS... */


drop table if exists tmp_mel1_claims;
create table tmp_mel1_claims as
	SELECT
mel.master_episode_id,
'2' as level,
mel.episode_begin_date,
mel.episode_end_date,
mel.trig_begin_date,
mel.trig_end_date,
mel.master_claim_id, physician_id, facility_id,
mel.claim_source,
mel.rule,
mel.assigned_count,
mel.assigned_type,
mel.begin_date,
mel.end_date,
mel.allowed_amt,
mel.split_allowed_amt as split_allowed_amt,
original_assignment
	FROM  master_epid_level_claim_type_claims mel 
	WHERE mel.level=1 and mel.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=2
	)

;

insert /*+ direct */  into master_epid_level_claim_type_claims
select
mel.master_episode_id,
'2' as level,
mel.episode_begin_date,
mel.episode_end_date,
mel.trig_begin_date,
mel.trig_end_date,
mel.master_claim_id, physician_id, facility_id,
mel.claim_source,
mel.rule,
mel.assigned_count,
case when assigned_type='TC' then 'T' else mel.assigned_type end as assigned_type,
mel.begin_date,
mel.end_date,
mel.allowed_amt,
mel.split_allowed_amt ,
original_assignment
from tmp_mel1_claims mel
;



drop TABLE tmp_mel1_claims;

/* MUST RUN the SAME LEVEL SUB ROLLUPS FOR THIS LEVEL HERE */


create table tmp_mel1_claims as
select distinct
mel.master_episode_id,
'2' as level,
mel.episode_begin_date,
mel.episode_end_date,
mel.trig_begin_date,
mel.trig_end_date,
mel.master_claim_id, physician_id, facility_id,
mel.claim_source,
mel.rule,
mel.assigned_count,
case when left(tmp_sub_association_rank.association_type,1)='C' then 'C' else mel.assigned_type end as assigned_type,
mel.begin_date,
mel.end_date,
mel.allowed_amt as allowed_amt,
mel.split_allowed_amt/association_count as split_allowed_amt,
original_assignment, association_count, rank

FROM master_epid_level_claim_type_claims mel 
join tmp_sub_association_rank 
on tmp_sub_association_rank.child_master_episode_id=mel.master_episode_id
	AND mel.level = 1
	AND association_level=2
;


/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other query… */
update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1 
and sa.association_level=2
AND sa.association_sub_level=31;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=31;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=30;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=30;


update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=29;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=29;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=28;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=28;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=27;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=27;


update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=26;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=26;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=25;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=25;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=24;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=24;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=23;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=23;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=22;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=22;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=21;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=21;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=20;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=20;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=19;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=19;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=18;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=18;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=17;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=17;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=16;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=16;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=15;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=14;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=14;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=13;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=13;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=12;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=12;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=11;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=11;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=10;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=10;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=9;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=9;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=8;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=8;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=7;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=7;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=6;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=6;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=5;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=5;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=4;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=4;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=3;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=3;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=2;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=2;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=1;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=1;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=2
AND sa.association_sub_level=0;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=2
AND sa.association_sub_level=0;

update /*+ direct */  tmp_mel1_claims mel
set
episode_begin_date=e.episode_begin_date,
episode_end_date=e.episode_end_date,
trig_begin_date=e.trig_begin_date,
trig_end_date=e.trig_end_date
from episode e
where e.master_episodE_id=mel.master_episodE_id;




insert /*+ direct */  into master_epid_level_claim_type_claims
select
mel.master_episode_id,
'2' as level,
mel.episode_begin_date,
mel.episode_end_date,
mel.trig_begin_date,
mel.trig_end_date,
mel.master_claim_id, physician_id, facility_id,
mel.claim_source,
mel.rule,
mel.assigned_count,
mel.assigned_type,
mel.begin_date,
mel.end_date,
allowed_amt,
split_allowed_amt,
original_assignment
from tmp_mel1_claims mel;

/*****START LEVEL 3*****/

drop table if exists tmp_mel1_claims;
create table tmp_mel1_claims as
	SELECT
mel.master_episode_id,
'3' as level,
mel.episode_begin_date,
mel.episode_end_date,
mel.trig_begin_date,
mel.trig_end_date,
mel.master_claim_id, physician_id, facility_id,
mel.claim_source,
mel.rule,
mel.assigned_count,
mel.assigned_type,
mel.begin_date,
mel.end_date,
mel.allowed_amt,
mel.split_allowed_amt,
original_assignment
	FROM  master_epid_level_claim_type_claims mel 
	WHERE mel.level=2 and mel.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=3
	)
;

insert /*+ direct */  into master_epid_level_claim_type_claims
select
mel.master_episode_id,
'3' as level,
mel.episode_begin_date,
mel.episode_end_date,
mel.trig_begin_date,
mel.trig_end_date,
mel.master_claim_id, physician_id, facility_id,
mel.claim_source,
mel.rule,
mel.assigned_count,
case when assigned_type='TC' then 'T' else mel.assigned_type end as assigned_type,
mel.begin_date,
mel.end_date,
mel.allowed_amt,
mel.split_allowed_amt ,
original_assignment
from tmp_mel1_claims mel;



drop TABLE tmp_mel1_claims;

/* MUST RUN the SAME LEVEL SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL II SUB ROLLUP */
/*
create table 	tmp_sub_association_rank_count as
select
tmp_sub_association_rank.*
from tmp_sub_association_rank

union

select * from input_medical_claims where claim_id in ('16208250041095', '16230263091678','16232265701662','16232265702110','16233265944706'); 

select * from input_medical_claims where left(TYPE_OF_BILL,2)='11' and claim_line_id <> 1  and allowed_amt <>0;*/
/* at a given mel level, all the children can take on their own costs from mel */
create table tmp_mel1_claims as
select distinct
mel.master_episode_id,
'3' as level,
mel.episode_begin_date,
mel.episode_end_date,
mel.trig_begin_date,
mel.trig_end_date,
mel.master_claim_id, physician_id, facility_id,
mel.claim_source,
mel.rule,
mel.assigned_count,
case when left(tmp_sub_association_rank.association_type,1)='C' then 'C' else mel.assigned_type end as assigned_type,
mel.begin_date,
mel.end_date,
mel.allowed_amt,
mel.split_allowed_amt,
original_assignment, association_count, rank
FROM master_epid_level_claim_type_claims mel 
join tmp_sub_association_rank 
on tmp_sub_association_rank.child_master_episode_id=mel.master_episode_id
	AND mel.level = 2
	AND association_level=3;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other query… */
update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1 
and sa.association_level=3
AND sa.association_sub_level=31;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=31;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=30;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=30;


update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=29;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=29;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=28;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=28;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=27;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=27;


update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=26;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=26;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=25;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=25;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=24;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=24;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=23;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=23;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=22;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=22;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=21;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=21;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=20;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=20;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=19;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=19;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=18;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=18;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=17;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=17;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=16;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=16;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=15;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=14;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=14;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=13;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=13;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=12;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=12;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=11;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=11;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=10;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=10;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=9;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=9;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=8;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=8;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=7;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=7;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=6;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=6;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=5;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=5;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=4;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=4;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=3;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=3;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=2;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=2;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=1;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=1;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=3
AND sa.association_sub_level=0;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=3
AND sa.association_sub_level=0;

update /*+ direct */  tmp_mel1_claims mel
set
episode_begin_date=e.episode_begin_date,
episode_end_date=e.episode_end_date,
trig_begin_date=e.trig_begin_date,
trig_end_date=e.trig_end_date
from episode e
where e.master_episodE_id=mel.master_episodE_id;

insert /*+ direct */  into master_epid_level_claim_type_claims
select
mel.master_episode_id,
'3' as level,
mel.episode_begin_date,
mel.episode_end_date,
mel.trig_begin_date,
mel.trig_end_date,
mel.master_claim_id, physician_id, facility_id,
mel.claim_source,
mel.rule,
mel.assigned_count,
mel.assigned_type,
mel.begin_date,
mel.end_date,
mel.allowed_amt,
mel.split_allowed_amt ,
original_assignment
from tmp_mel1_claims mel;

/*****START LEVEL 4*****/

drop table if exists tmp_mel1_claims;
create table tmp_mel1_claims as
	SELECT
mel.master_episode_id,
'4' as level,
mel.episode_begin_date,
mel.episode_end_date,
mel.trig_begin_date,
mel.trig_end_date,
mel.master_claim_id, physician_id, facility_id,
mel.claim_source,
mel.rule,
mel.assigned_count,
mel.assigned_type,
mel.begin_date,
mel.end_date,
mel.allowed_amt,
mel.split_allowed_amt,
original_assignment
	FROM  master_epid_level_claim_type_claims mel 
	WHERE mel.level=3 and mel.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=4
	)
;

insert /*+ direct */  into master_epid_level_claim_type_claims
select
mel.master_episode_id,
'4' as level,
mel.episode_begin_date,
mel.episode_end_date,
mel.trig_begin_date,
mel.trig_end_date,
mel.master_claim_id, physician_id, facility_id,
mel.claim_source,
mel.rule,
mel.assigned_count,
case when assigned_type='TC' then 'T' else mel.assigned_type end as assigned_type,
mel.begin_date,
mel.end_date,
mel.allowed_amt,
mel.split_allowed_amt,
original_assignment
from tmp_mel1_claims mel
	;



drop TABLE tmp_mel1_claims;

/* MUST RUN the SAME LEVEL SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL II SUB ROLLUP */
/*
create table 	tmp_sub_association_rank_count as
select
tmp_sub_association_rank.*
from tmp_sub_association_rank

union

select * from input_medical_claims where claim_id in ('16208250041095', '16230263091678','16232265701662','16232265702110','16233265944706'); 

select * from input_medical_claims where left(TYPE_OF_BILL,2)='11' and claim_line_id <> 1  and allowed_amt <>0;*/
/* at a given mel level, all the children can take on their own costs from mel */
create table tmp_mel1_claims as
select distinct
mel.master_episode_id,
'4' as level,
mel.episode_begin_date,
mel.episode_end_date,
mel.trig_begin_date,
mel.trig_end_date,
mel.master_claim_id, physician_id, facility_id,
mel.claim_source,
mel.rule,
mel.assigned_count,
case when left(tmp_sub_association_rank.association_type,1)='C' then 'C' else mel.assigned_type end as assigned_type,
mel.begin_date,
mel.end_date,
mel.allowed_amt as allowed_amt,
mel.split_allowed_amt/association_count as split_allowed_amt,
original_assignment, association_count, rank
FROM master_epid_level_claim_type_claims mel 
join tmp_sub_association_rank 
on tmp_sub_association_rank.child_master_episode_id=mel.master_episode_id
	AND mel.level = 3
	AND association_level=4;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other query… */
update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1 
and sa.association_level=4
AND sa.association_sub_level=31;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=31;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=30;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=30;


update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=29;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=29;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=28;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=28;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=27;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=27;


update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=26;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=26;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=25;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=25;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=24;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=24;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=23;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=23;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=22;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=22;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=21;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=21;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=20;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=20;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=19;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=19;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=18;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=18;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=17;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=17;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=16;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=16;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=15;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=14;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=14;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=13;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=13;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=12;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=12;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=11;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=11;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=10;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=10;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=9;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=9;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=8;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=8;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=7;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=7;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=6;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=6;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=5;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=5;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=4;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=4;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=3;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=3;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=2;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=2;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=1;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=1;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=4
AND sa.association_sub_level=0;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=4
AND sa.association_sub_level=0;

update /*+ direct */  tmp_mel1_claims mel
set
episode_begin_date=e.episode_begin_date,
episode_end_date=e.episode_end_date,
trig_begin_date=e.trig_begin_date,
trig_end_date=e.trig_end_date
from episode e
where e.master_episodE_id=mel.master_episodE_id;

insert /*+ direct */  into master_epid_level_claim_type_claims
select
mel.master_episode_id,
'4' as level,
mel.episode_begin_date,
mel.episode_end_date,
mel.trig_begin_date,
mel.trig_end_date,
mel.master_claim_id, physician_id, facility_id,
mel.claim_source,
mel.rule,
mel.assigned_count,
mel.assigned_type,
mel.begin_date,
mel.end_date,
allowed_amt,
split_allowed_amt,
original_assignment
from tmp_mel1_claims mel;

/*****START LEVEL 5*****/


drop table if exists tmp_mel1_claims;
create table tmp_mel1_claims as
	SELECT
mel.master_episode_id,
'5' as level,
mel.episode_begin_date,
mel.episode_end_date,
mel.trig_begin_date,
mel.trig_end_date,
mel.master_claim_id, physician_id, facility_id,
mel.claim_source,
mel.rule,
mel.assigned_count,
mel.assigned_type,
mel.begin_date,
mel.end_date,
mel.allowed_amt,
mel.split_allowed_amt,
original_assignment
	FROM  master_epid_level_claim_type_claims mel 
	WHERE mel.level=4 and mel.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=5
	)
;

insert /*+ direct */  into master_epid_level_claim_type_claims
select
mel.master_episode_id,
'5' as level,
mel.episode_begin_date,
mel.episode_end_date,
mel.trig_begin_date,
mel.trig_end_date,
mel.master_claim_id, physician_id, facility_id,
mel.claim_source,
mel.rule,
mel.assigned_count,
case when assigned_type='TC' then 'T' else mel.assigned_type end as assigned_type,
mel.begin_date,
mel.end_date,
mel.allowed_amt,
mel.split_allowed_amt,
original_assignment
from tmp_mel1_claims mel;



drop TABLE tmp_mel1_claims;

/* MUST RUN the SAME LEVEL SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL II SUB ROLLUP */
/*
create table 	tmp_sub_association_rank_count as
select
tmp_sub_association_rank.*
from tmp_sub_association_rank

union

select * from input_medical_claims where claim_id in ('16208250041095', '16230263091678','16232265701662','16232265702110','16233265944706'); 

select * from input_medical_claims where left(TYPE_OF_BILL,2)='11' and claim_line_id <> 1  and allowed_amt <>0;*/
/* at a given mel level, all the children can take on their own costs from mel */
create table tmp_mel1_claims as
select distinct
mel.master_episode_id,
'5' as level,
mel.episode_begin_date,
mel.episode_end_date,
mel.trig_begin_date,
mel.trig_end_date,
mel.master_claim_id, physician_id, facility_id,
mel.claim_source,
mel.rule,
mel.assigned_count,
case when left(tmp_sub_association_rank.association_type,1)='C' then 'C' else mel.assigned_type end as assigned_type,
mel.begin_date,
mel.end_date,
mel.allowed_amt as allowed_amt,
mel.split_allowed_amt/association_count as split_allowed_amt,
original_assignment, association_count, rank
FROM master_epid_level_claim_type_claims mel 
join tmp_sub_association_rank 
on tmp_sub_association_rank.child_master_episode_id=mel.master_episode_id
	AND mel.level = 4
	AND association_level=5

;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other query… */
select * from tmp_mel1_claims;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1 
and sa.association_level=5
AND sa.association_sub_level=31;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=31;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=30;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=30;


update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=29;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=29;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=28;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=28;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=27;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=27;


update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=26;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=26;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=25;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=25;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=24;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=24;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=23;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=23;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=22;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=22;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=21;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=21;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=20;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=20;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=19;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=19;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=18;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=18;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=17;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=17;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=16;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=16;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=15;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=14;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=14;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=13;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=13;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=12;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=12;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=11;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=11;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=10;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=10;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=9;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=9;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=8;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=8;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=7;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=7;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=6;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=6;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=5;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=5;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=4;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=4;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=3;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=3;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=2;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=2;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=1;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=1;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and  sa.rank=tmp_mel1_claims.rank and sa.association_count>1
and sa.association_level=5
AND sa.association_sub_level=0;

update /*+ direct */  tmp_mel1_claims
set master_episode_id=sa.parent_master_episode_id
FROM tmp_sub_association_rank sa 
WHERE sa.child_master_episode_id=tmp_mel1_claims.master_episode_id and sa.association_count=1 
and sa.association_level=5
AND sa.association_sub_level=0;


update /*+ direct */  tmp_mel1_claims mel
set
episode_begin_date=e.episode_begin_date,
episode_end_date=e.episode_end_date,
trig_begin_date=e.trig_begin_date,
trig_end_date=e.trig_end_date
from episode e
where e.master_episodE_id=mel.master_episodE_id;

insert /*+ direct */  into master_epid_level_claim_type_claims
select
mel.master_episode_id,
'5' as level,
mel.episode_begin_date,
mel.episode_end_date,
mel.trig_begin_date,
mel.trig_end_date,
mel.master_claim_id, physician_id, facility_id,
mel.claim_source,
mel.rule,
mel.assigned_count,
mel.assigned_type,
mel.begin_date,
mel.end_date,
mel.allowed_amt,
mel.split_allowed_amt,
original_assignment
from tmp_mel1_claims mel;
/*********************/
drop table if exists test;
create table test as
select 
melc.master_episode_id,
melc.level,
sum(allowed_amt) as unsplit,
sum(split_allowed_amt) as split
from master_epid_level_claim_type_claims melc
group by melc.master_episode_id, melc.level;

select 
melc.master_episode_id,
melc.level,
melc.unsplit as unsplit_calc,
melc.split as split_calc,
sum(case when mel.split=1 then 0 else cost end) as unsplit,
sum(case when mel.split=0 then 0 else cost end ) as split,
melc.unsplit-sum(case when mel.split=1 then 0 else cost end),
melc.split-sum(case when mel.split=0 then 0 else cost end )
from test melc
left join master_epid_level mel
on melc.master_episode_id=mel.master_episode_id and melc.level=mel.level
where annualized=0 and filter_id=1  and melc.unsplit is not null and melc.level=5 
group by melc.master_episode_id,
melc.level, melc.split, melc.unsplit;

/* level 1-3 PASS QC!!!!!*/
