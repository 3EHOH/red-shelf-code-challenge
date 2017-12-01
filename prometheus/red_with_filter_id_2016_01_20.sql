/* TRUNCATE master_epid_level; */

TRUNCATE tmp_ann_mel;
TRUNCATE tmp_annids;
TRUNCATE tmp_annids_c;
TRUNCATE tmp_mel1;

/* The following code sets up the tmp_sub_association table
to get all the costs of episodes that have sub-associations within the same level (chains of parent>child>grandchild etc within the same level */

/* DROP TABLE tmp_uber; */

TRUNCATE tmp_uber;
TRUNCATE tmp_sub_association;

/* this gets all episode who do not have a parent at their level of association */

INSERT INTO tmp_uber
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



/* this should get the grandchildren*/
INSERT INTO tmp_sub_association 
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
u.parent_master_episode_id AS uber_master_episode_id,
u.child_master_episode_id as parent_master_episode_id,
case when a.child_master_episode_id is null then u.child_master_episode_id else a.child_master_episode_id end as child_master_episode_id,
case when a.child_master_episode_id is null then u.association_type else a.association_type end as association_type,
case when a.child_master_episode_id is null then u.association_level else a.association_level end as association_level,
case when a.child_master_episode_id is null then u.association_count else a.association_count end as association_count,
1 AS association_sub_level
FROM tmp_uber u
left JOIN association a ON u.child_master_episode_id=a.parent_master_episode_id
AND a.association_level=u.association_level
JOIN filtered_episodes fe ON fe.master_episode_id=u.child_master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0;

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

INSERT INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
association_count,
association_sub_level)
SELECT 
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

SET @maxLvl = (SELECT max(association_sub_level) FROM tmp_sub_association);

UPDATE tmp_sub_association
SET association_sub_level_inv=
ABS(association_sub_level-(1+@maxLvl));




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


/*filtered episodes */

drop table if exists sub_distinct_filter;
create table sub_distinct_filter as
select  child_master_episode_id,
min(association_level) as association_level
from tmp_sub_association
group by child_master_episode_id;

create index sdf on sub_distinct_filter(child_master_episode_id);
create index sdlf on sub_distinct_filter(association_level);


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

# Make a prepared statement using the eb variable for the current metadata schema
set @s = CONCAT('
insert into report_episode_detail
(Filter_ID, Member_ID, Master_Episode_ID, Episode_ID, Episode_Name, Episode_Description, Episode_Type, MDC, MDC_Description, Episode_Begin_Date, Episode_End_Date, Episode_Length, Level, Split_Total_Cost, Split_Total_PAC_Cost, Split_Total_Typical_Cost, Split_Total_TypicalwPAC_Cost, Facility_ID, Physician_ID)
SELECT
master_epid_level.filter_id,
e.member_id,
e.master_episode_id,
e.episode_id,
eb.NAME,
eb.DESCRIPTION,
eb.TYPE,
eb.MDC_CATEGORY,
mdc_desc.mdc_description,
e.episode_begin_date,
e.episode_end_date,
e.episode_length_days,
master_epid_level.level,
master_epid_level.cost,
master_epid_level.cost_c,
master_epid_level.cost_t,
master_epid_level.cost_tc,
e.attrib_default_facility,
e.attrib_default_physician
#provider.specialty_id

FROM episode e
join `',@eb,'`.episode eb
on e.episode_id=eb.EPISODE_ID 
join master_epid_level
on e.master_episode_id=master_epid_level.master_episode_id
left join mdc_desc
on eb.MDC_CATEGORY=mdc_desc.mdc



left join sub_distinct
on master_epid_level.master_episode_id=sub_distinct.child_master_episode_id 

left join sub_distinct_filter
on master_epid_level.master_episode_id=sub_distinct_filter.child_master_episode_id 

where master_epid_level.level<sub_distinct.association_level and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=0
or  sub_distinct.association_level is null  and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0  and master_epid_level.filter_id=0


or master_epid_level.level<sub_distinct_filter.association_level and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1
or  sub_distinct_filter.association_level is null  and  master_epid_level.claim_type="CL"
AND master_epid_level.split=1
AND master_epid_level.annualized=0 and master_epid_level.filter_id=1
');

# execute the prepared statement
PREPARE stmt1 FROM @s;
EXECUTE stmt1;
DEALLOCATE PREPARE stmt1;


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
left join sub_distinct 
on mel.master_episode_id=sub_distinct.child_master_episode_id 

left join sub_distinct_filter 
on mel.master_episode_id=sub_distinct_filter.child_master_episode_id 

where mel.level<sub_distinct.association_level and  mel.claim_type='CL' AND mel.split=0 AND mel.annualized=0 and filter_id=0
or   sub_distinct.association_level is null and  mel.claim_type='CL' AND mel.split=0 AND mel.annualized=0 and filter_id=0

or mel.level<sub_distinct_filter.association_level and  mel.claim_type='CL' AND mel.split=0 AND mel.annualized=0 and filter_id=1
or   sub_distinct_filter.association_level is null and  mel.claim_type='CL' AND mel.split=0 AND mel.annualized=0  and filter_id=1;

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
left join sub_distinct 
on mel.master_episode_id=sub_distinct.child_master_episode_id 

left join sub_distinct_filter 
on mel.master_episode_id=sub_distinct_filter.child_master_episode_id 

where mel.level<sub_distinct.association_level and  mel.claim_type='CL' AND mel.split=1 AND mel.annualized=1
or   sub_distinct.association_level is null and  mel.claim_type='CL' AND mel.split=1 AND mel.annualized=1

or  mel.level<sub_distinct_filter.association_level and  mel.claim_type='CL' AND mel.split=1 AND mel.annualized=1
or   sub_distinct_filter.association_level is null and  mel.claim_type='CL' AND mel.split=1 AND mel.annualized=1;


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
left join sub_distinct 
on mel.master_episode_id=sub_distinct.child_master_episode_id 

left join sub_distinct_filter 
on mel.master_episode_id=sub_distinct_filter.child_master_episode_id 

where mel.level<sub_distinct_filter.association_level and  mel.claim_type='CL' AND mel.split=0 AND mel.annualized=1
or   sub_distinct_filter.association_level is null and  mel.claim_type='CL' AND mel.split=0 AND mel.annualized=1;

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
red.episode_id,
red.level,
cast(substring_index(substring_index(
	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_1st',
cast(substring_index(substring_index(
	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_99th',
cast(substring_index(substring_index(
	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),
	',', 80/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_80th',
cast(substring_index(substring_index(
	group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_1st',
cast(substring_index(substring_index(
	group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_99th'

from report_episode_detail red
where red.level=1
and red.annualized_split_total_cost>0
and red.episode_id IN ('EC0202', 'EC0301', 'EC0401', 'EC0402', 'EC0508', 'EC0510', 'EC0511', 'EC0518', 'EC0601', 'EC0801', 'EC0802', 'EC1001', 'EC1902', 'EC1903', 'EX9901')
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
red.episode_id,
red.level,
cast(substring_index(substring_index(
	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_1st',
cast(substring_index(substring_index(
	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_99th',
cast(substring_index(substring_index(
	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),
	',', 80/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_80th',
cast(substring_index(substring_index(
	group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_1st',
cast(substring_index(substring_index(
	group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_99th'

from report_episode_detail red
where red.level=2
and red.annualized_split_total_cost>0
and red.episode_id IN ('EC0202', 'EC0301', 'EC0401', 'EC0402', 'EC0508', 'EC0510', 'EC0511', 'EC0518', 'EC0601', 'EC0801', 'EC0802', 'EC1001', 'EC1902', 'EC1903', 'EX9901')
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
red.episode_id,
red.level,
cast(substring_index(substring_index(
	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_1st',
cast(substring_index(substring_index(
	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_99th',
cast(substring_index(substring_index(
	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),
	',', 80/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_80th',
cast(substring_index(substring_index(
	group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_1st',
cast(substring_index(substring_index(
	group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_99th'

from report_episode_detail red
where red.level=3
and red.annualized_split_total_cost>0
and red.episode_id IN ('EC0202', 'EC0301', 'EC0401', 'EC0402', 'EC0508', 'EC0510', 'EC0511', 'EC0518', 'EC0601', 'EC0801', 'EC0802', 'EC1001', 'EC1902', 'EC1903', 'EX9901')
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
red.episode_id,
red.level,
cast(substring_index(substring_index(
	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_1st',
cast(substring_index(substring_index(
	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_99th',
cast(substring_index(substring_index(
	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),
	',', 80/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_80th',
cast(substring_index(substring_index(
	group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_1st',
cast(substring_index(substring_index(
	group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_99th'

from report_episode_detail red
where red.level=4
and red.annualized_split_total_cost>0
and red.episode_id IN ('EC0202', 'EC0301', 'EC0401', 'EC0402', 'EC0508', 'EC0510', 'EC0511', 'EC0518', 'EC0601', 'EC0801', 'EC0802', 'EC1001', 'EC1902', 'EC1903', 'EX9901')
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
red.episode_id,
red.level,
cast(substring_index(substring_index(
	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_1st',
cast(substring_index(substring_index(
	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_99th',
cast(substring_index(substring_index(
	group_concat(red.annualized_split_total_cost order by red.annualized_split_total_cost separator ','),
	',', 80/100 * count(*) +1), ',', -1) as decimal) as 'ann_split_80th',
cast(substring_index(substring_index(
	group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),
	',', 1/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_1st',
cast(substring_index(substring_index(
	group_concat(red.annualized_unsplit_total_cost order by red.annualized_unsplit_total_cost separator ','),
	',', 99/100 * count(*) +1), ',', -1) as decimal) as 'ann_unsplit_99th'

from report_episode_detail red
where red.level=5
and red.annualized_split_total_cost>0
and red.episode_id IN ('EC0202', 'EC0301', 'EC0401', 'EC0402', 'EC0508', 'EC0510', 'EC0511', 'EC0518', 'EC0601', 'EC0801', 'EC0802', 'EC1001', 'EC1902', 'EC1903', 'EX9901')
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

create index emeid on episode(master_episode_id);
create index asmed on association(child_master_episode_id);

create table level_2a as
select
episode.`member_id`,
episode.master_episode_id as LEVEL_1,
case when association.association_level=2 then association.parent_master_episode_id else episode.master_episode_id end as LEVEL_2
from episode
join association
on episode.master_episode_id=association.child_master_episode_id
where association.association_level=2;



create table level_2b as
select
episode.`member_id`,
episode.master_episode_id as LEVEL_1,
episode.master_episode_id  as LEVEL_2
from episode
left join association
on episode.master_episode_id=association.child_master_episode_id
where association.association_level<>2 or association.association_level is null;



create table level_2 as
select * from level_2a
union
select * from level_2b;

drop table level_2a;
drop table level_2b;

/**********************************************/
create table level_3a as
select
level_2.*,
case when association.association_level=3 then association.parent_master_episode_id else level_2.LEVEL_2 end as LEVEL_3
from level_2
join association
on level_2.LEVEL_2=association.child_master_episode_id
where association.association_level=3;



create table level_3b as
select
level_2.*,
level_2.LEVEL_2  as LEVEL_3
from level_2
left join association
on level_2.LEVEL_2=association.child_master_episode_id
where association.association_level<>3 or association.association_level is null;



create table level_3 as
select * from level_3a
union
select * from level_3b;

drop table level_3a;
drop table level_3b;




/*********************************************/
create table level_4a as
select
level_3.*,
case when association.association_level=4 then association.parent_master_episode_id else level_3.LEVEL_3 end as LEVEL_4
from level_3
join association
on level_3.LEVEL_3=association.child_master_episode_id
where association.association_level=4;



create table level_4b as
select
level_3.*,
level_3.LEVEL_3  as LEVEL_4
from level_3
left join association
on level_3.LEVEL_3=association.child_master_episode_id
where association.association_level<>4 or association.association_level is null;



create table level_4 as
select * from level_4a
union
select * from level_4b;

drop table level_4a;
drop table level_4b;




/*********************************************/
create table level_5a as
select
level_4.*,
case when association.association_level=5 then association.parent_master_episode_id else level_4.LEVEL_4 end as LEVEL_5
from level_4
join association
on level_4.LEVEL_4=association.child_master_episode_id
where association.association_level=5;



create table level_5b as
select
level_4.*,
level_4.LEVEL_4  as LEVEL_5
from level_4
left join association
on level_4.LEVEL_4=association.child_master_episode_id
where association.association_level<>5 or association.association_level is null;



create table level_5 as
select * from level_5a
union
select * from level_5b;

drop table level_5a;
drop table level_5b;
drop table level_2;
drop table level_3;
drop table level_4;



/*********************************************/

create table assign_1 as
select
level_5.*,
assignment.claim_source,
assignment.assigned_type,
assignment.master_claim_id
from level_5
join assignment
on level_5.LEVEL_1=assignment.master_episode_id
where assigned_type='C' ;

create table Assign_PAC_Totals as
select
LEVEL_1 as EPISODE,
'1' as Level_Assignment,
count(distinct (case when claim_source = 'IP' then master_claim_id end)) as IP_PAC_Count,
count(distinct (case when claim_source = 'OP'  then master_claim_id end)) as OP_PAC_Count,
count(distinct (case when claim_source = 'PB' then master_claim_id end))  as PB_PAC_Count,
count(distinct (case when claim_source = 'RX' then master_claim_id end)) as RX_PAC_Count
from assign_1
group by LEVEL_1

union

select
LEVEL_2 as EPISODE,
'2' as Level_Assignment,
count(distinct (case when claim_source = 'IP' then master_claim_id end))  as IP_PAC_Count,
count(distinct (case when claim_source = 'OP'  then master_claim_id end)) as OP_PAC_Count,
count(distinct (case when claim_source = 'PB' then master_claim_id end))  as PB_PAC_Count,
count(distinct (case when claim_source = 'RX' then master_claim_id end)) as RX_PAC_Count
from assign_1
group by LEVEL_2
union

select
LEVEL_3 as EPISODE,
'3' as Level_Assignment,
count(distinct (case when claim_source = 'IP' then master_claim_id end))  as IP_PAC_Count,
count(distinct (case when claim_source = 'OP'  then master_claim_id end)) as OP_PAC_Count,
count(distinct (case when claim_source = 'PB' then master_claim_id end)) as PB_PAC_Count,
count(distinct (case when claim_source = 'RX' then master_claim_id end)) as RX_PAC_Count
from assign_1
group by LEVEL_3

union

select
LEVEL_4 as EPISODE,
'4' as Level_Assignment,
count(distinct (case when claim_source = 'IP' then master_claim_id end))  as IP_PAC_Count,
count(distinct (case when claim_source = 'OP'  then master_claim_id end)) as OP_PAC_Count,
count(distinct (case when claim_source = 'PB' then master_claim_id end))   as PB_PAC_Count,
count(distinct (case when claim_source = 'RX' then master_claim_id end)) as RX_PAC_Count
from assign_1
group by LEVEL_4

union

select
LEVEL_5 as EPISODE,
'5' as Level_Assignment,
count(distinct (case when claim_source = 'IP' then master_claim_id end)) as IP_PAC_Count,
count(distinct (case when claim_source = 'OP' then master_claim_id end)) as OP_PAC_Count,
count(distinct (case when claim_source = 'PB' then master_claim_id end)) as PB_PAC_Count,
count(distinct (case when claim_source = 'RX' then master_claim_id end)) as RX_PAC_Count
from assign_1
group by LEVEL_5
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
order by red.level, red.episode_id;



