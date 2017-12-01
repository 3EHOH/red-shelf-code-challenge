set search_path=epbuilder;

drop table if exists master_epid_level_claim_type;
create table master_epid_level_claim_type as select * from master_epid_level where claim_type='CL';


TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;

/* The following code sets up the tmp_sub_association table
to get all the costs of episodes that have sub-associations within the same level (chains of parent>child>grandchild etc within the same level */

/* DROP TABLE tmp_uber; */

TRUNCATE TABLE tmp_uber;
TRUNCATE TABLE tmp_sub_association;



insert /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=2
)
AND a1.association_level=2;

insert /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=3
)
AND a1.association_level=3;

insert /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=4
)
AND a1.association_level=4;

insert /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
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
AND a.association_level=u.association_level;

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
AND a.association_level=u.association_level;


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
AND u.association_sub_level=2;

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
AND u.association_sub_level=3;

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
AND u.association_sub_level=4;

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
AND u.association_sub_level=5;

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
AND u.association_sub_level=6;

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
AND u.association_sub_level=7;

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
AND u.association_sub_level=8;

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
AND u.association_sub_level=9;

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
AND u.association_sub_level=10;

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
AND u.association_sub_level=11;

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
AND u.association_sub_level=12;

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
AND u.association_sub_level=13;

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
AND u.association_sub_level=14;

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
AND u.association_sub_level=15;

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
AND u.association_sub_level=16;

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
AND u.association_sub_level=17;

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
AND u.association_sub_level=18;

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
AND u.association_sub_level=19;

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
AND u.association_sub_level=20;

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
AND u.association_sub_level=21;

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
AND u.association_sub_level=22;

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
AND u.association_sub_level=23;

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
AND u.association_sub_level=24;

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
AND u.association_sub_level=25;

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
AND u.association_sub_level=26;

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
AND u.association_sub_level=27;

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
AND u.association_sub_level=28;

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
AND u.association_sub_level=29;

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
AND u.association_sub_level=30;

update /*+ direct */  tmp_sub_association
SET association_sub_level_inv=
ABS(association_sub_level-(1+(SELECT max(association_sub_level) FROM tmp_sub_association)));

/*=====================*/
/*GOOD LEVEL 1 QUERY... */
/*=====================*/
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'IP' AS claim_type,
1 AS 'level',
0 AS split,
0 AS annualized,
SUM(CASE
	WHEN allowed_amt IS NULL THEN 0.0000 ELSE allowed_amt
END) AS cost,
SUM(CASE
	WHEN sig.assigned_type='T' THEN 
	CASE WHEN cc.allowed_amt IS NULL THEN 0 ELSE cc.allowed_amt END 
	ELSE 0 
END) AS cost_t,
SUM(CASE
	WHEN sig.assigned_type='TC' THEN 
	CASE WHEN cc.allowed_amt IS NULL THEN 0 ELSE cc.allowed_amt END 
	ELSE 0 
END) AS cost_tc,
SUM(CASE
	WHEN sig.assigned_type='C' THEN 
	CASE WHEN cc.allowed_amt IS NULL THEN 0 ELSE cc.allowed_amt END 
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
FROM episode e
LEFT OUTER JOIN assignment sig ON e.master_episode_id = sig.master_episode_id
LEFT OUTER JOIN claims_combined cc
ON sig.master_claim_id=cc.master_claim_id and  cc.claim_line_type_code='IP' 
GROUP BY e.master_episode_id;
/*=====================*/


/*==========================*/
/* insert /*+ direct */  BLANK EPISODE ROWS FOR LEVEL 2 */
/*==========================*/
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'IP' AS claim_type,
2 AS 'level',
0 AS split,
0 AS annualized,
0.0000 AS 'cost',
0.0000 AS cost_t,
0.0000 AS cost_tc,
0.0000 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length 
FROM episode e;
/*==============================*/


/* SURVIVING (NON-CHILD) EPISODE COSTS... */

insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	e.master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM episode e
	JOIN master_epid_level_claim_type mel ON e.master_episode_id = mel.master_episode_id
	AND mel.level = 1
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'IP'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE 
		association_level<=2
	)
);

/* NOT WORKING IN VERTICA 
update /*+ direct */  master_epid_level_claim_type mel
FROM tmp_mel1 t
WHERE mel.master_episode_id = t.master_episode_id
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'IP'
SET mel.cost=mel.cost+t.cost, mel.cost_t = mel.cost_t + t.cost_t, mel.cost_c = mel.cost_c + t.cost_c;
*/
/*
update /*+ direct */  master_epid_level_claim_type mel
SET mel.cost= (mel.cost+t.cost), mel.cost_t = (mel.cost_t + t.cost_t), mel.cost_c = (mel.cost_c + t.cost_c)
FROM tmp_mel1 t
WHERE t.master_episode_id = mel.master_episode_id
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'IP';
*/

/* OPTION 2 
repeat of the above, working */

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'IP') AS t2
WHERE master_epid_level_claim_type.master_episode_id=t2.master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 0
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=32
);

/*
update /*+ direct */  tmp_sub_association
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM 
(SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
WHERE child_master_episode_id = t.master_episode_id
AND association_sub_level = 31
AND association_level=2 
;*/

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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
AND sa.association_sub_level = 18
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 1
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'IP'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=2)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'IP';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 2
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'IP';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
/*update /*+ direct */  master_epid_level_claim_type
SET cost=cost+sa.cost, cost_t = cost_t + sa.cost_t, cost_c = cost_c + sa.cost_c
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
WHERE master_episode_id = sa.uber_master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'IP';*/

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
	AND m.annualized = 0
	AND m.filter_id = 0
	AND m.claim_type = 'IP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'IP';


/*==============================*/
/*==============================*/
/* LEVEL III LEVEL 3 LEVEL III LEVEL 3 */
/*==============================*/
/*==============================*/

/*==========================*/
/* insert /*+ direct */  BLANK EPISODE ROWS FOR LEVEL 3 */
/*==========================*/
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'IP' AS claim_type,
3 AS 'level',
0 AS split,
0 AS annualized,
0.0000 AS 'cost',
0.0000 AS cost_t,
0.0000 AS cost_tc,
0.0000 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length 
FROM episode e;
/*==============================*/


/* SURVIVING (NON-CHILD) EPISODE COSTS... */

insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	e.master_episode_id,
	mel.cost,
	mel.cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM episode e
	JOIN master_epid_level_claim_type mel ON e.master_episode_id = mel.master_episode_id
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'IP'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE 
		association_level<=3
	)
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'IP';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 0
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'IP'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=3)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'IP';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'IP';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
/*update /*+ direct */  master_epid_level_claim_type
SET cost=cost+sa.cost, cost_t = cost_t + sa.cost_t, cost_c = cost_c + sa.cost_c
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
WHERE master_episode_id = sa.uber_master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'IP';*/

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
	AND m.annualized = 0
	AND m.filter_id = 0
	AND m.claim_type = 'IP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'IP';

/*==============================*/
/*==============================*/
/* LEVEL IV LEVEL 4 LEVEL IV LEVEL 4 */
/*==============================*/
/*==============================*/


/*==========================*/
/* insert /*+ direct */  BLANK EPISODE ROWS FOR LEVEL 4 */
/*==========================*/
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'IP' AS claim_type,
4 AS 'level',
0 AS split,
0 AS annualized,
0.0000 AS 'cost',
0.0000 AS cost_t,
0.0000 AS cost_tc,
0.0000 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length 
FROM episode e;
/*==============================*/


/* SURVIVING (NON-CHILD) EPISODE COSTS... */

insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	e.master_episode_id,
	mel.cost,
	mel.cost_t AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM episode e
	JOIN master_epid_level_claim_type mel ON e.master_episode_id = mel.master_episode_id
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'IP'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE 
		association_level<=4
	)
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'IP';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 0
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 3
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'IP'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=4)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'IP';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'IP';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
/*update /*+ direct */  master_epid_level_claim_type
SET cost=cost+sa.cost, cost_t = cost_t + sa.cost_t, cost_c = cost_c + sa.cost_c
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
WHERE master_episode_id = sa.uber_master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'IP';*/

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
	AND m.annualized = 0
	AND m.filter_id = 0
	AND m.claim_type = 'IP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'IP';

/*==============================*/
/*==============================*/
/* LEVEL V LEVEL 5 LEVEL V LEVEL 5  */
/*==============================*/
/*==============================*/


/*==========================*/
/* insert /*+ direct */  BLANK EPISODE ROWS FOR LEVEL 5 */
/*==========================*/
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'IP' AS claim_type,
5 AS 'level',
0 AS split,
0 AS annualized,
0.0000 AS 'cost',
0.0000 AS cost_t,
0.0000 AS cost_tc,
0.0000 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length 
FROM episode e;
/*==============================*/


/* SURVIVING (NON-CHILD) EPISODE COSTS... */

insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	e.master_episode_id,
	mel.cost,
	mel.cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM episode e
	JOIN master_epid_level_claim_type mel ON e.master_episode_id = mel.master_episode_id
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'IP'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE 
		association_level<=5
	)
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 5
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'IP';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 0
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
AND sa.association_sub_level = 23
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 23
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 4
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'IP'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=5)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 5
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'IP';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'IP';

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
/*update /*+ direct */  master_epid_level_claim_type mel
SET mel.cost=mel.cost+sa.cost, mel.cost_t = mel.cost_t + sa.cost_t, mel.cost_c = mel.cost_c + sa.cost_c
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
WHERE mel.master_episode_id = sa.uber_master_episode_id
AND mel.level = 5
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'IP';*/

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
	AND m.annualized = 0
	AND m.filter_id = 0
	AND m.claim_type = 'IP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 5
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'IP';

TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;

/*OP STARTS HERE*/


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
/*insert /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=a1.association_level
);

Breaking into separate queries per level because vertica can't to not in on correlated subquery
*/

insert /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=2
)
AND a1.association_level=2;

insert /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=3
)
AND a1.association_level=3;

insert /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=4
)
AND a1.association_level=4;

insert /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
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
AND a.association_level=u.association_level;

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
AND a.association_level=u.association_level;


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
AND u.association_sub_level=2;

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
AND u.association_sub_level=3;

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
AND u.association_sub_level=4;

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
AND u.association_sub_level=5;

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
AND u.association_sub_level=6;

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
AND u.association_sub_level=7;

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
AND u.association_sub_level=8;

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
AND u.association_sub_level=9;

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
AND u.association_sub_level=10;

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
AND u.association_sub_level=11;

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
AND u.association_sub_level=12;

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
AND u.association_sub_level=13;

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
AND u.association_sub_level=14;

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
AND u.association_sub_level=15;

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
AND u.association_sub_level=16;

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
AND u.association_sub_level=17;

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
AND u.association_sub_level=18;

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
AND u.association_sub_level=19;

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
AND u.association_sub_level=20;

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
AND u.association_sub_level=21;

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
AND u.association_sub_level=22;

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
AND u.association_sub_level=23;

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
AND u.association_sub_level=24;

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
AND u.association_sub_level=25;

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
AND u.association_sub_level=26;

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
AND u.association_sub_level=27;

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
AND u.association_sub_level=28;

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
AND u.association_sub_level=29;

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
AND u.association_sub_level=30;

update /*+ direct */  tmp_sub_association
SET association_sub_level_inv=
ABS(association_sub_level-(1+(SELECT max(association_sub_level) FROM tmp_sub_association)));

/*=====================*/
/*GOOD LEVEL 1 QUERY... */
/*=====================*/
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'OP' AS claim_type,
1 AS 'level',
0 AS split,
0 AS annualized,
SUM(CASE
	WHEN allowed_amt IS NULL THEN 0.0000 ELSE allowed_amt
END) AS cost,
SUM(CASE
	WHEN sig.assigned_type='T' THEN 
	CASE WHEN cc.allowed_amt IS NULL THEN 0 ELSE cc.allowed_amt END 
	ELSE 0 
END) AS cost_t,
SUM(CASE
	WHEN sig.assigned_type='TC' THEN 
	CASE WHEN cc.allowed_amt IS NULL THEN 0 ELSE cc.allowed_amt END 
	ELSE 0 
END) AS cost_tc,
SUM(CASE
	WHEN sig.assigned_type='C' THEN 
	CASE WHEN cc.allowed_amt IS NULL THEN 0 ELSE cc.allowed_amt END 
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
FROM episode e
LEFT OUTER JOIN assignment sig ON e.master_episode_id = sig.master_episode_id
LEFT OUTER JOIN claims_combined cc
ON sig.master_claim_id=cc.master_claim_id and  cc.claim_line_type_code='OP'
GROUP BY e.master_episode_id;
/*=====================*/




/*==========================*/
/* insert /*+ direct */  BLANK EPISODE ROWS FOR LEVEL 2 */
/*==========================*/
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'OP' AS claim_type,
2 AS 'level',
0 AS split,
0 AS annualized,
0.0000 AS 'cost',
0.0000 AS cost_t,
0.0000 AS cost_tc,
0.0000 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length 
FROM episode e;
/*==============================*/


/* SURVIVING (NON-CHILD) EPISODE COSTS... */

insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	e.master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM episode e
	JOIN master_epid_level_claim_type mel ON e.master_episode_id = mel.master_episode_id
	AND mel.level = 1
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'OP'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE 
		association_level<=2
	)
);

/* NOT WORKING IN VERTICA 
update /*+ direct */  master_epid_level_claim_type mel
FROM tmp_mel1 t
WHERE mel.master_episode_id = t.master_episode_id
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'OP'
SET mel.cost=mel.cost+t.cost, mel.cost_t = mel.cost_t + t.cost_t, mel.cost_c = mel.cost_c + t.cost_c;
*/
/*
update /*+ direct */  master_epid_level_claim_type mel
SET mel.cost= (mel.cost+t.cost), mel.cost_t = (mel.cost_t + t.cost_t), mel.cost_c = (mel.cost_c + t.cost_c)
FROM tmp_mel1 t
WHERE t.master_episode_id = mel.master_episode_id
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'OP';
*/

/* OPTION 2 
repeat of the above, working */

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'OP') AS t2
WHERE master_epid_level_claim_type.master_episode_id=t2.master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 0
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=32
);

/*
update /*+ direct */  tmp_sub_association
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM 
(SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
WHERE child_master_episode_id = t.master_episode_id
AND association_sub_level = 31
AND association_level=2 
;*/

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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
AND sa.association_sub_level = 18
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 1
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'OP'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=2)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'OP';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 2
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'OP';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
/*update /*+ direct */  master_epid_level_claim_type
SET cost=cost+sa.cost, cost_t = cost_t + sa.cost_t, cost_c = cost_c + sa.cost_c
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
WHERE master_episode_id = sa.uber_master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'OP';*/

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
	AND m.annualized = 0
	AND m.filter_id = 0
	AND m.claim_type = 'OP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'OP';


/*==============================*/
/*==============================*/
/* LEVEL III LEVEL 3 LEVEL III LEVEL 3 */
/*==============================*/
/*==============================*/

/*==========================*/
/* insert /*+ direct */  BLANK EPISODE ROWS FOR LEVEL 3 */
/*==========================*/
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'OP' AS claim_type,
3 AS 'level',
0 AS split,
0 AS annualized,
0.0000 AS 'cost',
0.0000 AS cost_t,
0.0000 AS cost_tc,
0.0000 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length 
FROM episode e;
/*==============================*/


/* SURVIVING (NON-CHILD) EPISODE COSTS... */

insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	e.master_episode_id,
	mel.cost,
	mel.cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM episode e
	JOIN master_epid_level_claim_type mel ON e.master_episode_id = mel.master_episode_id
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'OP'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE 
		association_level<=3
	)
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'OP';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 0
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'OP'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=3)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'OP';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'OP';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
/*update /*+ direct */  master_epid_level_claim_type
SET cost=cost+sa.cost, cost_t = cost_t + sa.cost_t, cost_c = cost_c + sa.cost_c
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
WHERE master_episode_id = sa.uber_master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'OP';*/

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
	AND m.annualized = 0
	AND m.filter_id = 0
	AND m.claim_type = 'OP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'OP';

/*==============================*/
/*==============================*/
/* LEVEL IV LEVEL 4 LEVEL IV LEVEL 4 */
/*==============================*/
/*==============================*/


/*==========================*/
/* insert /*+ direct */  BLANK EPISODE ROWS FOR LEVEL 4 */
/*==========================*/
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'OP' AS claim_type,
4 AS 'level',
0 AS split,
0 AS annualized,
0.0000 AS 'cost',
0.0000 AS cost_t,
0.0000 AS cost_tc,
0.0000 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length 
FROM episode e;
/*==============================*/


/* SURVIVING (NON-CHILD) EPISODE COSTS... */

insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	e.master_episode_id,
	mel.cost,
	mel.cost_t AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM episode e
	JOIN master_epid_level_claim_type mel ON e.master_episode_id = mel.master_episode_id
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'OP'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE 
		association_level<=4
	)
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'OP';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 0
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 3
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'OP'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=4)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'OP';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'OP';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
/*update /*+ direct */  master_epid_level_claim_type
SET cost=cost+sa.cost, cost_t = cost_t + sa.cost_t, cost_c = cost_c + sa.cost_c
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
WHERE master_episode_id = sa.uber_master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'OP';*/

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
	AND m.annualized = 0
	AND m.filter_id = 0
	AND m.claim_type = 'OP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'OP';

/*==============================*/
/*==============================*/
/* LEVEL V LEVEL 5 LEVEL V LEVEL 5  */
/*==============================*/
/*==============================*/


/*==========================*/
/* insert /*+ direct */  BLANK EPISODE ROWS FOR LEVEL 5 */
/*==========================*/
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'OP' AS claim_type,
5 AS 'level',
0 AS split,
0 AS annualized,
0.0000 AS 'cost',
0.0000 AS cost_t,
0.0000 AS cost_tc,
0.0000 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length 
FROM episode e;
/*==============================*/


/* SURVIVING (NON-CHILD) EPISODE COSTS... */

insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	e.master_episode_id,
	mel.cost,
	mel.cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM episode e
	JOIN master_epid_level_claim_type mel ON e.master_episode_id = mel.master_episode_id
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'OP'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE 
		association_level<=5
	)
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 5
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'OP';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 0
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
AND sa.association_sub_level = 23
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 23
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 4
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'OP'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=5)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 5
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'OP';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'OP';

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
/*update /*+ direct */  master_epid_level_claim_type mel
SET mel.cost=mel.cost+sa.cost, mel.cost_t = mel.cost_t + sa.cost_t, mel.cost_c = mel.cost_c + sa.cost_c
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
WHERE mel.master_episode_id = sa.uber_master_episode_id
AND mel.level = 5
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'OP';*/

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
	AND m.annualized = 0
	AND m.filter_id = 0
	AND m.claim_type = 'OP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 5
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'OP';

TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;


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
/*insert /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=a1.association_level
);

Breaking into separate queries per level because vertica can't to not in on correlated subquery
*/

insert /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=2
)
AND a1.association_level=2;

insert /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=3
)
AND a1.association_level=3;

insert /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=4
)
AND a1.association_level=4;

insert /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
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
AND a.association_level=u.association_level;

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
AND a.association_level=u.association_level;


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
AND u.association_sub_level=2;

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
AND u.association_sub_level=3;

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
AND u.association_sub_level=4;

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
AND u.association_sub_level=5;

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
AND u.association_sub_level=6;

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
AND u.association_sub_level=7;

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
AND u.association_sub_level=8;

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
AND u.association_sub_level=9;

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
AND u.association_sub_level=10;

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
AND u.association_sub_level=11;

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
AND u.association_sub_level=12;

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
AND u.association_sub_level=13;

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
AND u.association_sub_level=14;

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
AND u.association_sub_level=15;

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
AND u.association_sub_level=16;

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
AND u.association_sub_level=17;

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
AND u.association_sub_level=18;

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
AND u.association_sub_level=19;

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
AND u.association_sub_level=20;

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
AND u.association_sub_level=21;

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
AND u.association_sub_level=22;

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
AND u.association_sub_level=23;

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
AND u.association_sub_level=24;

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
AND u.association_sub_level=25;

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
AND u.association_sub_level=26;

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
AND u.association_sub_level=27;

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
AND u.association_sub_level=28;

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
AND u.association_sub_level=29;

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
AND u.association_sub_level=30;

update /*+ direct */  tmp_sub_association
SET association_sub_level_inv=
ABS(association_sub_level-(1+(SELECT max(association_sub_level) FROM tmp_sub_association)));

/*=====================*/
/*GOOD LEVEL 1 QUERY... */
/*=====================*/
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'PB' AS claim_type,
1 AS 'level',
0 AS split,
0 AS annualized,
SUM(CASE
	WHEN allowed_amt IS NULL THEN 0.0000 ELSE allowed_amt
END) AS cost,
SUM(CASE
	WHEN sig.assigned_type='T' THEN 
	CASE WHEN cc.allowed_amt IS NULL THEN 0 ELSE cc.allowed_amt END 
	ELSE 0 
END) AS cost_t,
SUM(CASE
	WHEN sig.assigned_type='TC' THEN 
	CASE WHEN cc.allowed_amt IS NULL THEN 0 ELSE cc.allowed_amt END 
	ELSE 0 
END) AS cost_tc,
SUM(CASE
	WHEN sig.assigned_type='C' THEN 
	CASE WHEN cc.allowed_amt IS NULL THEN 0 ELSE cc.allowed_amt END 
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
FROM episode e
LEFT OUTER JOIN assignment sig ON e.master_episode_id = sig.master_episode_id
LEFT OUTER JOIN claims_combined cc 
ON sig.master_claim_id=cc.master_claim_id and cc.claim_line_type_code='PB'
GROUP BY e.master_episode_id;
/*=====================*/




/*==========================*/
/* insert /*+ direct */  BLANK EPISODE ROWS FOR LEVEL 2 */
/*==========================*/
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'PB' AS claim_type,
2 AS 'level',
0 AS split,
0 AS annualized,
0.0000 AS 'cost',
0.0000 AS cost_t,
0.0000 AS cost_tc,
0.0000 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length 
FROM episode e;
/*==============================*/


/* SURVIVING (NON-CHILD) EPISODE COSTS... */

insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	e.master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM episode e
	JOIN master_epid_level_claim_type mel ON e.master_episode_id = mel.master_episode_id
	AND mel.level = 1
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'PB'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE 
		association_level<=2
	)
);

/* NOT WORKING IN VERTICA 
update /*+ direct */  master_epid_level_claim_type mel
FROM tmp_mel1 t
WHERE mel.master_episode_id = t.master_episode_id
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'PB'
SET mel.cost=mel.cost+t.cost, mel.cost_t = mel.cost_t + t.cost_t, mel.cost_c = mel.cost_c + t.cost_c;
*/
/*
update /*+ direct */  master_epid_level_claim_type mel
SET mel.cost= (mel.cost+t.cost), mel.cost_t = (mel.cost_t + t.cost_t), mel.cost_c = (mel.cost_c + t.cost_c)
FROM tmp_mel1 t
WHERE t.master_episode_id = mel.master_episode_id
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'PB';
*/

/* OPTION 2 
repeat of the above, working */

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'PB') AS t2
WHERE master_epid_level_claim_type.master_episode_id=t2.master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 0
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=32
);

/*
update /*+ direct */  tmp_sub_association
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM 
(SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
WHERE child_master_episode_id = t.master_episode_id
AND association_sub_level = 31
AND association_level=2 
;*/

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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
AND sa.association_sub_level = 18
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 1
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'PB'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=2)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'PB';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 2
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'PB';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
/*update /*+ direct */  master_epid_level_claim_type
SET cost=cost+sa.cost, cost_t = cost_t + sa.cost_t, cost_c = cost_c + sa.cost_c
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
WHERE master_episode_id = sa.uber_master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'PB';*/

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
	AND m.annualized = 0
	AND m.filter_id = 0
	AND m.claim_type = 'PB'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'PB';


/*==============================*/
/*==============================*/
/* LEVEL III LEVEL 3 LEVEL III LEVEL 3 */
/*==============================*/
/*==============================*/

/*==========================*/
/* insert /*+ direct */  BLANK EPISODE ROWS FOR LEVEL 3 */
/*==========================*/
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'PB' AS claim_type,
3 AS 'level',
0 AS split,
0 AS annualized,
0.0000 AS 'cost',
0.0000 AS cost_t,
0.0000 AS cost_tc,
0.0000 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length 
FROM episode e;
/*==============================*/


/* SURVIVING (NON-CHILD) EPISODE COSTS... */

insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	e.master_episode_id,
	mel.cost,
	mel.cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM episode e
	JOIN master_epid_level_claim_type mel ON e.master_episode_id = mel.master_episode_id
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'PB'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE 
		association_level<=3
	)
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'PB';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 0
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'PB'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=3)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'PB';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'PB';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
/*update /*+ direct */  master_epid_level_claim_type
SET cost=cost+sa.cost, cost_t = cost_t + sa.cost_t, cost_c = cost_c + sa.cost_c
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
WHERE master_episode_id = sa.uber_master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'PB';*/

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
	AND m.annualized = 0
	AND m.filter_id = 0
	AND m.claim_type = 'PB'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'PB';

/*==============================*/
/*==============================*/
/* LEVEL IV LEVEL 4 LEVEL IV LEVEL 4 */
/*==============================*/
/*==============================*/


/*==========================*/
/* insert /*+ direct */  BLANK EPISODE ROWS FOR LEVEL 4 */
/*==========================*/
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'PB' AS claim_type,
4 AS 'level',
0 AS split,
0 AS annualized,
0.0000 AS 'cost',
0.0000 AS cost_t,
0.0000 AS cost_tc,
0.0000 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length 
FROM episode e;
/*==============================*/


/* SURVIVING (NON-CHILD) EPISODE COSTS... */

insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	e.master_episode_id,
	mel.cost,
	mel.cost_t AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM episode e
	JOIN master_epid_level_claim_type mel ON e.master_episode_id = mel.master_episode_id
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'PB'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE 
		association_level<=4
	)
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'PB';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 0
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 3
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'PB'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=4)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'PB';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'PB';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
/*update /*+ direct */  master_epid_level_claim_type
SET cost=cost+sa.cost, cost_t = cost_t + sa.cost_t, cost_c = cost_c + sa.cost_c
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
WHERE master_episode_id = sa.uber_master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'PB';*/

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
	AND m.annualized = 0
	AND m.filter_id = 0
	AND m.claim_type = 'PB'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'PB';

/*==============================*/
/*==============================*/
/* LEVEL V LEVEL 5 LEVEL V LEVEL 5  */
/*==============================*/
/*==============================*/


/*==========================*/
/* insert /*+ direct */  BLANK EPISODE ROWS FOR LEVEL 5 */
/*==========================*/
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'PB' AS claim_type,
5 AS 'level',
0 AS split,
0 AS annualized,
0.0000 AS 'cost',
0.0000 AS cost_t,
0.0000 AS cost_tc,
0.0000 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length 
FROM episode e;
/*==============================*/


/* SURVIVING (NON-CHILD) EPISODE COSTS... */

insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	e.master_episode_id,
	mel.cost,
	mel.cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM episode e
	JOIN master_epid_level_claim_type mel ON e.master_episode_id = mel.master_episode_id
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'PB'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE 
		association_level<=5
	)
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 5
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'PB';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 0
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
AND sa.association_sub_level = 23
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 23
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 4
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'PB'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=5)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 5
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'PB';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'PB';

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
/*update /*+ direct */  master_epid_level_claim_type mel
SET mel.cost=mel.cost+sa.cost, mel.cost_t = mel.cost_t + sa.cost_t, mel.cost_c = mel.cost_c + sa.cost_c
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
WHERE mel.master_episode_id = sa.uber_master_episode_id
AND mel.level = 5
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'PB';*/

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
	AND m.annualized = 0
	AND m.filter_id = 0
	AND m.claim_type = 'PB'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 5
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'PB';

TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;


TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;

/* The following code sets up the tmp_sub_association table
to get all the costs of episodes that have sub-associations within the same level (chains of parent>child>grandchild etc within the same level */

/* DROP TABLE tmp_uber; */

TRUNCATE TABLE tmp_uber;
TRUNCATE TABLE tmp_sub_association;



insert /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=2
)
AND a1.association_level=2;

insert /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=3
)
AND a1.association_level=3;

insert /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=4
)
AND a1.association_level=4;

insert /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
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
AND a.association_level=u.association_level;

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
AND a.association_level=u.association_level;


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
AND u.association_sub_level=2;

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
AND u.association_sub_level=3;

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
AND u.association_sub_level=4;

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
AND u.association_sub_level=5;

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
AND u.association_sub_level=6;

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
AND u.association_sub_level=7;

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
AND u.association_sub_level=8;

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
AND u.association_sub_level=9;

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
AND u.association_sub_level=10;

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
AND u.association_sub_level=11;

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
AND u.association_sub_level=12;

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
AND u.association_sub_level=13;

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
AND u.association_sub_level=14;

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
AND u.association_sub_level=15;

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
AND u.association_sub_level=16;

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
AND u.association_sub_level=17;

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
AND u.association_sub_level=18;

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
AND u.association_sub_level=19;

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
AND u.association_sub_level=20;

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
AND u.association_sub_level=21;

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
AND u.association_sub_level=22;

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
AND u.association_sub_level=23;

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
AND u.association_sub_level=24;

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
AND u.association_sub_level=25;

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
AND u.association_sub_level=26;

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
AND u.association_sub_level=27;

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
AND u.association_sub_level=28;

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
AND u.association_sub_level=29;

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
AND u.association_sub_level=30;

update /*+ direct */  tmp_sub_association
SET association_sub_level_inv=
ABS(association_sub_level-(1+(SELECT max(association_sub_level) FROM tmp_sub_association)));

/*=====================*/
/*GOOD LEVEL 1 QUERY... */
/*=====================*/
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'RX' AS claim_type,
1 AS 'level',
0 AS split,
0 AS annualized,
SUM(CASE
	WHEN allowed_amt IS NULL THEN 0.0000 ELSE allowed_amt
END) AS cost,
SUM(CASE
	WHEN sig.assigned_type='T' THEN 
	CASE WHEN cc.allowed_amt IS NULL THEN 0 ELSE cc.allowed_amt END 
	ELSE 0 
END) AS cost_t,
SUM(CASE
	WHEN sig.assigned_type='TC' THEN 
	CASE WHEN cc.allowed_amt IS NULL THEN 0 ELSE cc.allowed_amt END 
	ELSE 0 
END) AS cost_tc,
SUM(CASE
	WHEN sig.assigned_type='C' THEN 
	CASE WHEN cc.allowed_amt IS NULL THEN 0 ELSE cc.allowed_amt END 
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
FROM episode e
LEFT OUTER JOIN assignment sig ON e.master_episode_id = sig.master_episode_id
LEFT OUTER JOIN claims_combined cc
ON sig.master_claim_id=cc.master_claim_id and  cc.claim_line_type_code='RX'
GROUP BY e.master_episode_id;
/*=====================*/




/*==========================*/
/* insert   BLANK EPISODE ROWS FOR LEVEL 2 */
/*==========================*/
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'RX' AS claim_type,
2 AS 'level',
0 AS split,
0 AS annualized,
0.0000 AS 'cost',
0.0000 AS cost_t,
0.0000 AS cost_tc,
0.0000 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length 
FROM episode e;
/*==============================*/


/* SURVIVING (NON-CHILD) EPISODE COSTS... */

insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	e.master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM episode e
	JOIN master_epid_level_claim_type mel ON e.master_episode_id = mel.master_episode_id
	AND mel.level = 1
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'RX'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE 
		association_level<=2
	)
);

/* NOT WORKING IN VERTICA 
update /*+ direct */  master_epid_level_claim_type mel
FROM tmp_mel1 t
WHERE mel.master_episode_id = t.master_episode_id
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'RX'
SET mel.cost=mel.cost+t.cost, mel.cost_t = mel.cost_t + t.cost_t, mel.cost_c = mel.cost_c + t.cost_c;
*/
/*
update /*+ direct */  master_epid_level_claim_type mel
SET mel.cost= (mel.cost+t.cost), mel.cost_t = (mel.cost_t + t.cost_t), mel.cost_c = (mel.cost_c + t.cost_c)
FROM tmp_mel1 t
WHERE t.master_episode_id = mel.master_episode_id
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'RX';
*/

/* OPTION 2 
repeat of the above, working */

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'RX') AS t2
WHERE master_epid_level_claim_type.master_episode_id=t2.master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 0
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
	ELSE (sa.cost_t + sa.cost_tc + sa.cost_c) 
END AS cost_c
FROM tmp_sub_association sa
WHERE sa.association_level=2 
AND sa.association_sub_level=32
);

/*
update /*+ direct */  tmp_sub_association
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM 
(SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
WHERE child_master_episode_id = t.master_episode_id
AND association_sub_level = 31
AND association_level=2 
;*/

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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
AND sa.association_sub_level = 18
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 1
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'RX'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=2)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'RX';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 2
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'RX';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
/*update /*+ direct */  master_epid_level_claim_type
SET cost=cost+sa.cost, cost_t = cost_t + sa.cost_t, cost_c = cost_c + sa.cost_c
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
WHERE master_episode_id = sa.uber_master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'RX';*/

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
	AND m.annualized = 0
	AND m.filter_id = 0
	AND m.claim_type = 'RX'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'RX';


/*==============================*/
/*==============================*/
/* LEVEL III LEVEL 3 LEVEL III LEVEL 3 */
/*==============================*/
/*==============================*/

/*==========================*/
/* insert /*+ direct */  BLANK EPISODE ROWS FOR LEVEL 3 */
/*==========================*/
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'RX' AS claim_type,
3 AS 'level',
0 AS split,
0 AS annualized,
0.0000 AS 'cost',
0.0000 AS cost_t,
0.0000 AS cost_tc,
0.0000 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length 
FROM episode e;
/*==============================*/


/* SURVIVING (NON-CHILD) EPISODE COSTS... */

insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	e.master_episode_id,
	mel.cost,
	mel.cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM episode e
	JOIN master_epid_level_claim_type mel ON e.master_episode_id = mel.master_episode_id
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'RX'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE 
		association_level<=3
	)
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'RX';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 0
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'RX'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=3)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'RX';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'RX';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
/*update /*+ direct */  master_epid_level_claim_type
SET cost=cost+sa.cost, cost_t = cost_t + sa.cost_t, cost_c = cost_c + sa.cost_c
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
WHERE master_episode_id = sa.uber_master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'RX';*/

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
	AND m.annualized = 0
	AND m.filter_id = 0
	AND m.claim_type = 'RX'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'RX';

/*==============================*/
/*==============================*/
/* LEVEL IV LEVEL 4 LEVEL IV LEVEL 4 */
/*==============================*/
/*==============================*/


/*==========================*/
/* insert /*+ direct */  BLANK EPISODE ROWS FOR LEVEL 4 */
/*==========================*/
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'RX' AS claim_type,
4 AS 'level',
0 AS split,
0 AS annualized,
0.0000 AS 'cost',
0.0000 AS cost_t,
0.0000 AS cost_tc,
0.0000 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length 
FROM episode e;
/*==============================*/


/* SURVIVING (NON-CHILD) EPISODE COSTS... */

insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	e.master_episode_id,
	mel.cost,
	mel.cost_t AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM episode e
	JOIN master_epid_level_claim_type mel ON e.master_episode_id = mel.master_episode_id
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'RX'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE 
		association_level<=4
	)
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'RX';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 0
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 3
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'RX'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=4)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'RX';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'RX';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
/*update /*+ direct */  master_epid_level_claim_type
SET cost=cost+sa.cost, cost_t = cost_t + sa.cost_t, cost_c = cost_c + sa.cost_c
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
WHERE master_episode_id = sa.uber_master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'RX';*/

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
	AND m.annualized = 0
	AND m.filter_id = 0
	AND m.claim_type = 'RX'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'RX';

/*==============================*/
/*==============================*/
/* LEVEL V LEVEL 5 LEVEL V LEVEL 5  */
/*==============================*/
/*==============================*/


/*==========================*/
/* insert /*+ direct */  BLANK EPISODE ROWS FOR LEVEL 5 */
/*==========================*/
insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'RX' AS claim_type,
5 AS 'level',
0 AS split,
0 AS annualized,
0.0000 AS 'cost',
0.0000 AS cost_t,
0.0000 AS cost_tc,
0.0000 AS cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length 
FROM episode e;
/*==============================*/


/* SURVIVING (NON-CHILD) EPISODE COSTS... */

insert /*+ direct */  INTO tmp_mel1
(
	SELECT
	e.master_episode_id,
	mel.cost,
	mel.cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM episode e
	JOIN master_epid_level_claim_type mel ON e.master_episode_id = mel.master_episode_id
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'RX'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE 
		association_level<=5
	)
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 5
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'RX';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 0
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
AND sa.association_sub_level = 23
AND sa.association_level = 5) as tt
WHERE tt.master_episode_id=s1.child_master_episode_id
AND association_sub_level = 23
AND s1.association_level = 5;

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
insert /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
	ELSE 0.0000 
END AS cost_t,
0.0000 AS cost_tc,
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	sa.cost_c 
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
JOIN master_epid_level_claim_type mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 4
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'RX'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=5)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

/*
update /*+ direct */  master_epid_level_claim_type
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 5
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'RX';
*/

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
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'RX';

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
/*update /*+ direct */  master_epid_level_claim_type mel
SET mel.cost=mel.cost+sa.cost, mel.cost_t = mel.cost_t + sa.cost_t, mel.cost_c = mel.cost_c + sa.cost_c
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
WHERE mel.master_episode_id = sa.uber_master_episode_id
AND mel.level = 5
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'RX';*/

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
	AND m.annualized = 0
	AND m.filter_id = 0
	AND m.claim_type = 'RX'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 5
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'RX';

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
where filter_id=0 and split=0 and annualized=0 
group by master_episode_id,
filter_id,
level,
split,
annualized;


