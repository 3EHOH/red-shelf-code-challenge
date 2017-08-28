?set search_path=epbuilder;

select 'Step: SQL-costRollups-unsplit.sql' as curstep, current_timestamp as datetime;
\set AUTOCOMMIT on

/* ScriptName:costRollups-unsplit.sql    */
/* Last edited: Jean W 05-4-2017, clean up direct, no cast */


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

select 'running 04172017 post_ec scripts no direct errors..';

/* this gets all episode who do not have a parent at their level of association */

INSERT /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=2
)
AND a1.association_level=2;

INSERT /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=3
)
AND a1.association_level=3;

INSERT /*+ direct */  INTO tmp_uber
SELECT
*
FROM association a1
WHERE parent_master_episode_id NOT IN
(
SELECT a2.child_master_episode_id FROM association a2
WHERE a2.association_level=4
)
AND a1.association_level=4;

INSERT /*+ direct */  INTO tmp_uber
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
INSERT /*+ direct */  INTO tmp_sub_association 
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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


INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

INSERT /*+ direct */  INTO tmp_sub_association
(uber_master_episode_id,
parent_master_episode_id,
child_master_episode_id,
association_type,
association_level,
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

update /*+direct*/  tmp_sub_association
SET association_sub_level_inv=
ABS(association_sub_level-(1+(SELECT max(association_sub_level) FROM tmp_sub_association)));

/*=====================*/
/*GOOD LEVEL 1 QUERY... */
/*=====================*/
INSERT /*+ direct */  INTO master_epid_level
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'CL' AS claim_type,
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
ON sig.master_claim_id=cc.master_claim_id
GROUP BY e.master_episode_id;
/*=====================*/




/*==========================*/
/* INSERT BLANK EPISODE ROWS FOR LEVEL 2 */
/*==========================*/
INSERT /*+ direct */  INTO master_epid_level
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'CL' AS claim_type,
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

INSERT /*+ direct */  INTO tmp_mel1
(
	SELECT
	e.master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM episode e
	JOIN master_epid_level mel ON e.master_episode_id = mel.master_episode_id
	AND mel.level = 1
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'CL'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE 
		association_level<=2
	)
);

/*
update  master_epid_level mel
SET mel.cost= (mel.cost+t.cost), mel.cost_t = (mel.cost_t + t.cost_t), mel.cost_c = (mel.cost_c + t.cost_c)
FROM tmp_mel1 t
WHERE t.master_episode_id = mel.master_episode_id
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'CL';
*/

/* OPTION 2 
repeat of the above, working */

update /*+direct*/  master_epid_level
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'CL') AS t2
WHERE master_epid_level.master_episode_id=t2.master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'CL';

TRUNCATE TABLE tmp_mel1;

/* MUST RUN the SAME LEVEL SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL II SUB ROLLUP */

/* at a given mel level, all the children can take on their own costs from mel */
update /*+direct*/  tmp_sub_association
SET cost=mel.cost,
cost_t=(mel.cost_t + mel.cost_tc),
cost_tc=0.0000,
cost_c=mel.cost_c
FROM master_epid_level mel
WHERE child_master_episode_id=mel.master_episode_id
	AND mel.level = 1
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'CL'
	AND association_level=2;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other queryÉ */

TRUNCATE TABLE tmp_mel1;

/* sub association level 2 sub level 1 (deepest progeny) */
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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
update tmp_sub_association
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM 
(SELECT master_episode_id, SUM(cost) AS cost, SUM(cost_t) AS cost_t, SUM(cost_c) AS cost_c 
FROM tmp_mel1 GROUP BY master_episode_id) AS t 
WHERE child_master_episode_id = t.master_episode_id
AND association_sub_level = 31
AND association_level=2 
;*/

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
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
JOIN master_epid_level mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 1
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'CL'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=2)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

/*
update  master_epid_level
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'CL';
*/

update /*+direct*/  master_epid_level AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'CL') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 2
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'CL';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
/*update  master_epid_level
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
AND claim_type = 'CL';*/

update /*+direct*/  master_epid_level 
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
	JOIN master_epid_level m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 2
	AND m.split = 0
	AND m.annualized = 0
	AND m.filter_id = 0
	AND m.claim_type = 'CL'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'CL';


/*==============================*/
/*==============================*/
/* LEVEL III LEVEL 3 LEVEL III LEVEL 3 */
/*==============================*/
/*==============================*/

/*==========================*/
/* INSERT BLANK EPISODE ROWS FOR LEVEL 3 */
/*==========================*/
INSERT /*+ direct */  INTO master_epid_level
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'CL' AS claim_type,
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

INSERT /*+ direct */  INTO tmp_mel1
(
	SELECT
	e.master_episode_id,
	mel.cost,
	mel.cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM episode e
	JOIN master_epid_level mel ON e.master_episode_id = mel.master_episode_id
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'CL'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE 
		association_level<=3
	)
);

/*
update master_epid_level
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'CL';
*/

update /*+direct*/  master_epid_level AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'CL') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'CL';

TRUNCATE TABLE tmp_mel1;

/* MUST RUN the LEVEL III SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL 3 SUB ROLLUP */

/* at a given mel level, all the children can take on their own costs from mel */
update /*+direct*/  tmp_sub_association
SET cost=mel.cost,
cost_t=(mel.cost_t + mel.cost_tc),
cost_tc=0.0000,
cost_c=mel.cost_c
FROM master_epid_level mel 
WHERE child_master_episode_id=mel.master_episode_id
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'CL'
	AND association_level=3;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other queryÉ */

TRUNCATE TABLE tmp_mel1;

/* sub association level 3 sub level 1 (deepest progeny) */
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
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
JOIN master_epid_level mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'CL'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=3)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

/*
update master_epid_level
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'CL';
*/

update /*+direct*/  master_epid_level AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'CL') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'CL';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
/*update master_epid_level
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
AND claim_type = 'CL';*/

update /*+direct*/  master_epid_level 
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
	JOIN master_epid_level m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 3
	AND m.split = 0
	AND m.annualized = 0
	AND m.filter_id = 0
	AND m.claim_type = 'CL'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'CL';

/*==============================*/
/*==============================*/
/* LEVEL IV LEVEL 4 LEVEL IV LEVEL 4 */
/*==============================*/
/*==============================*/


/*==========================*/
/* INSERT BLANK EPISODE ROWS FOR LEVEL 4 */
/*==========================*/
INSERT /*+ direct */  INTO master_epid_level
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'CL' AS claim_type,
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

INSERT /*+ direct */  INTO tmp_mel1
(
	SELECT
	e.master_episode_id,
	mel.cost,
	mel.cost_t AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM episode e
	JOIN master_epid_level mel ON e.master_episode_id = mel.master_episode_id
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'CL'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE 
		association_level<=4
	)
);

/*
update master_epid_level
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'CL';
*/

update /*+direct*/  master_epid_level AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'CL') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'CL';

TRUNCATE TABLE tmp_mel1;

/* MUST RUN the LEVEL 4 SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL 4 SUB ROLLUP */

/* at a given mel level, all the children can take on their own costs from mel */
update /*+direct*/  tmp_sub_association
SET cost=mel.cost,
cost_t=(mel.cost_t + mel.cost_tc),
cost_tc=0.0000,
cost_c=mel.cost_c
FROM master_epid_level mel 
WHERE child_master_episode_id=mel.master_episode_id
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'CL'
	AND association_level=4;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other queryÉ */

TRUNCATE TABLE tmp_mel1;

/* sub association level 4 sub level 1 (deepest progeny) */
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
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
JOIN master_epid_level mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 3
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'CL'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=4)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

/*
update master_epid_level
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'CL';
*/

update /*+direct*/  master_epid_level AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'CL') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'CL';

TRUNCATE TABLE tmp_mel1;

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
/*update master_epid_level
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
AND claim_type = 'CL';*/

update /*+direct*/  master_epid_level 
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
	JOIN master_epid_level m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 4
	AND m.split = 0
	AND m.annualized = 0
	AND m.filter_id = 0
	AND m.claim_type = 'CL'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'CL';

/*==============================*/
/*==============================*/
/* LEVEL V LEVEL 5 LEVEL V LEVEL 5  */
/*==============================*/
/*==============================*/


/*==========================*/
/* INSERT BLANK EPISODE ROWS FOR LEVEL 5 */
/*==========================*/
INSERT /*+ direct */  INTO master_epid_level
SELECT 
0 AS id,
0 AS filter_id,
e.master_episode_id,
'CL' AS claim_type,
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

INSERT /*+ direct */  INTO tmp_mel1
(
	SELECT
	e.master_episode_id,
	mel.cost,
	mel.cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM episode e
	JOIN master_epid_level mel ON e.master_episode_id = mel.master_episode_id
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'CL'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE 
		association_level<=5
	)
);

/*
update master_epid_level
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 5
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'CL';
*/

update /*+direct*/  master_epid_level AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 5
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'CL') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'CL';

TRUNCATE TABLE tmp_mel1;

/* MUST RUN the LEVEL 5 SUB ROLLUPS FOR THIS LEVEL HERE */


/* <BEGIN> THIS WHOLE THING IS CURRENT JUST LEVEL 5 SUB ROLLUP */

/* at a given mel level, all the children can take on their own costs from mel */
update tmp_sub_association
SET cost=mel.cost,
cost_t=(mel.cost_t + mel.cost_tc),
cost_tc=0.0000,
cost_c=mel.cost_c
FROM master_epid_level mel 
WHERE child_master_episode_id=mel.master_episode_id
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'CL'
	AND association_level=5;

/* now it gets juicy */
/* starting with the highest association_sub_level_inv, start rolling up the costs just as we would in the other queryÉ */

TRUNCATE TABLE tmp_mel1;

/* sub association level 5 sub level 1 (deepest progeny) */
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
(
SELECT
sa.parent_master_episode_id AS master_episode_id,
sa.cost as 'cost',
CASE
	WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
	(sa.cost_t + sa.cost_tc) 
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

update /*+direct*/  tmp_sub_association s1
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
INSERT /*+ direct */  INTO tmp_mel1
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
JOIN master_epid_level mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 4
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'CL'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=5)
/* end new for same level associations */
GROUP BY e.master_episode_id
);

/*
update  master_epid_level
SET cost=cost+t.cost, cost_t = cost_t + t.cost_t, cost_c = cost_c + t.cost_c
FROM tmp_mel1 t 
WHERE master_episode_id = t.master_episode_id
AND level = 5
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'CL';
*/

update /*+direct*/  master_epid_level AS mel1
SET cost=t2.cost, cost_t=t2.cost_t, cost_c=t2.cost_c
FROM (
	SELECT 
	mel.master_episode_id AS master_episode_id,
	(mel.cost+t.cost) AS cost, 
	(mel.cost_t + t.cost_t) AS cost_t, 
	(mel.cost_c + t.cost_c) AS cost_c
	FROM master_epid_level mel
	JOIN tmp_mel1 t ON t.master_episode_id = mel.master_episode_id
	AND mel.level = 5
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 0
	AND mel.claim_type = 'CL') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 0
AND mel1.claim_type = 'CL';

/* this grabs the cost of all children that have grandchildren or deeper and adds them in */
/*update  master_epid_level mel
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
AND mel.claim_type = 'CL';*/

update /*+direct*/  master_epid_level 
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
	JOIN master_epid_level m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 5
	AND m.split = 0
	AND m.annualized = 0
	AND m.filter_id = 0
	AND m.claim_type = 'CL'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 5
AND split = 0
AND annualized = 0
AND filter_id = 0
AND claim_type = 'CL';

TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;


