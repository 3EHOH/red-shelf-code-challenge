

TRUNCATE tmp_ann_mel;
TRUNCATE tmp_annids;
TRUNCATE tmp_annids_c;
TRUNCATE tmp_mel1;

/* !!!NEED TO FIND MAX END DATE TO UPDATE RUN TABLE!
IF NOT DONE ALREADY!!!*/

/*
Need to find all child episodes of the annualized episodes, and all of their progeny
*/

/* first find all parent episodes (the ones we will actually annualize costs on) */
INSERT INTO tmp_annids
SELECT
e.master_episode_id,
'P' AS parent_child_id
FROM episode e
JOIN build_episode_reference be ON e.episode_id = be.episode_id
AND be.END_OF_STUDY=1
AND be.VERSION='5.2.007'
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE episode_begin_date<
(SELECT DATE_ADD(data_latest_begin_date,INTERVAL -1 YEAR) FROM run);

/* get first generational children of parentsÉ */
INSERT INTO tmp_annids_c
SELECT
soc.child_master_episode_id AS master_episode_id
FROM association soc
JOIN tmp_annids t ON soc.parent_master_episode_id = t.master_episode_id
JOIN episode e ON soc.child_master_episode_id = e.master_episode_id
AND e.trig_begin_date>=(SELECT DATE_ADD(data_latest_begin_date,INTERVAL -1 YEAR) FROM run)
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE t.parent_child_id='P';

/* put first generation into table */
INSERT INTO tmp_annids
SELECT
master_episode_id,
'C' AS parent_child_id
FROM tmp_annids_c;

TRUNCATE tmp_annids_c;

/*=====*/
/* below x3 */
/* get second generation */

INSERT INTO tmp_annids_c
SELECT
soc.child_master_episode_id AS master_episode_id
FROM association soc
JOIN tmp_annids t ON soc.parent_master_episode_id = t.master_episode_id
JOIN episode e ON soc.child_master_episode_id = e.master_episode_id
AND e.trig_begin_date>=(SELECT DATE_ADD(data_latest_begin_date,INTERVAL -1 YEAR) FROM run)
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE t.parent_child_id='C';

INSERT INTO tmp_annids
SELECT
master_episode_id,
'C' AS parent_child_id
FROM tmp_annids_c;

TRUNCATE tmp_annids_c;

/* Above X3 */

/* second time... */
/* below x3 */
/* get third generation */

INSERT INTO tmp_annids_c
SELECT
soc.child_master_episode_id AS master_episode_id
FROM association soc
JOIN tmp_annids t ON soc.parent_master_episode_id = t.master_episode_id
JOIN episode e ON soc.child_master_episode_id = e.master_episode_id
AND e.trig_begin_date>=(SELECT DATE_ADD(data_latest_begin_date,INTERVAL -1 YEAR) FROM run)
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE t.parent_child_id='C';

INSERT INTO tmp_annids
SELECT
master_episode_id,
'C' AS parent_child_id
FROM tmp_annids_c;

TRUNCATE tmp_annids_c;

/* Above X3 */

/* below x3 */
/* third time */
/* get fourth generation */

INSERT INTO tmp_annids_c
SELECT
soc.child_master_episode_id AS master_episode_id
FROM association soc
JOIN tmp_annids t ON soc.parent_master_episode_id = t.master_episode_id
JOIN episode e ON soc.child_master_episode_id = e.master_episode_id
AND e.trig_begin_date>=(SELECT DATE_ADD(data_latest_begin_date,INTERVAL -1 YEAR) FROM run)
JOIN filtered_episodes fe ON fe.master_episode_id=e.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
WHERE t.parent_child_id='C';

INSERT INTO tmp_annids
SELECT
master_episode_id,
'C' AS parent_child_id
FROM tmp_annids_c;

TRUNCATE tmp_annids_c;

/* Above X3 */

CREATE TEMPORARY TABLE tmp_ids AS
SELECT DISTINCT * FROM tmp_annids;

TRUNCATE tmp_annids;

INSERT INTO `tmp_annids`
SELECT
master_episode_id,
MIN(parent_child_id)
FROM tmp_ids
GROUP BY master_episode_id;

DROP TABLE tmp_ids;
/*
SELECT count(distinct master_episode_id), count(*) FROM tmp_annids
SELECT master_episode_id, count(master_episode_id) FROM tmp_annids
GROUP BY master_episode_id
HAVING count(master_episode_id)>1
*/
/* grab split level 1 costs for children */

INSERT INTO tmp_ann_mel
SELECT
t.master_episode_id,
1 AS level,
t.parent_child_id,
mel.cost,
mel.cost_t,
mel.cost_tc,
mel.cost_c
FROM tmp_annids t
JOIN master_epid_level mel ON t.master_episode_id = mel.master_episode_id
AND mel.level = 1
AND mel.split = 1
AND mel.annualized = 0
AND mel.filter_id = 0
AND mel.claim_type = 'CL'
WHERE t.parent_child_id='C';

/* grab split level 1 costs for parents */
INSERT INTO tmp_ann_mel
SELECT
t.master_episode_id,
1 AS level,
'P' AS parent_child_id,
if (SUM(cc.allowed_amt/cc.assigned_count) IS NULL, 0.0000, SUM(cc.allowed_amt/cc.assigned_count) ) AS cost,
if (SUM(if(sig.assigned_type='T',cc.allowed_amt/cc.assigned_count,0)) IS NULL, 0.0000, SUM(if(sig.assigned_type='T',cc.allowed_amt/cc.assigned_count,0)) ) AS cost_t,
if (SUM(if(sig.assigned_type='TC',cc.allowed_amt/cc.assigned_count,0)) IS NULL, 0.0000, SUM(if(sig.assigned_type='TC',cc.allowed_amt/cc.assigned_count,0)) ) AS cost_tc,
if (SUM(if(sig.assigned_type='C',cc.allowed_amt/cc.assigned_count,0)) IS NULL, 0.0000, SUM(if(sig.assigned_type='C',cc.allowed_amt/cc.assigned_count,0)) ) AS cost_c
FROM tmp_annids t
LEFT OUTER JOIN assignment sig ON t.master_episode_id = sig.master_episode_id
LEFT OUTER JOIN claims_combined cc
ON sig.master_claim_id=cc.master_claim_id
AND cc.begin_date>=(SELECT DATE_ADD(data_latest_begin_date,INTERVAL -1 YEAR) FROM run)
WHERE t.parent_child_id='P'
GROUP BY t.master_episode_id;



/* TRUNCATE tmp_ann_mel */

/*==========================*/
/* INSERT BLANK EPISODE ROWS FOR LEVEL 2 */
/*==========================*/
INSERT INTO tmp_ann_mel
SELECT
master_episode_id,
2 AS 'level',
parent_child_id,
0.0000 AS 'cost',
0.0000 AS cost_t,
0.0000 AS cost_tc,
0.0000 AS cost_c
FROM tmp_annids;
/*==============================*/



/* SURVIVING (NON-CHILD) EPISODE COSTS... */

INSERT INTO tmp_mel1
(
	SELECT
	t.master_episode_id,
	t.cost,
	(t.cost_t + t.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	t.cost_c
	FROM tmp_ann_mel t
	WHERE t.level = 1
	AND t.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE
		association_level<=2
	)
);

UPDATE tmp_ann_mel t
JOIN tmp_mel1 tl ON t.master_episode_id = tl.master_episode_id
AND t.level = 2
SET t.cost=t.cost+tl.cost, t.cost_t = t.cost_t + tl.cost_t, t.cost_c = t.cost_c + tl.cost_c;

TRUNCATE tmp_mel1;

/*
====
====
*/

/* LVL2 CHILD EPISODE COSTS... */
INSERT INTO tmp_mel1
(
SELECT
e.master_episode_id,
if (soc.association_count>0,
	SUM(mel.cost/soc.association_count),
	SUM(mel.cost)
) AS 'cost',
if (soc.association_count>0,
	if ((soc.association_type='T' OR soc.association_type='Typical'),
		SUM(mel.cost_t/soc.association_count + mel.cost_tc/soc.association_count),
		0.0000
	),
	if ((soc.association_type='T' OR soc.association_type='Typical'),
		SUM(mel.cost_t + mel.cost_tc),
		0.0000
	)
)
AS cost_t,
0.0000 AS cost_tc,

if(soc.association_count>0,
	if ((soc.association_type='T' OR soc.association_type='Typical'),
		SUM(mel.cost_c/soc.association_count),
		SUM(mel.cost_t/soc.association_count + mel.cost_tc/soc.association_count + mel.cost_c/soc.association_count)
	),
	if ((soc.association_type='T' OR soc.association_type='Typical'),
		SUM(mel.cost_c),
		SUM(mel.cost_t + mel.cost_tc + mel.cost_c)
	)
)
AS cost_c
FROM tmp_annids e
JOIN association soc ON e.master_episode_id = soc.parent_master_episode_id
AND soc.child_master_episode_id in (SELECT master_episode_id FROM tmp_annids)
AND soc.association_level=2
JOIN tmp_ann_mel mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 1
GROUP BY e.master_episode_id
);

UPDATE tmp_ann_mel mel
JOIN tmp_mel1 t ON mel.master_episode_id = t.master_episode_id
AND mel.level = 2
SET mel.cost=mel.cost+t.cost, mel.cost_t = mel.cost_t + t.cost_t, mel.cost_c = mel.cost_c + t.cost_c;

TRUNCATE tmp_mel1;



/*==============================*/
/*==============================*/
/* LEVEL III LEVEL 3 LEVEL III LEVEL 3 */
/*==============================*/
/*==============================*/

/*==========================*/
/* INSERT BLANK EPISODE ROWS FOR LEVEL 3 */
/*==========================*/
INSERT INTO tmp_ann_mel
SELECT
master_episode_id,
3 AS 'level',
parent_child_id,
0.0000 AS 'cost',
0.0000 AS cost_t,
0.0000 AS cost_tc,
0.0000 AS cost_c
FROM tmp_annids;
/*==============================*/


/* SURVIVING (NON-CHILD) EPISODE COSTS... */

INSERT INTO tmp_mel1
(
	SELECT
	master_episode_id,
	cost,
	cost_t,
	0.0000 AS cost_tc,
	cost_c
	FROM tmp_ann_mel
	WHERE level = 2
	AND master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE
		association_level<=3
	)

);

UPDATE tmp_ann_mel mel
JOIN tmp_mel1 t ON mel.master_episode_id = t.master_episode_id
AND mel.level = 3
SET mel.cost=mel.cost+t.cost, mel.cost_t = mel.cost_t + t.cost_t, mel.cost_c = mel.cost_c + t.cost_c;

TRUNCATE tmp_mel1;

/* LVL3 CHILD EPISODE COSTS... */
INSERT INTO tmp_mel1
(
SELECT
e.master_episode_id,
if (soc.association_count>0,
	SUM(mel.cost/soc.association_count),
	SUM(mel.cost)
) AS 'cost',
if (soc.association_count>0,
	if ((soc.association_type='T' OR soc.association_type='Typical'),
		SUM(mel.cost_t/soc.association_count),
		0.0000
	),
	if ((soc.association_type='T' OR soc.association_type='Typical'),
		SUM(mel.cost_t),
		0.0000
	)
)
AS cost_t,
0.0000 AS cost_tc,

if(soc.association_count>0,
	if ((soc.association_type='T' OR soc.association_type='Typical'),
		SUM(mel.cost_c/soc.association_count),
		SUM(mel.cost_t/soc.association_count + mel.cost_c/soc.association_count)
	),
	if ((soc.association_type='T' OR soc.association_type='Typical'),
		SUM(mel.cost_c),
		SUM(mel.cost_t + mel.cost_c)
	)
)
AS cost_c
FROM tmp_annids e
JOIN association soc ON e.master_episode_id = soc.parent_master_episode_id
AND soc.child_master_episode_id in (SELECT master_episode_id FROM tmp_annids)
AND soc.association_level=3
JOIN master_epid_level mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 2
AND mel.filter_id=0
AND mel.claim_type='CL'
AND mel.split=1
AND mel.annualized=0
GROUP BY e.master_episode_id
);

UPDATE tmp_ann_mel mel
JOIN tmp_mel1 t ON mel.master_episode_id = t.master_episode_id
AND mel.level = 3
SET mel.cost=mel.cost+t.cost, mel.cost_t = mel.cost_t + t.cost_t, mel.cost_c = mel.cost_c + t.cost_c;

TRUNCATE tmp_mel1;




/*==============================*/
/*==============================*/
/* LEVEL IV LEVEL 4 LEVEL IV LEVEL 4 */
/*==============================*/
/*==============================*/

/*==========================*/
/* INSERT BLANK EPISODE ROWS FOR LEVEL 4 */
/*==========================*/
INSERT INTO tmp_ann_mel
SELECT
master_episode_id,
4 AS 'level',
parent_child_id,
0.0000 AS 'cost',
0.0000 AS cost_t,
0.0000 AS cost_tc,
0.0000 AS cost_c
FROM tmp_annids;
/*==============================*/


/* SURVIVING (NON-CHILD) EPISODE COSTS... */

INSERT INTO tmp_mel1
(
	SELECT
	master_episode_id,
	cost,
	cost_t,
	0.0000 AS cost_tc,
	cost_c
	FROM tmp_ann_mel
	WHERE level = 3
	AND master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE
		association_level<=4
	)

);

UPDATE tmp_ann_mel mel
JOIN tmp_mel1 t ON mel.master_episode_id = t.master_episode_id
AND mel.level = 4
SET mel.cost=mel.cost+t.cost, mel.cost_t = mel.cost_t + t.cost_t, mel.cost_c = mel.cost_c + t.cost_c;

TRUNCATE tmp_mel1;

/* LVL4 CHILD EPISODE COSTS... */
INSERT INTO tmp_mel1
(
SELECT
e.master_episode_id,
if (soc.association_count>0,
	SUM(mel.cost/soc.association_count),
	SUM(mel.cost)
) AS 'cost',
if (soc.association_count>0,
	if ((soc.association_type='T' OR soc.association_type='Typical'),
		SUM(mel.cost_t/soc.association_count),
		0.0000
	),
	if ((soc.association_type='T' OR soc.association_type='Typical'),
		SUM(mel.cost_t),
		0.0000
	)
)
AS cost_t,
0.0000 AS cost_tc,

if(soc.association_count>0,
	if ((soc.association_type='T' OR soc.association_type='Typical'),
		SUM(mel.cost_c/soc.association_count),
		SUM(mel.cost_t/soc.association_count + mel.cost_c/soc.association_count)
	),
	if ((soc.association_type='T' OR soc.association_type='Typical'),
		SUM(mel.cost_c),
		SUM(mel.cost_t + mel.cost_c)
	)
)
AS cost_c
FROM tmp_annids e
JOIN association soc ON e.master_episode_id = soc.parent_master_episode_id
AND soc.child_master_episode_id in (SELECT master_episode_id FROM tmp_annids)
AND soc.association_level=4
JOIN master_epid_level mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 3
AND mel.filter_id=0
AND mel.claim_type='CL'
AND mel.split=1
AND mel.annualized=0
GROUP BY e.master_episode_id
);

UPDATE tmp_ann_mel mel
JOIN tmp_mel1 t ON mel.master_episode_id = t.master_episode_id
AND mel.level = 4
SET mel.cost=mel.cost+t.cost, mel.cost_t = mel.cost_t + t.cost_t, mel.cost_c = mel.cost_c + t.cost_c;

TRUNCATE tmp_mel1;




/*==============================*/
/*==============================*/
/* LEVEL V LEVEL 5 LEVEL V LEVEL 5 */
/*==============================*/
/*==============================*/

/*==========================*/
/* INSERT BLANK EPISODE ROWS FOR LEVEL 5 */
/*==========================*/
INSERT INTO tmp_ann_mel
SELECT
master_episode_id,
5 AS 'level',
parent_child_id,
0.0000 AS 'cost',
0.0000 AS cost_t,
0.0000 AS cost_tc,
0.0000 AS cost_c
FROM tmp_annids;
/*==============================*/


/* SURVIVING (NON-CHILD) EPISODE COSTS... */

INSERT INTO tmp_mel1
(
	SELECT
	master_episode_id,
	cost,
	cost_t,
	0.0000 AS cost_tc,
	cost_c
	FROM tmp_ann_mel
	WHERE level = 4
	AND master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE
		association_level<=5
	)

);

UPDATE tmp_ann_mel mel
JOIN tmp_mel1 t ON mel.master_episode_id = t.master_episode_id
AND mel.level = 5
SET mel.cost=mel.cost+t.cost, mel.cost_t = mel.cost_t + t.cost_t, mel.cost_c = mel.cost_c + t.cost_c;

TRUNCATE tmp_mel1;

/*DELETE FROM tmp_ann_mel WHERE level=5*/

/* LVL5 CHILD EPISODE COSTS... */
INSERT INTO tmp_mel1
(
SELECT
e.master_episode_id,
if (soc.association_count>0,
	SUM(mel.cost/soc.association_count),
	SUM(mel.cost)
) AS 'cost',
if (soc.association_count>0,
	if ((soc.association_type='T' OR soc.association_type='Typical'),
		SUM(mel.cost_t/soc.association_count),
		0.0000
	),
	if ((soc.association_type='T' OR soc.association_type='Typical'),
		SUM(mel.cost_t),
		0.0000
	)
)
AS cost_t,
0.0000 AS cost_tc,

if(soc.association_count>0,
	if ((soc.association_type='T' OR soc.association_type='Typical'),
		SUM(mel.cost_c/soc.association_count),
		SUM(mel.cost_t/soc.association_count + mel.cost_c/soc.association_count)
	),
	if ((soc.association_type='T' OR soc.association_type='Typical'),
		SUM(mel.cost_c),
		SUM(mel.cost_t + mel.cost_c)
	)
)
AS cost_c
FROM tmp_annids e
JOIN association soc ON e.master_episode_id = soc.parent_master_episode_id
AND soc.child_master_episode_id in (SELECT master_episode_id FROM tmp_annids)
AND soc.association_level=5
JOIN master_epid_level mel ON soc.child_master_episode_id = mel.master_episode_id
AND mel.level = 4
AND mel.filter_id=0
AND mel.claim_type='CL'
AND mel.split=1
AND mel.annualized=0
GROUP BY e.master_episode_id
);

UPDATE tmp_ann_mel mel
JOIN tmp_mel1 t ON mel.master_episode_id = t.master_episode_id
AND mel.level = 5
SET mel.cost=mel.cost+t.cost, mel.cost_t = mel.cost_t + t.cost_t, mel.cost_c = mel.cost_c + t.cost_c;

TRUNCATE tmp_mel1;

/* ==================
  !!! FINISH IT !!!
================== */

INSERT INTO master_epid_level
SELECT
NULL AS id,
1 AS filter_id,
master_episode_id,
'CL' AS claim_type,
level,
1 AS split,
1 AS annualized,
cost,
cost_t,
cost_tc,
cost_c,
NULL AS risk_factor_count,
NULL AS sub_type_count,
NULL AS probability_of_complications,
NULL AS IP_stay_count,
NULL AS IP_stay_facility_costs,
NULL AS IP_stay_prof_costs,
NULL AS IP_stay_total_costs,
NULL AS IP_stay_bed_days,
NULL AS IP_stay_avg_length
FROM tmp_ann_mel
WHERE parent_child_id='P';

TRUNCATE tmp_ann_mel;
TRUNCATE tmp_annids;
TRUNCATE tmp_annids_c;
TRUNCATE tmp_mel1;
