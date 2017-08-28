/* TRUNCATE master_epid_level; */

TRUNCATE tmp_ann_mel;
TRUNCATE tmp_annids;
TRUNCATE tmp_annids_c;
TRUNCATE tmp_mel1;


CREATE TEMPORARY TABLE IF NOT EXISTS tmp_melfil AS (
SELECT
mel.*
FROM `master_epid_level` mel
JOIN filtered_episodes fe ON fe.master_episode_id=mel.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
AND mel.filter_id=0
AND mel.claim_type='CL'
AND mel.`level`=1
AND mel.split=0
AND mel.annualized=0
);


INSERT INTO master_epid_level
SELECT
NULL AS id,
1 AS filter_id,
master_episode_id,
'CL' AS claim_type,
1 AS 'level',
0 AS split,
0 AS annualized,
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

INSERT INTO master_epid_level
SELECT
NULL AS id,
1 AS filter_id,
master_episode_id,
'CL' AS claim_type,
2 AS 'level',
0 AS split,
0 AS annualized,
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

INSERT INTO master_epid_level
SELECT
NULL AS id,
1 AS filter_id,
master_episode_id,
'CL' AS claim_type,
3 AS 'level',
0 AS split,
0 AS annualized,
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

INSERT INTO master_epid_level
SELECT
NULL AS id,
1 AS filter_id,
master_episode_id,
'CL' AS claim_type,
4 AS 'level',
0 AS split,
0 AS annualized,
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

INSERT INTO master_epid_level
SELECT
NULL AS id,
1 AS filter_id,
master_episode_id,
'CL' AS claim_type,
5 AS 'level',
0 AS split,
0 AS annualized,
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


/* LEVEL 2 SURVIVING (NON-CHILD) EPISODE COSTS... */

INSERT INTO tmp_mel1
(
	SELECT
	master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM master_epid_level mel
	WHERE master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE
		association_level<=2
	)
	AND mel.level = 1
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'CL'
);

UPDATE master_epid_level mel
JOIN tmp_mel1 t ON mel.master_episode_id = t.master_episode_id
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 1
AND mel.claim_type = 'CL'
SET mel.cost=mel.cost+t.cost, mel.cost_t = mel.cost_t + t.cost_t, mel.cost_c = mel.cost_c + t.cost_c;

TRUNCATE tmp_mel1;

/* LVL2 CHILD EPISODE COSTS... */
INSERT INTO tmp_mel1
(
SELECT
mel.master_episode_id,
SUM(mel.cost) as 'cost',
if ((soc.association_type='T' OR soc.association_type='Typical'),
	SUM(mel.cost_t + mel.cost_tc),
	0.0000
	)
	AS cost_t,
0.0000 AS cost_tc,
if ((soc.association_type='T' OR soc.association_type='Typical'),
	SUM(mel.cost_c),
	SUM(mel.cost_t + mel.cost_tc + mel.cost_c)
	)
AS cost_c
FROM master_epid_level mel
JOIN association soc ON mel.master_episode_id = soc.parent_master_episode_id
AND soc.association_level=2
AND mel.level = 1
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 1
AND mel.claim_type = 'CL'
GROUP BY mel.master_episode_id
);

UPDATE master_epid_level mel
JOIN tmp_mel1 t ON mel.master_episode_id = t.master_episode_id
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 1
AND mel.claim_type = 'CL'
SET mel.cost=mel.cost+t.cost, mel.cost_t = mel.cost_t + t.cost_t, mel.cost_c = mel.cost_c + t.cost_c;

TRUNCATE tmp_mel1;


/*==============================*/
/*==============================*/
/* LEVEL III LEVEL 3 LEVEL III LEVEL 3 */
/*==============================*/
/*==============================*/

/* LEVEL 3 SURVIVING (NON-CHILD) EPISODE COSTS... */

INSERT INTO tmp_mel1
(
	SELECT
	master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM master_epid_level mel
	WHERE master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE
		association_level<=3
	)
	AND mel.level = 2
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'CL'
);

UPDATE master_epid_level mel
JOIN tmp_mel1 t ON mel.master_episode_id = t.master_episode_id
AND mel.level = 3
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 1
AND mel.claim_type = 'CL'
SET mel.cost=mel.cost+t.cost, mel.cost_t = mel.cost_t + t.cost_t, mel.cost_c = mel.cost_c + t.cost_c;

TRUNCATE tmp_mel1;

/* LVL2 CHILD EPISODE COSTS... */
INSERT INTO tmp_mel1
(
SELECT
mel.master_episode_id,
SUM(mel.cost) as 'cost',
if ((soc.association_type='T' OR soc.association_type='Typical'),
	SUM(mel.cost_t + mel.cost_tc),
	0.0000
	)
	AS cost_t,
0.0000 AS cost_tc,
if ((soc.association_type='T' OR soc.association_type='Typical'),
	SUM(mel.cost_c),
	SUM(mel.cost_t + mel.cost_tc + mel.cost_c)
	)
AS cost_c
FROM master_epid_level mel
JOIN association soc ON mel.master_episode_id = soc.parent_master_episode_id
AND soc.association_level=3
AND mel.level = 2
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 1
AND mel.claim_type = 'CL'
GROUP BY mel.master_episode_id
);

UPDATE master_epid_level mel
JOIN tmp_mel1 t ON mel.master_episode_id = t.master_episode_id
AND mel.level = 3
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 1
AND mel.claim_type = 'CL'
SET mel.cost=mel.cost+t.cost, mel.cost_t = mel.cost_t + t.cost_t, mel.cost_c = mel.cost_c + t.cost_c;

TRUNCATE tmp_mel1;

/*==============================*/
/*==============================*/
/* LEVEL IV LEVEL 4 LEVEL IV LEVEL 4 */
/*==============================*/
/*==============================*/

/* LEVEL 4 SURVIVING (NON-CHILD) EPISODE COSTS... */

INSERT INTO tmp_mel1
(
	SELECT
	master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM master_epid_level mel
	WHERE master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE
		association_level<=4
	)
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'CL'
);

UPDATE master_epid_level mel
JOIN tmp_mel1 t ON mel.master_episode_id = t.master_episode_id
AND mel.level = 4
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 1
AND mel.claim_type = 'CL'
SET mel.cost=mel.cost+t.cost, mel.cost_t = mel.cost_t + t.cost_t, mel.cost_c = mel.cost_c + t.cost_c;

TRUNCATE tmp_mel1;

/* LVL2 CHILD EPISODE COSTS... */
INSERT INTO tmp_mel1
(
SELECT
mel.master_episode_id,
SUM(mel.cost) as 'cost',
if ((soc.association_type='T' OR soc.association_type='Typical'),
	SUM(mel.cost_t + mel.cost_tc),
	0.0000
	)
	AS cost_t,
0.0000 AS cost_tc,
if ((soc.association_type='T' OR soc.association_type='Typical'),
	SUM(mel.cost_c),
	SUM(mel.cost_t + mel.cost_tc + mel.cost_c)
	)
AS cost_c
FROM master_epid_level mel
JOIN association soc ON mel.master_episode_id = soc.parent_master_episode_id
AND soc.association_level=4
AND mel.level = 3
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 1
AND mel.claim_type = 'CL'
GROUP BY mel.master_episode_id
);

UPDATE master_epid_level mel
JOIN tmp_mel1 t ON mel.master_episode_id = t.master_episode_id
AND mel.level = 4
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 1
AND mel.claim_type = 'CL'
SET mel.cost=mel.cost+t.cost, mel.cost_t = mel.cost_t + t.cost_t, mel.cost_c = mel.cost_c + t.cost_c;

TRUNCATE tmp_mel1;




/*==============================*/
/*==============================*/
/* LEVEL V LEVEL 5 LEVEL V LEVEL 5  */
/*==============================*/
/*==============================*/

/* LEVEL 4 SURVIVING (NON-CHILD) EPISODE COSTS... */

INSERT INTO tmp_mel1
(
	SELECT
	master_episode_id,
	mel.cost,
	(mel.cost_t + mel.cost_tc) AS cost_t,
	0.0000 AS cost_tc,
	mel.cost_c
	FROM master_epid_level mel
	WHERE master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association
		WHERE
		association_level<=5
	)
	AND mel.level = 4
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'CL'
);

UPDATE master_epid_level mel
JOIN tmp_mel1 t ON mel.master_episode_id = t.master_episode_id
AND mel.level = 5
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 1
AND mel.claim_type = 'CL'
SET mel.cost=mel.cost+t.cost, mel.cost_t = mel.cost_t + t.cost_t, mel.cost_c = mel.cost_c + t.cost_c;

TRUNCATE tmp_mel1;

/* LVL2 CHILD EPISODE COSTS... */
INSERT INTO tmp_mel1
(
SELECT
mel.master_episode_id,
SUM(mel.cost) as 'cost',
if ((soc.association_type='T' OR soc.association_type='Typical'),
	SUM(mel.cost_t + mel.cost_tc),
	0.0000
	)
	AS cost_t,
0.0000 AS cost_tc,
if ((soc.association_type='T' OR soc.association_type='Typical'),
	SUM(mel.cost_c),
	SUM(mel.cost_t + mel.cost_tc + mel.cost_c)
	)
AS cost_c
FROM master_epid_level mel
JOIN association soc ON mel.master_episode_id = soc.parent_master_episode_id
AND soc.association_level=5
AND mel.level = 4
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 1
AND mel.claim_type = 'CL'
GROUP BY mel.master_episode_id
);

UPDATE master_epid_level mel
JOIN tmp_mel1 t ON mel.master_episode_id = t.master_episode_id
AND mel.level = 5
AND mel.split = 0
AND mel.annualized = 0
AND mel.filter_id = 1
AND mel.claim_type = 'CL'
SET mel.cost=mel.cost+t.cost, mel.cost_t = mel.cost_t + t.cost_t, mel.cost_c = mel.cost_c + t.cost_c;

TRUNCATE tmp_mel1;
TRUNCATE tmp_ann_mel;
TRUNCATE tmp_annids;
TRUNCATE tmp_annids_c;
TRUNCATE tmp_mel1;
