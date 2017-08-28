set search_path=epbuilder;


/* TRUNCATE TABLE master_epid_level; */
TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;

/* reset tmp_sub_assocation for split costs… */
update /*+ direct */  tmp_sub_association
SET cost=0, cost_t=0, cost_tc=0, cost_c=0;

drop table if exists tmp_melfil;

CREATE TABLE IF NOT EXISTS tmp_melfil AS (
SELECT distinct
mel.master_episode_id,
mel.cost,
mel.cost_t,
mel.cost_tc,
mel.cost_c,
mel.risk_factor_count,
mel.sub_type_count,
mel.probability_of_complications,
mel.IP_stay_count,
mel.IP_stay_facility_costs,
mel.IP_stay_prof_costs,
mel.IP_stay_total_costs,
mel.IP_stay_bed_days,
mel.IP_stay_avg_length
FROM master_epid_level_claim_type mel
left JOIN filtered_episodes fe ON fe.master_episode_id=mel.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
where mel.filter_id=0
AND mel.claim_type='IP'
AND mel.level=1
AND mel.split=0
AND mel.annualized=0 
group by mel.master_episode_id,
mel.cost,
mel.cost_t,
mel.cost_tc,
mel.cost_c,
mel.risk_factor_count,
mel.sub_type_count,
mel.probability_of_complications,
mel.IP_stay_count,
mel.IP_stay_facility_costs,
mel.IP_stay_prof_costs,
mel.IP_stay_total_costs,
mel.IP_stay_bed_days,
mel.IP_stay_avg_length
);

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'IP' AS claim_type,
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

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'IP' AS claim_type,
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

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'IP' AS claim_type,
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

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'IP' AS claim_type,
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

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'IP' AS claim_type,
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

drop table if exists tmp_melfil;

/* LEVEL 2 SURVIVING (NON-CHILD) EPISODE COSTS... */
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
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=2
	)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP') AS t2
WHERE master_epid_level_claim_type.master_episode_id=t2.master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
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
	AND mel.annualized = 0
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
CASE WHEN sa.association_count>0
	THEN (sa.cost/sa.association_count)
	ELSE (sa.cost)
END AS 'cost',
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count)
		ELSE 0.0000
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t + sa.cost_tc)
		ELSE 0.0000
	END
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c/sa.association_count)
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c)
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
AND sa.association_sub_level =20
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
SUM(CASE WHEN soc.association_count>0
	THEN (mel.cost/soc.association_count)
	ELSE (mel.cost)
END) AS 'cost',
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN(soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t/soc.association_count + mel.cost_tc/soc.association_count)
		ELSE 0.0000
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t + mel.cost_tc)
		ELSE 0.0000
	END
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c/soc.association_count)
		ELSE (mel.cost_t/soc.association_count + mel.cost_tc/soc.association_count + mel.cost_c/soc.association_count)
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c)
		ELSE (mel.cost_t + mel.cost_tc + mel.cost_c)
	END
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
AND mel.annualized = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 2
AND mel1.split = 0
AND mel1.annualized = 0
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
		WHERE association_level=2
		AND association_sub_level=1
		GROUP BY uber_master_episode_id
	) AS sa 
	JOIN master_epid_level_claim_type m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 2
	AND m.split = 0
	AND m.annualized = 0
	AND m.filter_id = 1
	AND m.claim_type = 'IP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 1
AND claim_type = 'IP';

/*==============================*/
/*==============================*/
/* LEVEL III LEVEL 3 LEVEL III LEVEL 3 */
/*==============================*/
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
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=3
	)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 0
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
	AND mel.annualized = 0
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
CASE WHEN sa.association_count>0
	THEN (sa.cost/sa.association_count)
	ELSE (sa.cost)
END AS 'cost',
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count)
		ELSE 0.0000
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t + sa.cost_tc)
		ELSE 0.0000
	END
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c/sa.association_count)
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c)
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
SUM(CASE WHEN soc.association_count>0
	THEN (mel.cost/soc.association_count)
	ELSE (mel.cost)
END) AS 'cost',
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN(soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t/soc.association_count)
		ELSE 0.0000
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t)
		ELSE 0.0000
	END
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c/soc.association_count)
		ELSE (mel.cost_t/soc.association_count + mel.cost_c/soc.association_count)
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c)
		ELSE (mel.cost_t + mel.cost_c)
	END
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
AND mel.annualized = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 0
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
	AND m.annualized = 0
	AND m.filter_id = 1
	AND m.claim_type = 'IP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 1
AND claim_type = 'IP';

/*==============================*/
/*==============================*/
/* LEVEL IV LEVEL 4 LEVEL IV LEVEL 4 */
/*==============================*/
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
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=4
	)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 1
AND mel1.claim_type = 'IP';

TRUNCATE TABLE tmp_mel1;

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
CASE WHEN sa.association_count>0
	THEN (sa.cost/sa.association_count)
	ELSE (sa.cost)
END AS 'cost',
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count)
		ELSE 0.0000
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t + sa.cost_tc)
		ELSE 0.0000
	END
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c/sa.association_count)
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c)
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
SUM(CASE WHEN soc.association_count>0
	THEN (mel.cost/soc.association_count)
	ELSE (mel.cost)
END) AS 'cost',
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN(soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t/soc.association_count)
		ELSE 0.0000
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t)
		ELSE 0.0000
	END
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c/soc.association_count)
		ELSE (mel.cost_t/soc.association_count + mel.cost_c/soc.association_count)
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c)
		ELSE (mel.cost_t + mel.cost_c)
	END
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
AND mel.annualized = 0
AND mel.filter_id = 1
AND mel.claim_type = 'IP'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=4)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 0
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
	AND m.annualized = 0
	AND m.filter_id = 1
	AND m.claim_type = 'IP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 1
AND claim_type = 'IP';

/*==============================*/
/*==============================*/
/* LEVEL V LEVEL 5 LEVEL V LEVEL 5  */
/*==============================*/
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
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=5
	)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 0
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
	AND mel.annualized = 0
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
CASE WHEN sa.association_count>0
	THEN (sa.cost/sa.association_count)
	ELSE (sa.cost)
END AS 'cost',
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count)
		ELSE 0.0000
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t + sa.cost_tc)
		ELSE 0.0000
	END
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c/sa.association_count)
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c)
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
SUM(CASE WHEN soc.association_count>0
	THEN (mel.cost/soc.association_count)
	ELSE (mel.cost)
END) AS 'cost',
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN(soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t/soc.association_count)
		ELSE 0.0000
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t)
		ELSE 0.0000
	END
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c/soc.association_count)
		ELSE (mel.cost_t/soc.association_count + mel.cost_c/soc.association_count)
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c)
		ELSE (mel.cost_t + mel.cost_c)
	END
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
AND mel.annualized = 0
AND mel.filter_id = 1
AND mel.claim_type = 'IP'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=5)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'IP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 0
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
	AND m.annualized = 0
	AND m.filter_id = 1
	AND m.claim_type = 'IP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 5
AND split = 0
AND annualized = 0
AND filter_id = 1
AND claim_type = 'IP';

TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;
/* TRUNCATE TABLE master_epid_level_claim_type; */
TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;

/* reset tmp_sub_assocation for split costs… */
update /*+ direct */  tmp_sub_association
SET cost=0, cost_t=0, cost_tc=0, cost_c=0;

drop table if exists tmp_melfil;

CREATE TABLE IF NOT EXISTS tmp_melfil AS (
SELECT distinct
mel.master_episode_id,
mel.cost,
mel.cost_t,
mel.cost_tc,
mel.cost_c,
mel.risk_factor_count,
mel.sub_type_count,
mel.probability_of_complications,
mel.IP_stay_count,
mel.IP_stay_facility_costs,
mel.IP_stay_prof_costs,
mel.IP_stay_total_costs,
mel.IP_stay_bed_days,
mel.IP_stay_avg_length
FROM master_epid_level_claim_type mel
left JOIN filtered_episodes fe ON fe.master_episode_id=mel.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
where mel.filter_id=0
AND mel.claim_type='OP'
AND mel.level=1
AND mel.split=0
AND mel.annualized=0 
group by mel.master_episode_id,
mel.cost,
mel.cost_t,
mel.cost_tc,
mel.cost_c,
mel.risk_factor_count,
mel.sub_type_count,
mel.probability_of_complications,
mel.IP_stay_count,
mel.IP_stay_facility_costs,
mel.IP_stay_prof_costs,
mel.IP_stay_total_costs,
mel.IP_stay_bed_days,
mel.IP_stay_avg_length
);


insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'OP' AS claim_type,
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

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'OP' AS claim_type,
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

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'OP' AS claim_type,
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

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'OP' AS claim_type,
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

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'OP' AS claim_type,
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

drop table if exists tmp_melfil;

/* LEVEL 2 SURVIVING (NON-CHILD) EPISODE COSTS... */
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
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=2
	)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP') AS t2
WHERE master_epid_level_claim_type.master_episode_id=t2.master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
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
	AND mel.annualized = 0
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
CASE WHEN sa.association_count>0
	THEN (sa.cost/sa.association_count)
	ELSE (sa.cost)
END AS 'cost',
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count)
		ELSE 0.0000
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t + sa.cost_tc)
		ELSE 0.0000
	END
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c/sa.association_count)
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c)
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
AND sa.association_sub_level =20
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
SUM(CASE WHEN soc.association_count>0
	THEN (mel.cost/soc.association_count)
	ELSE (mel.cost)
END) AS 'cost',
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN(soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t/soc.association_count + mel.cost_tc/soc.association_count)
		ELSE 0.0000
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t + mel.cost_tc)
		ELSE 0.0000
	END
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c/soc.association_count)
		ELSE (mel.cost_t/soc.association_count + mel.cost_tc/soc.association_count + mel.cost_c/soc.association_count)
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c)
		ELSE (mel.cost_t + mel.cost_tc + mel.cost_c)
	END
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
AND mel.annualized = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 2
AND mel1.split = 0
AND mel1.annualized = 0
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
		WHERE association_level=2
		AND association_sub_level=1
		GROUP BY uber_master_episode_id
	) AS sa 
	JOIN master_epid_level_claim_type m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 2
	AND m.split = 0
	AND m.annualized = 0
	AND m.filter_id = 1
	AND m.claim_type = 'OP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 1
AND claim_type = 'OP';

/*==============================*/
/*==============================*/
/* LEVEL III LEVEL 3 LEVEL III LEVEL 3 */
/*==============================*/
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
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=3
	)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 0
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
	AND mel.annualized = 0
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
CASE WHEN sa.association_count>0
	THEN (sa.cost/sa.association_count)
	ELSE (sa.cost)
END AS 'cost',
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count)
		ELSE 0.0000
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t + sa.cost_tc)
		ELSE 0.0000
	END
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c/sa.association_count)
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c)
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
SUM(CASE WHEN soc.association_count>0
	THEN (mel.cost/soc.association_count)
	ELSE (mel.cost)
END) AS 'cost',
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN(soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t/soc.association_count)
		ELSE 0.0000
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t)
		ELSE 0.0000
	END
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c/soc.association_count)
		ELSE (mel.cost_t/soc.association_count + mel.cost_c/soc.association_count)
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c)
		ELSE (mel.cost_t + mel.cost_c)
	END
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
AND mel.annualized = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 0
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
	AND m.annualized = 0
	AND m.filter_id = 1
	AND m.claim_type = 'OP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 1
AND claim_type = 'OP';

/*==============================*/
/*==============================*/
/* LEVEL IV LEVEL 4 LEVEL IV LEVEL 4 */
/*==============================*/
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
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=4
	)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 1
AND mel1.claim_type = 'OP';

TRUNCATE TABLE tmp_mel1;

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
CASE WHEN sa.association_count>0
	THEN (sa.cost/sa.association_count)
	ELSE (sa.cost)
END AS 'cost',
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count)
		ELSE 0.0000
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t + sa.cost_tc)
		ELSE 0.0000
	END
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c/sa.association_count)
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c)
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
SUM(CASE WHEN soc.association_count>0
	THEN (mel.cost/soc.association_count)
	ELSE (mel.cost)
END) AS 'cost',
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN(soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t/soc.association_count)
		ELSE 0.0000
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t)
		ELSE 0.0000
	END
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c/soc.association_count)
		ELSE (mel.cost_t/soc.association_count + mel.cost_c/soc.association_count)
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c)
		ELSE (mel.cost_t + mel.cost_c)
	END
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
AND mel.annualized = 0
AND mel.filter_id = 1
AND mel.claim_type = 'OP'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=4)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 0
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
	AND m.annualized = 0
	AND m.filter_id = 1
	AND m.claim_type = 'OP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 1
AND claim_type = 'OP';

/*==============================*/
/*==============================*/
/* LEVEL V LEVEL 5 LEVEL V LEVEL 5  */
/*==============================*/
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
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=5
	)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 0
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
	AND mel.annualized = 0
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
CASE WHEN sa.association_count>0
	THEN (sa.cost/sa.association_count)
	ELSE (sa.cost)
END AS 'cost',
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count)
		ELSE 0.0000
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t + sa.cost_tc)
		ELSE 0.0000
	END
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c/sa.association_count)
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c)
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
SUM(CASE WHEN soc.association_count>0
	THEN (mel.cost/soc.association_count)
	ELSE (mel.cost)
END) AS 'cost',
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN(soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t/soc.association_count)
		ELSE 0.0000
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t)
		ELSE 0.0000
	END
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c/soc.association_count)
		ELSE (mel.cost_t/soc.association_count + mel.cost_c/soc.association_count)
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c)
		ELSE (mel.cost_t + mel.cost_c)
	END
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
AND mel.annualized = 0
AND mel.filter_id = 1
AND mel.claim_type = 'OP'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=5)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'OP') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 0
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
	AND m.annualized = 0
	AND m.filter_id = 1
	AND m.claim_type = 'OP'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 5
AND split = 0
AND annualized = 0
AND filter_id = 1
AND claim_type = 'OP';

TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;
/* TRUNCATE TABLE master_epid_level_claim_type; */
TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;

/* reset tmp_sub_assocation for split costs… */
update /*+ direct */  tmp_sub_association
SET cost=0, cost_t=0, cost_tc=0, cost_c=0;

drop table if exists tmp_melfil;

CREATE TABLE IF NOT EXISTS tmp_melfil AS (
SELECT distinct
mel.master_episode_id,
mel.cost,
mel.cost_t,
mel.cost_tc,
mel.cost_c,
mel.risk_factor_count,
mel.sub_type_count,
mel.probability_of_complications,
mel.IP_stay_count,
mel.IP_stay_facility_costs,
mel.IP_stay_prof_costs,
mel.IP_stay_total_costs,
mel.IP_stay_bed_days,
mel.IP_stay_avg_length
FROM master_epid_level_claim_type mel
left JOIN filtered_episodes fe ON fe.master_episode_id=mel.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
where mel.filter_id=0
AND mel.claim_type='PB'
AND mel.level=1
AND mel.split=0
AND mel.annualized=0 
group by mel.master_episode_id,
mel.cost,
mel.cost_t,
mel.cost_tc,
mel.cost_c,
mel.risk_factor_count,
mel.sub_type_count,
mel.probability_of_complications,
mel.IP_stay_count,
mel.IP_stay_facility_costs,
mel.IP_stay_prof_costs,
mel.IP_stay_total_costs,
mel.IP_stay_bed_days,
mel.IP_stay_avg_length
);


insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'PB' AS claim_type,
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

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'PB' AS claim_type,
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

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'PB' AS claim_type,
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

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'PB' AS claim_type,
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

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'PB' AS claim_type,
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

drop table if exists tmp_melfil;

/* LEVEL 2 SURVIVING (NON-CHILD) EPISODE COSTS... */
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
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=2
	)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB') AS t2
WHERE master_epid_level_claim_type.master_episode_id=t2.master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
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
	AND mel.annualized = 0
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
CASE WHEN sa.association_count>0
	THEN (sa.cost/sa.association_count)
	ELSE (sa.cost)
END AS 'cost',
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count)
		ELSE 0.0000
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t + sa.cost_tc)
		ELSE 0.0000
	END
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c/sa.association_count)
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c)
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
AND sa.association_sub_level =20
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
SUM(CASE WHEN soc.association_count>0
	THEN (mel.cost/soc.association_count)
	ELSE (mel.cost)
END) AS 'cost',
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN(soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t/soc.association_count + mel.cost_tc/soc.association_count)
		ELSE 0.0000
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t + mel.cost_tc)
		ELSE 0.0000
	END
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c/soc.association_count)
		ELSE (mel.cost_t/soc.association_count + mel.cost_tc/soc.association_count + mel.cost_c/soc.association_count)
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c)
		ELSE (mel.cost_t + mel.cost_tc + mel.cost_c)
	END
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
AND mel.annualized = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 2
AND mel1.split = 0
AND mel1.annualized = 0
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
		WHERE association_level=2
		AND association_sub_level=1
		GROUP BY uber_master_episode_id
	) AS sa 
	JOIN master_epid_level_claim_type m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 2
	AND m.split = 0
	AND m.annualized = 0
	AND m.filter_id = 1
	AND m.claim_type = 'PB'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 1
AND claim_type = 'PB';

/*==============================*/
/*==============================*/
/* LEVEL III LEVEL 3 LEVEL III LEVEL 3 */
/*==============================*/
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
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=3
	)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 0
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
	AND mel.annualized = 0
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
CASE WHEN sa.association_count>0
	THEN (sa.cost/sa.association_count)
	ELSE (sa.cost)
END AS 'cost',
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count)
		ELSE 0.0000
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t + sa.cost_tc)
		ELSE 0.0000
	END
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c/sa.association_count)
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c)
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
SUM(CASE WHEN soc.association_count>0
	THEN (mel.cost/soc.association_count)
	ELSE (mel.cost)
END) AS 'cost',
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN(soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t/soc.association_count)
		ELSE 0.0000
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t)
		ELSE 0.0000
	END
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c/soc.association_count)
		ELSE (mel.cost_t/soc.association_count + mel.cost_c/soc.association_count)
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c)
		ELSE (mel.cost_t + mel.cost_c)
	END
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
AND mel.annualized = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 0
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
	AND m.annualized = 0
	AND m.filter_id = 1
	AND m.claim_type = 'PB'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 1
AND claim_type = 'PB';

/*==============================*/
/*==============================*/
/* LEVEL IV LEVEL 4 LEVEL IV LEVEL 4 */
/*==============================*/
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
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=4
	)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 1
AND mel1.claim_type = 'PB';

TRUNCATE TABLE tmp_mel1;

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
CASE WHEN sa.association_count>0
	THEN (sa.cost/sa.association_count)
	ELSE (sa.cost)
END AS 'cost',
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count)
		ELSE 0.0000
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t + sa.cost_tc)
		ELSE 0.0000
	END
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c/sa.association_count)
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c)
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
SUM(CASE WHEN soc.association_count>0
	THEN (mel.cost/soc.association_count)
	ELSE (mel.cost)
END) AS 'cost',
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN(soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t/soc.association_count)
		ELSE 0.0000
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t)
		ELSE 0.0000
	END
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c/soc.association_count)
		ELSE (mel.cost_t/soc.association_count + mel.cost_c/soc.association_count)
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c)
		ELSE (mel.cost_t + mel.cost_c)
	END
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
AND mel.annualized = 0
AND mel.filter_id = 1
AND mel.claim_type = 'PB'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=4)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 0
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
	AND m.annualized = 0
	AND m.filter_id = 1
	AND m.claim_type = 'PB'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 1
AND claim_type = 'PB';

/*==============================*/
/*==============================*/
/* LEVEL V LEVEL 5 LEVEL V LEVEL 5  */
/*==============================*/
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
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=5
	)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 0
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
	AND mel.annualized = 0
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
CASE WHEN sa.association_count>0
	THEN (sa.cost/sa.association_count)
	ELSE (sa.cost)
END AS 'cost',
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count)
		ELSE 0.0000
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t + sa.cost_tc)
		ELSE 0.0000
	END
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c/sa.association_count)
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c)
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
SUM(CASE WHEN soc.association_count>0
	THEN (mel.cost/soc.association_count)
	ELSE (mel.cost)
END) AS 'cost',
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN(soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t/soc.association_count)
		ELSE 0.0000
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t)
		ELSE 0.0000
	END
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c/soc.association_count)
		ELSE (mel.cost_t/soc.association_count + mel.cost_c/soc.association_count)
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c)
		ELSE (mel.cost_t + mel.cost_c)
	END
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
AND mel.annualized = 0
AND mel.filter_id = 1
AND mel.claim_type = 'PB'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=5)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'PB') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 0
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
	AND m.annualized = 0
	AND m.filter_id = 1
	AND m.claim_type = 'PB'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 5
AND split = 0
AND annualized = 0
AND filter_id = 1
AND claim_type = 'PB';

TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;
/* TRUNCATE TABLE master_epid_level_claim_type; */
TRUNCATE TABLE tmp_ann_mel;
TRUNCATE TABLE tmp_annids;
TRUNCATE TABLE tmp_annids_c;
TRUNCATE TABLE tmp_mel1;

/* reset tmp_sub_assocation for split costs… */
update /*+ direct */  tmp_sub_association
SET cost=0, cost_t=0, cost_tc=0, cost_c=0;

drop table if exists tmp_melfil;

CREATE TABLE IF NOT EXISTS tmp_melfil AS (
SELECT distinct
mel.master_episode_id,
mel.cost,
mel.cost_t,
mel.cost_tc,
mel.cost_c,
mel.risk_factor_count,
mel.sub_type_count,
mel.probability_of_complications,
mel.IP_stay_count,
mel.IP_stay_facility_costs,
mel.IP_stay_prof_costs,
mel.IP_stay_total_costs,
mel.IP_stay_bed_days,
mel.IP_stay_avg_length
FROM master_epid_level_claim_type mel
left JOIN filtered_episodes fe ON fe.master_episode_id=mel.master_episode_id
AND fe.filter_id=1
AND fe.filter_fail=0
where mel.filter_id=0
AND mel.claim_type='RX'
AND mel.level=1
AND mel.split=0
AND mel.annualized=0 
group by mel.master_episode_id,
mel.cost,
mel.cost_t,
mel.cost_tc,
mel.cost_c,
mel.risk_factor_count,
mel.sub_type_count,
mel.probability_of_complications,
mel.IP_stay_count,
mel.IP_stay_facility_costs,
mel.IP_stay_prof_costs,
mel.IP_stay_total_costs,
mel.IP_stay_bed_days,
mel.IP_stay_avg_length
);


insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'RX' AS claim_type,
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

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'RX' AS claim_type,
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

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'RX' AS claim_type,
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

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'RX' AS claim_type,
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

insert /*+ direct */  INTO master_epid_level_claim_type
SELECT 
0 AS id,
1 AS filter_id,
master_episode_id,
'RX' AS claim_type,
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

drop table if exists tmp_melfil;

/* LEVEL 2 SURVIVING (NON-CHILD) EPISODE COSTS... */
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
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=2
	)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX') AS t2
WHERE master_epid_level_claim_type.master_episode_id=t2.master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
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
	AND mel.annualized = 0
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
CASE WHEN sa.association_count>0
	THEN (sa.cost/sa.association_count)
	ELSE (sa.cost)
END AS 'cost',
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count)
		ELSE 0.0000
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t + sa.cost_tc)
		ELSE 0.0000
	END
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c/sa.association_count)
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c)
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
AND sa.association_sub_level =20
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
SUM(CASE WHEN soc.association_count>0
	THEN (mel.cost/soc.association_count)
	ELSE (mel.cost)
END) AS 'cost',
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN(soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t/soc.association_count + mel.cost_tc/soc.association_count)
		ELSE 0.0000
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t + mel.cost_tc)
		ELSE 0.0000
	END
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c/soc.association_count)
		ELSE (mel.cost_t/soc.association_count + mel.cost_tc/soc.association_count + mel.cost_c/soc.association_count)
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c)
		ELSE (mel.cost_t + mel.cost_tc + mel.cost_c)
	END
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
AND mel.annualized = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 2
AND mel1.split = 0
AND mel1.annualized = 0
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
		WHERE association_level=2
		AND association_sub_level=1
		GROUP BY uber_master_episode_id
	) AS sa 
	JOIN master_epid_level_claim_type m ON m.master_episode_id=sa.uber_master_episode_id
	AND m.level = 2
	AND m.split = 0
	AND m.annualized = 0
	AND m.filter_id = 1
	AND m.claim_type = 'RX'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 2
AND split = 0
AND annualized = 0
AND filter_id = 1
AND claim_type = 'RX';

/*==============================*/
/*==============================*/
/* LEVEL III LEVEL 3 LEVEL III LEVEL 3 */
/*==============================*/
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
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=3
	)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 0
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
	AND mel.annualized = 0
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
CASE WHEN sa.association_count>0
	THEN (sa.cost/sa.association_count)
	ELSE (sa.cost)
END AS 'cost',
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count)
		ELSE 0.0000
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t + sa.cost_tc)
		ELSE 0.0000
	END
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c/sa.association_count)
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c)
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
SUM(CASE WHEN soc.association_count>0
	THEN (mel.cost/soc.association_count)
	ELSE (mel.cost)
END) AS 'cost',
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN(soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t/soc.association_count)
		ELSE 0.0000
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t)
		ELSE 0.0000
	END
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c/soc.association_count)
		ELSE (mel.cost_t/soc.association_count + mel.cost_c/soc.association_count)
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c)
		ELSE (mel.cost_t + mel.cost_c)
	END
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
AND mel.annualized = 0
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 3
AND mel1.split = 0
AND mel1.annualized = 0
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
	AND m.annualized = 0
	AND m.filter_id = 1
	AND m.claim_type = 'RX'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 3
AND split = 0
AND annualized = 0
AND filter_id = 1
AND claim_type = 'RX';

/*==============================*/
/*==============================*/
/* LEVEL IV LEVEL 4 LEVEL IV LEVEL 4 */
/*==============================*/
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
	AND mel.level = 3
	AND mel.split = 0
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=4
	)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 0
AND mel1.filter_id = 1
AND mel1.claim_type = 'RX';

TRUNCATE TABLE tmp_mel1;

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
CASE WHEN sa.association_count>0
	THEN (sa.cost/sa.association_count)
	ELSE (sa.cost)
END AS 'cost',
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count)
		ELSE 0.0000
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t + sa.cost_tc)
		ELSE 0.0000
	END
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c/sa.association_count)
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c)
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
SUM(CASE WHEN soc.association_count>0
	THEN (mel.cost/soc.association_count)
	ELSE (mel.cost)
END) AS 'cost',
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN(soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t/soc.association_count)
		ELSE 0.0000
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t)
		ELSE 0.0000
	END
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c/soc.association_count)
		ELSE (mel.cost_t/soc.association_count + mel.cost_c/soc.association_count)
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c)
		ELSE (mel.cost_t + mel.cost_c)
	END
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
AND mel.annualized = 0
AND mel.filter_id = 1
AND mel.claim_type = 'RX'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=4)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 4
AND mel1.split = 0
AND mel1.annualized = 0
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
	AND m.annualized = 0
	AND m.filter_id = 1
	AND m.claim_type = 'RX'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 4
AND split = 0
AND annualized = 0
AND filter_id = 1
AND claim_type = 'RX';

/*==============================*/
/*==============================*/
/* LEVEL V LEVEL 5 LEVEL V LEVEL 5  */
/*==============================*/
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
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX'
	WHERE e.master_episode_id NOT IN
	(
		SELECT child_master_episode_id
		FROM association a 
		JOIN filtered_episodes fe ON a.parent_master_episode_id=fe.master_episode_id
		AND fe.filter_fail=0
		AND fe.filter_id=1
		and a.association_level<=5
	)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 0
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
	AND mel.annualized = 0
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
CASE WHEN sa.association_count>0
	THEN (sa.cost/sa.association_count)
	ELSE (sa.cost)
END AS 'cost',
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count)
		ELSE 0.0000
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_t + sa.cost_tc)
		ELSE 0.0000
	END
END AS cost_t,
0.0000 AS cost_tc,
CASE WHEN sa.association_count>0
	THEN CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c/sa.association_count)
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END 
	ELSE CASE WHEN (sa.association_type='T' OR sa.association_type='Typical')
		THEN (sa.cost_c)
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
CASE
	WHEN sa.association_count>0 THEN (sa.cost/sa.association_count) ELSE (sa.cost)
END AS cost,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count) 
		ELSE 0
	END ELSE
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_t + sa.cost_tc) 
		ELSE 0
	END 
END AS cost_t,
0 AS cost_tc,
CASE
	WHEN sa.association_count>0 THEN
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c/sa.association_count) 
		ELSE (sa.cost_t/sa.association_count + sa.cost_tc/sa.association_count + sa.cost_c/sa.association_count)
	END ELSE 
	CASE
		WHEN (sa.association_type='T' OR sa.association_type='Typical') THEN 
		(sa.cost_c) 
		ELSE (sa.cost_t + sa.cost_tc + sa.cost_c)
	END 
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
SUM(CASE WHEN soc.association_count>0
	THEN (mel.cost/soc.association_count)
	ELSE (mel.cost)
END) AS 'cost',
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN(soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t/soc.association_count)
		ELSE 0.0000
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_t)
		ELSE 0.0000
	END
END) AS cost_t,
0.0000 AS cost_tc,
SUM(CASE WHEN soc.association_count>0
	THEN CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c/soc.association_count)
		ELSE (mel.cost_t/soc.association_count + mel.cost_c/soc.association_count)
	END
	ELSE CASE WHEN (soc.association_type='T' OR soc.association_type='Typical')
		THEN (mel.cost_c)
		ELSE (mel.cost_t + mel.cost_c)
	END
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
AND mel.annualized = 0
AND mel.filter_id = 1
AND mel.claim_type = 'RX'
/* new for same level associations */
WHERE soc.child_master_episode_id NOT IN 
(SELECT child_master_episode_id FROM tmp_sub_association WHERE association_level=5)
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
	AND mel.annualized = 0
	AND mel.filter_id = 1
	AND mel.claim_type = 'RX') AS t2
WHERE t2.master_episode_id=mel1.master_episode_id
AND mel1.level = 5
AND mel1.split = 0
AND mel1.annualized = 0
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
	AND m.annualized = 0
	AND m.filter_id = 1
	AND m.claim_type = 'RX'
) as st
WHERE master_episode_id = st.uber_master_episode_id
AND level = 5
AND split = 0
AND annualized = 0
AND filter_id = 1
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
where filter_id=1 and split=0 and annualized=0
group by master_episode_id,
filter_id,
level,
split,
annualized;




