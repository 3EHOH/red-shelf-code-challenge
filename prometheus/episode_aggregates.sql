/* AFTER COST ROLLUPS */

/*==============================*/
/* unsplit aggregates level 1   */
/*==============================*/
INSERT INTO episode_aggregates
SELECT
NULL AS id,
e.episode_id,
1 AS level,
COUNT(mel.master_episode_id) AS episode_count,
SUM(cost) AS total_cost_unsplit,
NULL AS total_cost_split,
SUM(mel.cost)/COUNT(mel.master_episode_id) AS avg_cost_unsplit,
NULL AS avg_cost_split,
STD(mel.cost) AS standard_deviation_unsplit,
NULL AS standard_deviation_split,
STD(mel.cost)/(SUM(mel.cost)/COUNT(mel.master_episode_id)) AS cv_unsplit,
NULL AS cv_split,
NULL AS percent_costs_unsplit,
NULL AS percent_costs_split,
SUM(mel.cost_t) AS typical_costs_unsplit,
NULL AS typical_costs_split,
SUM(mel.cost_tc) AS typ_w_comp_costs_unsplit,
NULL AS typ_w_comp_costs_split,
SUM(mel.cost_c) AS pac_costs_unsplit,
NULL AS pac_costs_split,
SUM(mel.cost_c)/SUM(mel.cost) AS complication_rate_unsplit,
NULL AS complication_rate_split
FROM episode e
JOIN master_epid_level mel ON e.master_episode_id=mel.master_episode_id
AND mel.claim_type='CL'
AND mel.level=1
AND mel.split=0
AND mel.annualized=0
GROUP BY e.episode_id;


/*==============================*/
/* split aggregates level 1     */
/*==============================*/
/*INSERT INTO episode_aggregates*/
CREATE TEMPORARY TABLE tmp_ea AS
SELECT
e.episode_id,
COUNT(mel.master_episode_id) AS episode_count,
SUM(cost) AS total_cost_split,
SUM(mel.cost)/COUNT(mel.master_episode_id) AS avg_cost_split,
STD(mel.cost) AS standard_deviation_split,
STD(mel.cost)/(SUM(mel.cost)/COUNT(mel.master_episode_id)) AS cv_split,
SUM(mel.cost_t) AS typical_costs_split,
SUM(mel.cost_tc) AS typ_w_comp_costs_split,
SUM(mel.cost_c) AS pac_costs_split,
SUM(mel.cost_c)/SUM(mel.cost) AS complication_rate_split
FROM episode e
JOIN master_epid_level mel ON e.master_episode_id=mel.master_episode_id
AND mel.claim_type='CL'
AND mel.level=1
AND mel.split=1
AND mel.annualized=0
GROUP BY e.episode_id;

UPDATE episode_aggregates ea
JOIN tmp_ea ta ON ea.episode_id=ta.episode_id
SET
ea.total_cost_split = ta.total_cost_split,
ea.avg_cost_split = ta.avg_cost_split,
ea.standard_deviation_split = ta.standard_deviation_split,
ea.cv_split = ta.cv_split,
ea.typical_costs_split = ta.typical_costs_split,
ea.typ_w_comp_costs_split = ta.typ_w_comp_costs_split,
ea.pac_costs_split = ta.pac_costs_split,
ea.complication_rate_split = ta.complication_rate_split
WHERE ea.level=1;

