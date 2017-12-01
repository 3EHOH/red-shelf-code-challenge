set search_path=epbuilder;

select 'Step: SQL-PostEC-AutoSQLSetup.sql' as curstep, current_timestamp as datetime;
\set AUTOCOMMIT on

/* grab temp data */

DROP TABLE IF EXISTS build_episode_reference;
CREATE TABLE build_episode_reference as select * from episode_builder_5_4_003.episode;

DROP TABLE IF EXISTS filter_params;
CREATE TABLE filter_params LIKE ECR_Template.filter_params;
INSERT /*+ direct */ INTO filter_params
	SELECT * FROM ECR_Template.filter_params;

DROP TABLE IF EXISTS filters;
CREATE TABLE filters LIKE ECR_Template.filters;
INSERT /*+ direct */ INTO filters
	SELECT * FROM ECR_Template.filters;

DROP TABLE IF EXISTS mdc_desc;
CREATE TABLE mdc_desc LIKE ECR_Template.mdc_desc;
INSERT /*+ direct */ INTO mdc_desc
	SELECT * FROM ECR_Template.mdc_desc;


/* grab assignments */


update /*+direct*/  claim_line
SET
assigned_count = a.assigned_count,
assigned = 1
FROM assignment a
WHERE claim_line.master_claim_id = a.master_claim_id;


update /*+direct*/  claim_line_rx
SET
assigned_count = a.assigned_count,
assigned = 1
FROM assignment a
WHERE claim_line_rx.master_claim_id = a.master_claim_id;



update /*+direct*/  claims_combined
SET
assigned_count = a.assigned_count
FROM assignment a
WHERE claims_combined.master_claim_id = a.master_claim_id;

update /*+direct*/  claim_line_rx SET assigned_count=0 WHERE assigned_count IS NULL;
update /*+direct*/  claim_line_rx SET assigned=0 WHERE assigned IS NULL;



/* unassigned grouper */
INSERT /*+ direct */ INTO unassigned_episodes
(member_id, episode_id, episode_begin_date, cost)
SELECT
cc.member_id,
CONCAT('UC',ec.CCS_CATEGORY) AS 'episode_id',
min(cc.begin_date) AS 'episode_begin_date',
sum(allowed_amt) AS 'cost'
FROM claims_combined cc
JOIN code c ON c.u_c_id=cc.id
AND c.nomen='DX'
AND c.principal=1
JOIN episode_builder_5_4_003.code ec ON ec.VALUE=c.code_value
AND ec.TYPE_ID='DX'
WHERE cc.master_claim_id NOT IN
	(
	SELECT master_claim_id FROM assignment
	)
GROUP BY cc.member_id, ec.CCS_CATEGORY;
