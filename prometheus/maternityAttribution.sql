set search_path=epbuilder;

select 'Step: maternityAttribution.sql' as curstep, current_timestamp as datetime;
\set AUTOCOMMIT on


/*

Create list of providers who are linked to maternity bundles
Need to know who is a delivery and who is pregnancy
See KPMG over (partition queries for a sweet way of doing this


*/


-- start by getting all maternity bundles (defined by the delivery):
DROP TABLE IF EXISTS maternity_provider_attr;

CREATE TABLE maternity_provider_attr (
master_episode_id  varchar(73) DEFAULT NULL,
provider_id  varchar(24) DEFAULT NULL,
source_type  varchar(20) DEFAULT NULL
);

--insert all delivery episodes to define the bundle (EX1401 PREGN (it is parent))...
INSERT INTO maternity_provider_attr ( master_episode_id )
SELECT master_episode_id FROM episode
WHERE episode_id IN ('EX1401');




/* 
initial code does NOT limit to ob/gyn providers! 
Need to link into the NPI tables once we have them
*/


/* 
find all pregnancy episodes to try and do attribution from first

The following gets all pregnancy episodes and ALL providers and orders them, per episode
by number of claims they are on and then highest number of dollars. 
*/
/*
SELECT
e.master_episode_id,
cl.provider_id,
count(a.master_claim_id) AS cnt, 
SUM(cl.allowed_amt) AS amt
FROM episode e 
JOIN assignment a ON e.master_episode_id=a.master_episode_id
JOIN claim_line cl ON cl.master_claim_id=a.master_claim_id
WHERE e.episode_id='EX1401'
GROUP BY e.master_episode_id, cl.provider_id
ORDER BY e.master_episode_id ASC, count(a.master_claim_id) DESC, SUM(cl.allowed_amt) DESC
*/

/*
This gets the provider with the highest count of claims with cost as a tie breaker per episode
of type EX1401 (PREGN). The OVER (PARTITION BY... ranks the providers by count and cost
and assigns a rank so select 1 to get top, or if you wanted top two you could rank < 3...
*/
DROP TABLE IF EXISTS maternity_provider_attr_pr;

CREATE TABLE maternity_provider_attr_pr AS
SELECT 
prov.master_episode_id, 
prov.provider_id FROM (
	SELECT
	e.master_episode_id,
	cl.provider_id,
	count(a.master_claim_id) AS cnt, 
	SUM(cl.allowed_amt) AS amt,
	row_number()
	OVER (PARTITION BY e.master_episode_id ORDER BY count(a.master_claim_id) DESC, SUM(cl.allowed_amt) DESC) as rank
	FROM episode e 
	JOIN assignment a ON e.master_episode_id=a.master_episode_id
	JOIN claim_line cl ON cl.master_claim_id=a.master_claim_id
	JOIN code c ON c.u_c_id=cl.id
	JOIN episode_builder_5_4_003.global_em_201603 gem ON c.code_value=gem.VALUE
	AND c.nomen in ('CPT','HCPC')
	WHERE e.episode_id='EX1401'
	GROUP BY e.master_episode_id, cl.provider_id
) as prov
WHERE prov.rank=1;

--Add the providers found to the mpa table by associating to the delivery...
UPDATE maternity_provider_attr mpa
SET provider_id = pr.provider_id, source_type='Prenatal'
FROM maternity_provider_attr_pr pr
WHERE pr.master_episode_id=mpa.master_episode_id;



/*

This one won't be as easy because we need to find the child delivery of the pregn episodes
to get the right ones to attribute based on...

*/

/*
--getting the delivery from the pregnancy for episodes that did not get attributed
--from the pregnancy attribution process... (test code)
SELECT
*
FROM maternity_provider_attr mpa 
JOIN association a on mpa.master_episode_id = a.parent_master_episode_id
AND mpa.provider_id IS NULL
JOIN assignment ass ON ass.master_episode_id=a.child_master_episode_id
JOIN claim_line cl on cl.master_claim_id=ass.master_claim_id
*/

/*
Get the provider to attribute based on the delivery. The deliveries will be the children of
the pregnancies. VAGDEL: EP1403 and CSECT: EP1404 
The query only gets children of episodes that do not already have a provider attributed since
this is the second attribution 
*/
DROP TABLE IF EXISTS maternity_provider_attr_del;

CREATE TABLE maternity_provider_attr_del AS
SELECT 
prov.master_episode_id, 
prov.provider_id FROM (
	SELECT
	a.parent_master_episode_id AS master_episode_id,
	cl.provider_id,
	count(ass.master_claim_id) AS cnt, 
	SUM(cl.allowed_amt) AS amt,
	row_number()
	OVER (PARTITION BY a.parent_master_episode_id ORDER BY count(ass.master_claim_id) DESC, SUM(cl.allowed_amt) DESC) as rank
	FROM maternity_provider_attr mpa 
	JOIN association a ON mpa.master_episode_id = a.parent_master_episode_id
	AND mpa.provider_id IS NULL
	JOIN assignment ass ON ass.master_episode_id=a.child_master_episode_id
	JOIN claim_line cl ON cl.master_claim_id=ass.master_claim_id
	JOIN episode e ON e.master_episode_id=ass.master_episode_id
	AND e.episode_id IN ('EP1403','EP1404')
	GROUP BY a.parent_master_episode_id, cl.provider_id
) as prov
WHERE prov.rank=1;

/*SELECT
ass.master_episode_id,
cl.provider_id,
count(ass.master_claim_id) AS cnt, 
SUM(cl.allowed_amt) AS amt,
row_number()
OVER (PARTITION BY ass.master_episode_id ORDER BY count(ass.master_claim_id) DESC, SUM(cl.allowed_amt) DESC) as rank
FROM maternity_provider_attr mpa 
JOIN association a ON mpa.master_episode_id = a.parent_master_episode_id
AND mpa.provider_id IS NULL
JOIN assignment ass ON ass.master_episode_id=a.child_master_episode_id
JOIN claim_line cl ON cl.master_claim_id=ass.master_claim_id
JOIN episode e ON e.master_episode_id=ass.master_episode_id
AND e.episode_id IN ('EP1403','EP1404')
GROUP BY ass.master_episode_id, cl.provider_id*/


--Add the providers found to the del table by associating to the delivery...
UPDATE maternity_provider_attr mpa
SET provider_id = del.provider_id, source_type='delivery'
FROM maternity_provider_attr_del del
WHERE del.master_episode_id=mpa.master_episode_id;



