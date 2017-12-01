create table xg_marshfield.tmp_subt_claims AS 
SELECT 
ceo.master_claim_id, 
date_format(str_to_date(e.episode_begin_date, '%m/%d/%Y'), '%Y-%m-%d') as epi, 
date_format(str_to_date(e.trig_begin_date, '%m/%d/%Y'), '%Y-%m-%d') as trig, 
g.begin_date,
e.master_episode_id
FROM xg_marshfield.claim_ec_output ceo
JOIN xg_marshfield.episode e ON e.master_episode_id = ceo.ass_master_episode_id
JOIN ecr.xg_v2claimservline2015_01_14_0304 g ON g.master_claim_id = ceo.master_claim_id
AND g.begin_date >= date_format(str_to_date(e.episode_begin_date, '%m/%d/%Y'), '%Y-%m-%d')
AND g.begin_date <= date_format(str_to_date(e.trig_begin_date, '%m/%d/%Y'), '%Y-%m-%d');

##get DX joined Subtypes...
insert into xg_marshfield.episode_sub_types select
tsc.master_episode_id,
tsc.master_claim_id,
g.value,
sc.subtype_group_id,
sc.group_name
FROM xg_marshfield.tmp_subt_claims tsc
JOIN xg_marshfield.code g ON tsc.master_claim_id = g.master_claim_id
JOIN xg_marshfield.sub_type_codes sc ON g.value = sc.CODE_VALUE
AND sc.TYPE_ID='DX'
AND g.nomen='DX';

##get PX/CPT/HCPC subtypes */
insert into xg_marshfield.episode_sub_types select
tsc.master_episode_id,
tsc.master_claim_id,
g.value,
sc.subtype_group_id,
sc.group_name
FROM tmp_subt_claims tsc
JOIN xg_marshfield.code g ON tsc.master_claim_id = g.master_claim_id
JOIN xg_marshfield.sub_type_codes sc ON g.value = sc.CODE_VALUE
AND sc.TYPE_ID IN ('PX','CPT','HCPC')
AND g.nomen IN ('PX','CPT','HCPC');