create table xg_marshfield.tmp_subt_claims AS 
SELECT 
ceo.master_claim_id, 
date_format(str_to_date(e.episode_begin_date, '%m/%d/%Y'), '%Y-%m-%d') as epi, 
date_format(str_to_date(e.trig_begin_date, '%m/%d/%Y'), '%Y-%m-%d') as trig, 
g.begin_date,
e.master_episode_id
FROM xg_marshfield.claim_ec_output ceo
JOIN xg_marshfield.episode e ON e.master_episode_id = ceo.ass_master_episode_id
JOIN ecr.xg_v2claimservline2015_01_14_0304 g ON CONCAT(g.claim_id, '_', g.claim_line_id) = ceo.master_claim_id
AND g.begin_date >= date_format(str_to_date(e.episode_begin_date, '%m/%d/%Y'), '%Y-%m-%d')
AND g.begin_date <= date_format(str_to_date(e.trig_begin_date, '%m/%d/%Y'), '%Y-%m-%d');

create index master_claim_id on xg_marshfield.tmp_subt_claims(master_claim_id);
create index codevalue on xg_marshfield.sub_type_codes(CODE_VALUE);