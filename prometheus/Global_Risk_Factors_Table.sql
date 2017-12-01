create table xg_marshfield.global_risk_factors as
select 
ecr.xg_v2claimservline2015_01_14_0304.member_id,
xg_marshfield.code.master_claim_id,
date_format(ecr.xg_v2claimservline2015_01_14_0304.begin_date,'%m/%d/%Y') as claim_date, 
xg_marshfield.code.value,
xg_marshfield.risk_factors.factor_id
from xg_marshfield.code
join xg_marshfield.risk_factors on xg_marshfield.code.value = xg_marshfield.risk_factors.dx
join ecr.xg_v2claimservline2015_01_14_0304 on xg_marshfield.code.master_claim_id=ecr.xg_v2claimservline2015_01_14_0304.master_claim_id 
where xg_marshfield.code.nomen='DX'
;

create index master_claim_id on xg_marshfield.global_risk_factors(master_claim_id);

create index claim_date on xg_marshfield.global_risk_factors(claim_date);

create index member_id on xg_marshfield.global_risk_factors(member_id);
