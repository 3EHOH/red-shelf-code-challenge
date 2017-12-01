#grab all PAC claims and add dates and allowed_amt
drop table if exists PAC_claims;


create table PAC_claims as
select assignment.`member_id`,
assignment.`master_episode_id`,
assignment.`master_claim_id`,
assignment.`assigned_count`,
assignment.`assigned_type`,
assignment.claim_source,
assignment.rule,
claims_combined.`id`,
claims_combined.begin_date,
claims_combined.end_date,
claims_combined.allowed_amt
from assignment
join claims_combined
on assignment.master_claim_id=claims_combined.master_claim_id
where assignment.assigned_type<>"T";


create index PACCLAM on PAC_claims(master_episode_id);
create index FILT on filtered_episodes(master_episode_id);
create index EPI on episode(master_episode_id);

#add episode info to PAC claims

drop table if exists PAC_claims_epi;

create table PAC_claims_epi
as
select
episode.member_id,
episode.master_episode_id,
PAC_claims.master_claim_id,
PAC_claims.assigned_count,
PAC_claims.assigned_type,
PAC_claims.claim_source,
PAC_claims.rule,
PAC_claims.id,
PAC_claims.begin_date,
PAC_claims.end_date,
PAC_claims.allowed_amt,
episode.episode_begin_date,
episode.episode_end_date,
episode.trig_begin_date,
episode.trig_end_date,
episode.attrib_default_physician,
episode.attrib_default_facility,
datediff(episode.episode_end_date, PAC_claims.end_date) as Day_from_epi_end
from PAC_claims
left join episode
on PAC_claims.master_episode_id=episode.master_episode_id
;

drop table PAC_claims;

drop table if exists PAC_claims_epi_codes;

create table PAC_claims_epi_codes
as
select PAC_claims_epi.*,
code.code_value,
code.`nomen`
from PAC_claims_epi
left join
code
on PAC_claims_epi.id=code.u_c_id;

drop table if exists PAC_claims_epi_codes_def;

create table PAC_claims_epi_codes_def as
select
left(PAC_claims_epi_codes.master_episode_id,6) as EPISODE_ID,
PAC_claims_epi_codes.member_id ,
PAC_claims_epi_codes.master_episode_id ,
PAC_claims_epi_codes.master_claim_id ,
PAC_claims_epi_codes.assigned_count ,
PAC_claims_epi_codes.assigned_type ,
PAC_claims_epi_codes.claim_source ,
PAC_claims_epi_codes.rule ,
PAC_claims_epi_codes.id ,
PAC_claims_epi_codes.begin_date ,
PAC_claims_epi_codes.end_date ,
PAC_claims_epi_codes.allowed_amt ,
PAC_claims_epi_codes.episode_begin_date ,
PAC_claims_epi_codes.episode_end_date ,
PAC_claims_epi_codes.trig_begin_date ,
PAC_claims_epi_codes.trig_end_date ,
PAC_claims_epi_codes.attrib_default_physician ,
PAC_claims_epi_codes.attrib_default_facility ,
PAC_claims_epi_codes.Day_from_epi_end ,
 PAC_claims_epi_codes.code_value ,
PAC_claims_epi_codes.nomen ,
COMPLICATION

from PAC_claims_epi_codes
left join `episode_builder-5.4.005`.episode_to_code
on left(PAC_claims_epi_codes.master_episode_id,6) = `episode_builder-5.4.005`.episode_to_code.episode_id
and
 PAC_claims_epi_codes.code_value = `episode_builder-5.4.005`.episode_to_code.code_value
and
 PAC_claims_epi_codes.nomen = `episode_builder-5.4.005`.episode_to_code.code_type_id
 where PAC_claims_epi_codes.nomen in ("DX",'DXX') and complication in (1,2)
 group by PAC_claims_epi_codes.master_episode_id, PAC_claims_epi_codes.master_claim_id, PAC_claims_epi_codes.code_value, PAC_claims_epi_codes.allowed_amt
 ;

drop table if exists PAC_claims_epi_codes_def2;

create table PAC_claims_epi_codes_def2 as
select PAC_claims_epi_codes_def.*,
`episode_builder-5.4.005`.code.Group_id,
`episode_builder-5.4.005`.code.Description
from PAC_claims_epi_codes_def
left join `episode_builder-5.4.005`.code
on
 PAC_claims_epi_codes_def.code_value = `episode_builder-5.4.005`.code.value
and
 PAC_claims_epi_codes_def.nomen = `episode_builder-5.4.005`.code.type_id
;


 drop table if exists PAC_claims_codes_grouped;

create table PAC_claims_codes_grouped as
select PAC_claims_epi_codes_def2.*,
`episode_builder-5.4.005`.group.Name as group_name
from PAC_claims_epi_codes_def2
left join `episode_builder-5.4.005`.group
on
 PAC_claims_epi_codes_def2.group_id = `episode_builder-5.4.005`.group.id
and
 PAC_claims_epi_codes_def2.nomen = `episode_builder-5.4.005`.group.type_id;

create index x on PAC_claims_codes_grouped(master_episode_id);

 create table PAC_DATA as
 select
left(episode.master_episode_id,6) as episode_id,
 episode.member_id,
episode.master_episode_id,
episode.episode_begin_date,
episode.episode_end_date,
episode.trig_begin_date,
episode.trig_end_date,
episode.attrib_default_physician,
episode.attrib_default_facility,
PAC_claims_codes_grouped.master_claim_id,
PAC_claims_codes_grouped.assigned_count,
PAC_claims_codes_grouped.assigned_type,
PAC_claims_codes_grouped.claim_source,
PAC_claims_codes_grouped.rule,
PAC_claims_codes_grouped.id,
PAC_claims_codes_grouped.begin_date,
PAC_claims_codes_grouped.end_date,
PAC_claims_codes_grouped.allowed_amt,
PAC_claims_codes_grouped.Day_from_epi_end,
PAC_claims_codes_grouped.code_value,
PAC_claims_codes_grouped.nomen,
PAC_claims_codes_grouped.COMPLICATION,
PAC_claims_codes_grouped.group_id,
PAC_claims_codes_grouped.description,
PAC_claims_codes_grouped.group_name
from PAC_claims_codes_grouped
right join episode
on PAC_claims_codes_grouped.master_episode_id=episode.master_episode_id;

drop tables if exists PAC_claims_epi, PAC_claims_epi_codes, PAC_claims_epi_codes_def, PAC_claims_epi_codes_def2, PAC_claims, PAC_claims_epi_codes_defa, PAC_claims_codes_grouped;


