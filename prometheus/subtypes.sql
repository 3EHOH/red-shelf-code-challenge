insert into xg_marshfield.ra_subtypes select
	distinct master_episode_id as epi_id,
	episode_id as epi_number,
	subtype_group_id as name,
	"1" as value
from episode_sub_types
order by master_episode_id,subtype_group_id;