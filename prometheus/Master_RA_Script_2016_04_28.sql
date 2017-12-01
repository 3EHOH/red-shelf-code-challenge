########SQL Script to Create the Input Files for RA/SA#################

######The script includes the following Steps

#1. Creating shells for the final tables and several intermediate tables

#2. Populating the 'ra_episode_data" table

#3. Populating the "ra_riskfactors' table

#4. Populating the 'ra_subtypes' table

#5. Populating the 'ra_cost_use' table

#6. Remove filtered episodes




#################################################

#	#

#	#

#	Step 1: Creating table shells	#

#	#

#	#

#################################################

#RA/SA Input Tables



drop table if exists ra_episode_data;

create table ra_episode_data

(epi_id varchar(73),

member_id varchar(50),

epi_number varchar(6),

epi_name varchar(6),

female int(1),

age int(2),

rec_enr int(1),

eol_ind int(1),

                index(member_id)

);



drop table if exists ra_riskfactors;

create table ra_riskfactors

(epi_id varchar(75),

        epi_number varchar(6),

name varchar(25),

value int(1)

);



drop table if exists ra_subtypes;

create table ra_subtypes

(epi_id varchar(75),

        epi_number varchar(6),

name varchar(25),

    value int(1)

    );





drop table if exists ra_cost_use;

create table ra_cost_use

(

    epi_id varchar(75),

    epi_number varchar(6),

name varchar(25),

    value int(1)

    );



##Intermediate tables to flag subtypes



drop table if exists tmp_subt_claims;

create table tmp_subt_claims

(master_claim_id varchar(100),

id int(11),

epi varchar(10),

trig varchar(10),

begin_date datetime,

master_episode_id varchar(73),

    episode_id varchar(10),

    index master_episode_id (master_episode_id),

index master_claim_id (master_claim_id),
index id (id)

);



drop table if exists episode_sub_types;

create table episode_sub_types

(master_episode_id varchar(73),

master_claim_id varchar(100),

code_value varchar(12),

subtype_group_id varchar(25),

group_name varchar(255),

episode_id varchar(10),

index code_value (code_value),

        index(master_episode_id)

);



##Temp Table to flag risk factors



drop table if exists global_risk_factors;

create table global_risk_factors

(member_id varchar(100),

master_claim_id varchar(62),

claim_date date,

code_value char(12),

factor_id varchar(25),

index member_id (member_id),

index master_claim_id (master_claim_id),

index claim_date (claim_date)

);







#Table of IP trigger claims



drop table if exists tmp_trig_claims;

create table tmp_trig_claims

(master_episode_id varchar(73),

episode_id varchar(10),

episode_type varchar(1),

assigned_type varchar(2),

claim_line_type_code varchar(12),

allowed_amt decimal(15,4),

split_allowed_amt decimal(15,4),

index master_episode_id (master_episode_id),

    index episode_type (episode_type),

    index episode_id (episode_id)

);









#################################################

#	#

#	#

#	Step 2: Populating ra_episode_data	#

#	#

#	#

#################################################



insert into ra_episode_data select

episode.master_episode_id as epi_id,

episode.member_id as member_id,

episode.episode_id as epi_number,

build_episode_reference.NAME as epi_name,

if(member.gender_code = 'F', 1, 0) as female,

DATE_FORMAT(episode.episode_begin_date, '%Y-%m-%d') - DATE_FORMAT(STR_TO_DATE(member.birth_year, '%Y-%m-%d'), '%Y')  as age,

if(date_sub(episode.episode_begin_date, interval 180 day) > enrollment.begin_date, 0, 1) as rec_enr,

if(DATE_FORMAT(member.date_of_death, '%Y-%m-%d') between DATE_FORMAT(episode.episode_begin_date, '%Y-%m-%d') and DATE_FORMAT(episode.episode_end_date, '%Y-%m-%d'), 1, 0) as eol_ind



from

episode



left join build_episode_reference on episode.episode_id = build_episode_reference.EPISODE_ID

left join member on episode.member_id = member.member_id

left join enrollment on episode.member_id = enrollment.member_id



group by episode.master_episode_id;







#################################################

#	#

#	#

#	Step 3: Populating ra_riskfactors	#

#	#

#	#

#################################################

##Insert all possible flagged risk factors into global risk factors table

insert into global_risk_factors select
	cl.member_id,
	c.master_claim_id,
	cl.begin_date as claim_date,
	c.code_value,
	r.factor_id
from code c
join risk_factors r on c.code_value = r.dx
join claims_combined cl on c.u_c_id = cl.id
where c.nomen='DX'
;



#######Put final risk factors in RA input file

###Non-Condition Episodes

insert into ra_riskfactors select
	e.master_episode_id as epi_id,
	e.episode_id as epi_number,
	g.factor_id as name,
	"1" as value
from episode e
join global_risk_factors g on e.member_id = g.member_id
where date_format(g.claim_date, '%m/%d/%Y') < date_format(e.episode_begin_date, '%m/%d/%Y')
and e.episode_type in ("A","P","X")
group by e.master_episode_id, g.factor_id
;





#Condition Episodes

insert into ra_riskfactors select
	e.master_episode_id as epi_id,
	e.episode_id as epi_number,
	g.factor_id as name,
	"1" as value
from episode e
join global_risk_factors g on e.member_id = g.member_id
where date_format(g.claim_date, '%m/%d/%Y') < date_format(date_sub(e.episode_end_date,interval 1 year), '%m/%d/%Y')
and e.episode_type = "C"
group by e.master_episode_id, g.factor_id
;



#################################################

#	#

#	#

#	Step 4: Populating ra_subtypes	#

#	#

#	#

#################################################

#Temp table to flag relevant claims for subtypes

#Acute, Procedure, and Other Episodes

insert into tmp_subt_claims
	SELECT
	ceo.master_claim_id,
	g.id,
	date_format(e.episode_begin_date, '%Y-%m-%d') as epi,
	date_format(e.trig_begin_date, '%Y-%m-%d') as trig,
	g.begin_date,
	e.master_episode_id,
	e.episode_id
FROM assignment ceo
JOIN episode e ON e.master_episode_id = ceo.master_episode_id
AND e.episode_type in ('A','P','X')
JOIN claims_combined g ON g.master_claim_id = ceo.master_claim_id
AND g.master_claim_id=e.master_claim_id
AND date_format(g.begin_date, '%Y-%m-%d') >= date_format(e.episode_begin_date, '%Y-%m-%d')
AND date_format(g.begin_date, '%Y-%m-%d') <= date_format(e.trig_begin_date, '%Y-%m-%d');


#Chronic

insert into tmp_subt_claims
	SELECT
	ceo.master_claim_id,
	g.id,
	date_format(e.episode_begin_date, '%Y-%m-%d') as epi,
	date_format(e.trig_begin_date, '%Y-%m-%d') as trig,
	g.begin_date,
	e.master_episode_id,
	e.episode_id
FROM assignment ceo
JOIN episode e ON e.master_episode_id = ceo.master_episode_id
AND e.episode_type = 'C'
JOIN claims_combined g ON g.master_claim_id = ceo.master_claim_id
AND g.master_claim_id=e.master_claim_id
AND date_format(g.begin_date, '%Y-%m-%d') <= date_format(date_sub(e.episode_end_date,interval 1 year), '%Y-%m-%d');


###Insert flagged subtypes into episode_sub_types table

##get DX joined Subttmp_subt_claimsypes...

insert into episode_sub_types select
	tsc.master_episode_id,
	tsc.master_claim_id,
	g.code_value,
	sc.subtype_group_id,
	sc.group_name,
	sc.episode_id
FROM tmp_subt_claims tsc
JOIN code g ON tsc.id = g.u_c_id
JOIN sub_type_codes sc ON g.code_value = sc.CODE_VALUE
AND tsc.episode_id=sc.episode_id
AND sc.TYPE_ID='DX'
AND g.nomen='DX';


##get PX/CPT/HCPC subtypes 

insert into episode_sub_types select
	tsc.master_episode_id,
	tsc.master_claim_id,
	g.code_value,
	sc.subtype_group_id,
	sc.group_name,
	sc.episode_id
FROM tmp_subt_claims tsc
JOIN code g ON tsc.id = g.u_c_id
JOIN sub_type_codes sc ON g.code_value = sc.CODE_VALUE
AND tsc.episode_id=sc.episode_id
AND sc.TYPE_ID IN ('PX','CPT','HCPC')
AND g.nomen IN ('PX','CPT','HCPC');



####Insert relevant subtypes into RA input file

insert into ra_subtypes select
	distinct master_episode_id as epi_id,
	episode_id as epi_number,
	subtype_group_id as name,
	"1" as value
from episode_sub_types e
order by master_episode_id,subtype_group_id;






#################################################
#												#
#												#
#		Step 5: Populating ra_cost_use			#
#												#
#												#
#################################################
###Script to calculate use and costs for SA/RA Inputs for All Episode Types####

###Flag Typical Claim Costs####
#Table for IP Trigger Stays
insert into tmp_trig_claims select
	 e.master_episode_id,
	 e.episode_id,
	 e.episode_type,
	case when sum(case when  sig.assigned_type = "TC" then 1 else 0 end) >=1 then "TC" else "T" end  as assigned_type,
	 cc.claim_line_type_code,
	sum(cc.allowed_amt) as allowed_amt,
	 sum(if(sig.assigned_count>0,cc.allowed_amt/sig.assigned_count,cc.allowed_amt) ) as split_allowed_amt
	 FROM episode e
JOIN assignment sig
ON e.master_episode_id = sig.master_episode_id
#AND e.master_claim_id = sig.master_claim_id
AND e.episode_type in ('A','P')
JOIN claims_combined cc on sig.master_claim_id=cc.master_claim_id
where left(sig.rule,3) in ('1.1','1.2','1.3','2.3') and cc.claim_line_type_code in ('IP','OP') and cc.begin_date BETWEEN e.trig_begin_date and e.trig_end_date 
group by e.master_episode_id;

####Procedure and Acute Episodes####
##Create typ_ip_ind##
insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"typ_ip_ind" as name,
	ifnull(if(t.assigned_type='T',1,0),0) as value
FROM master_epid_level me
left join tmp_trig_claims t on me.master_episode_id=t.master_episode_id
 and t.claim_line_type_code='IP'
JOIN episode e on me.master_episode_id=e.master_episode_id 
Where me.claim_type='CL'
AND me.level=1
AND me.split=0
AND me.annualized=0
AND me.filter_id=0
AND e.episode_type IN ('A','P');


###Use Variables###
##Typical IP Use
insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"use_typ_ip_l1" as name,
	ifnull(if(t.allowed_amt>0,1,0),0) as value
FROM master_epid_level me
left join tmp_trig_claims t on me.master_episode_id=t.master_episode_id
 and t.claim_line_type_code='IP'
JOIN episode e on me.master_episode_id=e.master_episode_id
Where me.claim_type='CL'
AND me.level=1
AND me.split=0
AND me.annualized=0
AND me.filter_id=0
AND e.episode_type IN ('A','P');

insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"use_typ_ip_l3" as name,
	ifnull(if(t.allowed_amt>0,1,0),0) as value
FROM master_epid_level me
left join tmp_trig_claims t on me.master_episode_id=t.master_episode_id
 and t.claim_line_type_code='IP'
JOIN episode e on me.master_episode_id=e.master_episode_id 
Where me.claim_type='CL'
AND me.level=3
AND me.split=0
AND me.annualized=0
AND me.filter_id=0
AND e.episode_type IN ('P');


insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"use_typ_ip_l4" as name,
	ifnull(if(t.allowed_amt>0,1,0),0) as value
FROM master_epid_level me
left join tmp_trig_claims t on me.master_episode_id=t.master_episode_id
 and t.claim_line_type_code='IP'
JOIN episode e on me.master_episode_id=e.master_episode_id 
Where me.claim_type='CL'
AND me.level=4
AND me.split=0
AND me.annualized=0
AND me.filter_id=0
AND e.episode_type IN ('A');


##Typ Other Use
insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"use_typ_other_l1" as name,
	if((me.cost_t + me.cost_tc) - ifnull(t.allowed_amt,0)>0,1,0) as value
FROM master_epid_level me
left join tmp_trig_claims t on me.master_episode_id=t.master_episode_id
and t.claim_line_type_code='IP'
JOIN episode e on me.master_episode_id=e.master_episode_id 
Where me.claim_type='CL'
AND me.level=1
AND me.split=0
AND me.annualized=0
AND me.filter_id=0
AND e.episode_type IN ('A','P');

insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"use_typ_other_l3" as name,
	if((me.cost_t + me.cost_tc) - ifnull(t.allowed_amt,0)>0,1,0) as value
FROM master_epid_level me
left join tmp_trig_claims t on me.master_episode_id=t.master_episode_id
and t.claim_line_type_code='IP'
JOIN episode e on me.master_episode_id=e.master_episode_id 
Where me.claim_type='CL'
AND me.level=3
AND me.split=0
AND me.annualized=0
AND me.filter_id=0
AND e.episode_type IN ('P');

insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"use_typ_other_l4" as name,
	if((me.cost_t + me.cost_tc) - ifnull(t.allowed_amt,0)>0,1,0) as value
FROM master_epid_level me
left join tmp_trig_claims t on me.master_episode_id=t.master_episode_id
and t.claim_line_type_code='IP'
JOIN episode e on me.master_episode_id=e.master_episode_id 
Where me.claim_type='CL'
AND me.level=4
AND me.split=0
AND me.annualized=0
AND me.filter_id=0
AND e.episode_type IN ('A');

##Complication Other Use
insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"use_comp_other_l1" as name,
	if(me.cost_c>0,1,0) as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
AND e.episode_type IN ('A','P')
	AND me.claim_type='CL'
	and me.level=1 
	and me.annualized=0
	and me.split=0
	AND me.filter_id=0;


insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"use_comp_other_l3" as name,
	if(me.cost_c>0,1,0) as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
AND e.episode_type IN ('P')
	AND me.claim_type='CL'
	and me.level=3 
	and me.annualized=0
	and me.split=0
	AND me.filter_id=0;

insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"use_comp_other_l4" as name,
	if(me.cost_c>0,1,0) as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
AND e.episode_type IN ('A')
	AND me.claim_type='CL'
	and me.level=4
	and me.annualized=0
	and me.split=0
	AND me.filter_id=0;


###Cost Variables###
##Typical IP Costs
#RA Costs
insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_ra_typ_ip_l1" as name,
	ifnull(t.allowed_amt,0) as value
FROM master_epid_level me
left join tmp_trig_claims t on me.master_episode_id=t.master_episode_id
 and t.claim_line_type_code='IP'
JOIN episode e on me.master_episode_id=e.master_episode_id 
Where me.claim_type='CL'
AND me.level=1
AND me.split=0
AND me.annualized=0
AND me.filter_id=0
AND e.episode_type IN ('A','P');

insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_ra_typ_ip_l3" as name,
	ifnull(t.allowed_amt,0) as value
FROM master_epid_level me
left join tmp_trig_claims t on me.master_episode_id=t.master_episode_id
 and t.claim_line_type_code='IP'
JOIN episode e on me.master_episode_id=e.master_episode_id 
Where me.claim_type='CL'
AND me.level=3
AND me.split=0
AND me.annualized=0
AND me.filter_id=0
AND e.episode_type IN ('P');


insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_ra_typ_ip_l4" as name,
	ifnull(t.allowed_amt,0) as value
FROM master_epid_level me
left join tmp_trig_claims t on me.master_episode_id=t.master_episode_id
 and t.claim_line_type_code='IP'
JOIN episode e on me.master_episode_id=e.master_episode_id 
Where me.claim_type='CL'
AND me.level=4
AND me.split=0
AND me.annualized=0
AND me.filter_id=0
AND e.episode_type IN ('A');


#SA Costs
insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_sa_typ_ip_l1" as name,
	ifnull(t.split_allowed_amt,0) as value
FROM master_epid_level me
left join tmp_trig_claims t on me.master_episode_id=t.master_episode_id
 and t.claim_line_type_code='IP'
JOIN episode e on me.master_episode_id=e.master_episode_id 
Where me.claim_type='CL'
AND me.level=1
AND me.split=1
AND me.annualized=0
AND me.filter_id=0
AND e.episode_type IN ('A','P');

insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_sa_typ_ip_l3" as name,
	ifnull(t.split_allowed_amt,0) as value
FROM master_epid_level me
left join tmp_trig_claims t on me.master_episode_id=t.master_episode_id
 and t.claim_line_type_code='IP'
JOIN episode e on me.master_episode_id=e.master_episode_id 
Where me.claim_type='CL'
AND me.level=3
AND me.split=1
AND me.annualized=0
AND me.filter_id=0
AND e.episode_type IN ('P');


insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_sa_typ_ip_l4" as name,
	ifnull(t.split_allowed_amt,0) as value
FROM master_epid_level me
left join tmp_trig_claims t on me.master_episode_id=t.master_episode_id
 and t.claim_line_type_code='IP'
JOIN episode e on me.master_episode_id=e.master_episode_id 
Where me.claim_type='CL'
AND me.level=4
AND me.split=1
AND me.annualized=0
AND me.filter_id=0
AND e.episode_type IN ('A');



##Typical Other Costs
#RA Costs
insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_ra_typ_other_l1" as name,
	(me.cost_t + me.cost_tc) - ifnull(t.allowed_amt,0) as value
FROM master_epid_level me
left join tmp_trig_claims t on me.master_episode_id=t.master_episode_id
 and t.claim_line_type_code='IP'
JOIN episode e on me.master_episode_id=e.master_episode_id 
Where me.claim_type='CL'
AND me.level=1
AND me.split=0
AND me.annualized=0
AND me.filter_id=0
AND e.episode_type IN ('A','P');

insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_ra_typ_other_l3" as name,
	(me.cost_t + me.cost_tc) - ifnull(t.allowed_amt,0) as value
FROM master_epid_level me
left join tmp_trig_claims t on me.master_episode_id=t.master_episode_id
 and t.claim_line_type_code='IP'
JOIN episode e on me.master_episode_id=e.master_episode_id 
Where me.claim_type='CL'
AND me.level=3
AND me.split=0
AND me.annualized=0
AND me.filter_id=0
AND e.episode_type IN ('P');

insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_ra_typ_other_l4" as name,
	(me.cost_t + me.cost_tc) - ifnull(t.allowed_amt,0) as value
FROM master_epid_level me
left join tmp_trig_claims t on me.master_episode_id=t.master_episode_id
 and t.claim_line_type_code='IP'
JOIN episode e on me.master_episode_id=e.master_episode_id 
Where me.claim_type='CL'
AND me.level=4
AND me.split=0
AND me.annualized=0
AND me.filter_id=0
AND e.episode_type IN ('A');

#SA Costs
insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_sa_typ_other_l1" as name,
	(me.cost_t + me.cost_tc) - ifnull(t.split_allowed_amt,0) as value
FROM master_epid_level me
left join tmp_trig_claims t on me.master_episode_id=t.master_episode_id
 and t.claim_line_type_code='IP'
JOIN episode e on me.master_episode_id=e.master_episode_id 
Where me.claim_type='CL'
AND me.level=1
AND me.split=1
AND me.annualized=0
AND me.filter_id=0
AND e.episode_type IN ('A','P');

insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_sa_typ_other_l3" as name,
	(me.cost_t + me.cost_tc) - ifnull(t.split_allowed_amt,0) as value
FROM master_epid_level me
left join tmp_trig_claims t on me.master_episode_id=t.master_episode_id
 and t.claim_line_type_code='IP'
JOIN episode e on me.master_episode_id=e.master_episode_id 
Where me.claim_type='CL'
AND me.level=3
AND me.split=1
AND me.annualized=0
AND me.filter_id=0
AND e.episode_type IN ('P');

insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_sa_typ_other_l4" as name,
	(me.cost_t + me.cost_tc) - ifnull(t.split_allowed_amt,0) as value
FROM master_epid_level me
left join tmp_trig_claims t on me.master_episode_id=t.master_episode_id
 and t.claim_line_type_code='IP'
JOIN episode e on me.master_episode_id=e.master_episode_id 
Where me.claim_type='CL'
AND me.level=4
AND me.split=1
AND me.annualized=0
AND me.filter_id=0
AND e.episode_type IN ('A');


##Other Comp Costs
#RA Costs
insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_ra_comp_other_l1" as name,
	me.cost_c as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type IN ('A','P')
	AND me.claim_type='CL'
	and me.level=1
	and me.annualized=0
	and me.split=0
	AND me.filter_id=0;


insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_ra_comp_other_l3" as name,
	me.cost_c as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type IN ('P')
	AND me.claim_type='CL'
	and me.level=3
	and me.annualized=0
	and me.split=0
	AND me.filter_id=0;

insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_ra_comp_other_l4" as name,
	me.cost_c as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type IN ('A')
	AND me.claim_type='CL'
	and me.level=4
	and me.annualized=0
	and me.split=0
	AND me.filter_id=0;

#SA Costs
insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_sa_comp_other_l1" as name,
	me.cost_c as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type IN ('A','P')
	AND me.claim_type='CL'
	and me.level=1
	and me.annualized=0
	and me.split=1
	AND me.filter_id=0;


insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_sa_comp_other_l3" as name,
	me.cost_c as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type IN ('P')
	AND me.claim_type='CL'
	and me.level=3
	and me.annualized=0
	and me.split=1
	AND me.filter_id=0;

insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_sa_comp_other_l4" as name,
	me.cost_c as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type IN ('A')
	AND me.claim_type='CL'
	and me.level=4
	and me.annualized=0
	and me.split=1
	AND me.filter_id=0;





#############################################################################################
####Other Episodes####
###Use Variables###
##Typical Use

insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"use_typ_l1" as name,
	if((me.cost_t + me.cost_tc)>0,1,0) as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('X') and e.episode_id != 'EX9901'
	AND me.claim_type='CL'
	and me.level=1 
	and me.annualized=0
	and me.split=0
	AND me.filter_id=0;


insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"use_typ_l5" as name,
	if((me.cost_t + me.cost_tc)>0,1,0) as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('X') and e.episode_id != 'EX9901'
	AND me.claim_type='CL'
	and me.level=5 
	and me.annualized=0
	and me.split=0
	AND me.filter_id=0;

##Complication Use
insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"use_comp_l1" as name,
	if(me.cost_c>0,1,0) as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('X') and e.episode_id != 'EX9901'
	AND me.claim_type='CL'
	and me.level=1 
	and me.annualized=0
	and me.split=0
	AND me.filter_id=0;


insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"use_comp_l5" as name,
	if(me.cost_c>0,1,0) as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('X') and e.episode_id != 'EX9901'
	AND me.claim_type='CL'
	and me.level=5 
	and me.annualized=0
	and me.split=0
	AND me.filter_id=0;


###Cost Variables###
##Typical Costs
#RA Costs
insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_ra_typ_l1" as name,
	me.cost_t + me.cost_tc as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('X') and e.episode_id != 'EX9901'
	AND me.claim_type='CL'
	and me.level=1 
	and me.annualized=0
	and me.split=0
	AND me.filter_id=0;


insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_ra_typ_l5" as name,
	me.cost_t + me.cost_tc as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('X') and e.episode_id != 'EX9901'
	AND me.claim_type='CL'
	and me.level=5 
	and me.annualized=0
	and me.split=0
	AND me.filter_id=0;

#SA Costs
insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_sa_typ_l1" as name,
	me.cost_t + me.cost_tc as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('X') and e.episode_id != 'EX9901'
	AND me.claim_type='CL'
	and me.level=1 
	and me.annualized=0
	and me.split=1
	AND me.filter_id=0;


insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_sa_typ_l5" as name,
	me.cost_t + me.cost_tc as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('X') and e.episode_id != 'EX9901'
	AND me.claim_type='CL'
	and me.level=5
	and me.annualized=0
	and me.split=1
	AND me.filter_id=0;


##Complication Costs
#RA Costs
insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_ra_comp_l1" as name,
	me.cost_c as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('X') and e.episode_id != 'EX9901'
	AND me.claim_type='CL'
	and me.level=1 
	and me.annualized=0
	and me.split=0
	AND me.filter_id=0;


insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_ra_comp_l5" as name,
	me.cost_c as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('X') and e.episode_id != 'EX9901'
	AND me.claim_type='CL'
	and me.level=5 
	and me.annualized=0
	and me.split=0
	AND me.filter_id=0;

#SA Costs
insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_sa_comp_l1" as name,
	me.cost_c as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('X') and e.episode_id != 'EX9901'
	AND me.claim_type='CL'
	and me.level=1 
	and me.annualized=0
	and me.split=1
	AND me.filter_id=0;


insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_sa_comp_l5" as name,
	me.cost_c as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('X') and e.episode_id != 'EX9901'
	AND me.claim_type='CL'
	and me.level=1 
	and me.annualized=0
	and me.split=1
	AND me.filter_id=0;



#############################################################################################
####Chronic Episodes####
###Use Variables###
##Typical Use

insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"use_typ_l1" as name,
	if((me.cost_t + me.cost_tc)>0,1,0) as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('C')
	AND me.claim_type='CL'
	and me.level=1 
	and me.annualized=1
	and me.split=0
	AND me.filter_id=1;


insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"use_typ_l5" as name,
	if((me.cost_t + me.cost_tc)>0,1,0) as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('C')
	AND me.claim_type='CL'
	and me.level=5 
	and me.annualized=1
	and me.split=0
	AND me.filter_id=1;

##Complication Use
insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"use_comp_l1" as name,
	if(me.cost_c>0,1,0) as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('C')
	AND me.claim_type='CL'
	and me.level=1 
	and me.annualized=1
	and me.split=0
	AND me.filter_id=1;


insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"use_comp_l5" as name,
	if(me.cost_c>0,1,0) as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('C')
	AND me.claim_type='CL'
	and me.level=5 
	and me.annualized=1
	and me.split=0
	AND me.filter_id=1;


###Cost Variables###
##Typical Costs
#RA Costs
insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_ra_typ_l1" as name,
	me.cost_t + me.cost_tc as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('C')
	AND me.claim_type='CL'
	and me.level=1 
	and me.annualized=1
	and me.split=0
	AND me.filter_id=1;


insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_ra_typ_l5" as name,
	me.cost_t + me.cost_tc as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('C')
	AND me.claim_type='CL'
	and me.level=5 
	and me.annualized=1
	and me.split=0
	AND me.filter_id=1;

#SA Costs
insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_sa_typ_l1" as name,
	me.cost_t + me.cost_tc as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('C')
	AND me.claim_type='CL'
	and me.level=1 
	and me.annualized=1
	and me.split=1
	AND me.filter_id=1;


insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_sa_typ_l5" as name,
	me.cost_t + me.cost_tc as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('C')
	AND me.claim_type='CL'
	and me.level=5
	and me.annualized=1
	and me.split=1
	AND me.filter_id=1;


##Complication Costs
#RA Costs
insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_ra_comp_l1" as name,
	me.cost_c as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('C')
	AND me.claim_type='CL'
	and me.level=1 
	and me.annualized=1
	and me.split=0
	AND me.filter_id=1;


insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_ra_comp_l5" as name,
	me.cost_c as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('C')
	AND me.claim_type='CL'
	and me.level=5 
	and me.annualized=1
	and me.split=0
	AND me.filter_id=1;

#SA Costs
insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_sa_comp_l1" as name,
	me.cost_c as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('C')
	AND me.claim_type='CL'
	and me.level=1 
	and me.annualized=1
	and me.split=1
	AND me.filter_id=1;


insert into ra_cost_use select
	me.master_episode_id as epi_id,
	e.episode_id as epi_number,
	"cost_sa_comp_l5" as name,
	me.cost_c as value
from master_epid_level me
JOIN episode e on me.master_episode_id = e.master_episode_id
where e.episode_type in ('C')
	AND me.claim_type='CL'
	and me.level=5
	and me.annualized=1
	and me.split=1
	AND me.filter_id=1;


#################################################
#												#
#												#
#		Step 6: Remove filtered episodes		#
#												#
#												#
#################################################

###REMOVE PATIENT SAFETY FAILURE EPISODES, PLUS PREGNANCY (EX1401), NEWBORN(EX1502), PREVNT (EX9901), and URI(EA0303)
DELETE FROM ra_episode_data
where left(epi_number,2)="ES" or epi_number in ("EX1401","EX1502","EX9901");

DELETE FROM ra_riskfactors
where left(epi_number,2) in ("ES") or epi_number in ("EX1401","EX1502","EX9901");

DELETE FROM ra_subtypes
where left(epi_number,2) in ("ES") or epi_number in ("EX1401","EX1502","EX9901");

DELETE FROM ra_cost_use
where left(epi_number,2) in ("ES") or epi_number in ("EX1401","EX1502","EX9901");


###APPLY EPISODE FILTERS
DELETE FROM ra_episode_data
WHERE exists
(SELECT *
FROM  filtered_episodes
WHERE filter_id=1 and left(master_episode_id,2) in ("EA","EP","EX",'EC') and filter_fail=1
AND ra_episode_data.epi_id=filtered_episodes.master_episode_id);

DELETE FROM ra_riskfactors
WHERE exists
(SELECT master_episode_id
FROM  filtered_episodes
WHERE filter_id=1 and left(master_episode_id,2) in ("EA","EP","EX",'EC') and filter_fail=1
AND ra_riskfactors.epi_id=filtered_episodes.master_episode_id
);


DELETE FROM ra_subtypes
WHERE exists
(SELECT master_episode_id
FROM  filtered_episodes
WHERE filter_id=1  and left(master_episode_id,2) in ("EA","EP","EX",'EC') and filter_fail=1
AND ra_subtypes.epi_id=filtered_episodes.master_episode_id
);


DELETE FROM ra_cost_use
WHERE exists
(SELECT master_episode_id
FROM  filtered_episodes
WHERE filter_id=1  and left(master_episode_id,2) in ("EA","EP","EX") and filter_fail=1
AND ra_cost_use.epi_id=filtered_episodes.master_episode_id
);





#################################################
#												#
#												#
#					END							#
#												#
#												#
#################################################

