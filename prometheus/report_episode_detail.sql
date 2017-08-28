set search_path=epbuilder;

select 'Step: report_episode_detail.sql' as curstep, current_timestamp as datetime;
\set AUTOCOMMIT on

drop table if exists sub_distinct;
create table sub_distinct as
select child_master_episode_id,
min(association_level) as association_level
from association
group by child_master_episode_id;


DROP TABLE IF EXISTS report_episode_detail;

CREATE TABLE report_episode_detail 
(
  Filter_ID integer DEFAULT NULL,
  Member_ID varchar(50) DEFAULT NULL,
  Master_Episode_ID varchar(255) DEFAULT NULL,
  Episode_ID varchar(6) DEFAULT NULL,
  Episode_Name varchar(6) DEFAULT NULL,
  Episode_Description varchar(255) DEFAULT NULL,
  Episode_Type varchar(50) DEFAULT NULL,
  MDC varchar(2),
  MDC_Description varchar(255) DEFAULT NULL,
  Episode_Begin_Date varchar(10) DEFAULT NULL,
  Episode_End_Date varchar(10) DEFAULT NULL,
  Episode_Length integer DEFAULT NULL,
  Level integer DEFAULT NULL,
  Split_Total_Cost numeric(42,20) DEFAULT NULL,
  Split_1stPercentile_Cost numeric(42,20) DEFAULT NULL,
  Split_99thPercentile_Cost numeric(42,20) DEFAULT NULL,
  Split_80thPercentile_Cost numeric(42,20) DEFAULT NULL,
  Split_Total_PAC_Cost numeric(42,20) DEFAULT NULL,
  Split_Total_Typical_Cost numeric(42,20) DEFAULT NULL,
  Split_Total_TypicalwPAC_Cost numeric(42,20) DEFAULT NULL,
  Annualized_Split_Total_Cost numeric(42,20) DEFAULT NULL,
  Annualized_Split_1stPercentile_Cost numeric(42,20) DEFAULT NULL,
  Annualized_Split_99thPercentile_Cost numeric(42,20) DEFAULT NULL,
  Annualized_Split_80thPercentile_Cost numeric(42,20) DEFAULT NULL,
  Annualized_Split_Total_PAC_Cost numeric(42,20) DEFAULT NULL,
  Annualized_Split_Total_Typical_Cost numeric(42,20) DEFAULT NULL,
  Annualized_Split_Total_TypicalwPAC_Cost numeric(42,20) DEFAULT NULL,
  Unsplit_Total_Cost numeric(42,20) DEFAULT NULL,
  Unsplit_1stPercentile_Cost numeric(42,20) DEFAULT NULL,
  Unsplit_99thPercentile_Cost numeric(42,20) DEFAULT NULL,
  Unsplit_Total_PAC_Cost numeric(42,20) DEFAULT NULL,
  Unsplit_Total_Typical_Cost numeric(42,20) DEFAULT NULL,
  Unsplit_Total_TypicalwPAC_Cost numeric(42,20) DEFAULT NULL,
  Annualized_Unsplit_Total_Cost numeric(42,20) DEFAULT NULL,
  Annualized_Unsplit_1stPercentile_Cost numeric(42,20) DEFAULT NULL,
  Annualized_Unsplit_99thPercentile_Cost numeric(42,20) DEFAULT NULL,
  Annualized_Unsplit_Total_PAC_Cost numeric(42,20) DEFAULT NULL,
  Annualized_Unsplit_Total_Typical_Cost numeric(42,20) DEFAULT NULL,
  Annualized_Unsplit_Total_TypicalwPAC_Cost numeric(42,20) DEFAULT NULL,
  Facility_ID varchar(50) DEFAULT NULL,
  Physician_ID varchar(50) DEFAULT NULL,
  Physician_Specialty varchar(2) DEFAULT NULL,
  Split_Expected_Total_Cost numeric(42,20) DEFAULT NULL,
  Split_Expected_Typical_IP_Cost numeric(42,20) DEFAULT NULL,
  Split_Expected_Typical_Other_Cost numeric(42,20) DEFAULT NULL,
  Split_Expected_PAC_Cost numeric(42,20) DEFAULT NULL,
  Unsplit_Expected_Total_Cost numeric(42,20) DEFAULT NULL,
  Unsplit_Expected_Typical_IP_Cost numeric(42,20) DEFAULT NULL,
  Unsplit_Expected_Typical_Other_Cost numeric(42,20) DEFAULT NULL,
  Unsplit_Expected_PAC_Cost numeric(42,20) DEFAULT NULL,
  IP_PAC_Count numeric(42,20) DEFAULT NULL,
  OP_PAC_Count numeric(42,20) DEFAULT NULL,
  PB_PAC_Count numeric(42,20) DEFAULT NULL,
  RX_PAC_Count numeric(42,20) DEFAULT NULL
) 
;

INSERT /*+ direct */  into report_episode_detail
(Filter_ID, Member_ID, Master_Episode_ID, Episode_ID, Episode_Name, Episode_Description, Episode_Type, MDC, MDC_Description, Episode_Begin_Date, Episode_End_Date, Episode_Length, Level, Split_Total_Cost, Split_Total_PAC_Cost, Split_Total_Typical_Cost, Split_Total_TypicalwPAC_Cost, Facility_ID, Physician_ID)
SELECT
master_epid_level.filter_id,
e.member_id,
e.master_episode_id,
e.episode_id,
eb.NAME,
eb.DESCRIPTION,
eb.TYPE,
eb.MDC_CATEGORY,
mdc_desc.mdc_description,
e.episode_begin_date,
e.episode_end_date,
e.episode_length_days,
master_epid_level.level,
master_epid_level.cost,
master_epid_level.cost_c,
master_epid_level.cost_t,
master_epid_level.cost_tc,
e.attrib_default_facility,
e.attrib_default_physician


FROM episode e
join episode_builder_5_4_003.episode eb
on e.episode_id=eb.EPISODE_ID
join master_epid_level
on e.master_episode_id=master_epid_level.master_episode_id
left join mdc_desc
on eb.MDC_CATEGORY=mdc_desc.mdc

left join sub_distinct
on master_epid_level.master_episode_id=sub_distinct.child_master_episode_id 
where master_epid_level.level<sub_distinct.association_level and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0
or  sub_distinct.association_level is null  and  master_epid_level.claim_type='CL'
AND master_epid_level.split=1
AND master_epid_level.annualized=0;


drop table if exists melDet;
CREATE table melDet as
SELECT
mel.filter_id,
mel.master_episode_id,
mel.level,
mel.cost,
mel.cost_c,
mel.cost_t,
mel.cost_tc
FROM master_epid_level mel
left join sub_distinct 
on mel.master_episode_id=sub_distinct.child_master_episode_id 
where mel.level<sub_distinct.association_level and  mel.claim_type='CL' AND mel.split=0 AND mel.annualized=0
or   sub_distinct.association_level is null and  mel.claim_type='CL' AND mel.split=0 AND mel.annualized=0;



update /*+direct*/  report_episode_detail red
SET
Unsplit_Total_Cost=mel.cost,
Unsplit_Total_PAC_Cost=mel.cost_c,
Unsplit_Total_Typical_Cost=mel.cost_t,
Unsplit_Total_TypicalwPAC_Cost=mel.cost_tc
from melDet mel 
where mel.master_episode_id=red.Master_Episode_ID
AND mel.level=red.Level
and  mel.filter_id=red.filter_id
;


DROP TABLE melDet;

CREATE  TABLE  melDet AS
SELECT
mel.filter_id,
mel.master_episode_id,
mel.level,
mel.cost,
mel.cost_c,
mel.cost_t,
mel.cost_tc
FROM master_epid_level mel
left join sub_distinct 
on mel.master_episode_id=sub_distinct.child_master_episode_id 
where mel.level<sub_distinct.association_level and  mel.claim_type='CL' AND mel.split=1 AND mel.annualized=1
or   sub_distinct.association_level is null and  mel.claim_type='CL' AND mel.split=1 AND mel.annualized=1;

update /*+direct*/  report_episode_detail red
SET
Annualized_Split_Total_Cost=mel.cost,
Annualized_Split_Total_PAC_Cost=mel.cost_c,
Annualized_Split_Total_Typical_Cost=mel.cost_t,
Annualized_Split_Total_TypicalwPAC_Cost=mel.cost_tc
from melDet mel 
where mel.master_episode_id=red.Master_Episode_ID
AND mel.level=red.Level
and  mel.filter_id=red.filter_id
;

DROP TABLE melDet;




CREATE TABLE  melDet AS
SELECT
mel.filter_id,
mel.master_episode_id,
mel.level,
mel.cost,
mel.cost_c,
mel.cost_t,
mel.cost_tc
FROM master_epid_level mel
left join sub_distinct 
on mel.master_episode_id=sub_distinct.child_master_episode_id 
where mel.level<sub_distinct.association_level and  mel.claim_type='CL' AND mel.split=0 AND mel.annualized=1
or   sub_distinct.association_level is null and  mel.claim_type='CL' AND mel.split=0 AND mel.annualized=1;


update /*+direct*/  report_episode_detail red
SET
Annualized_Unsplit_Total_Cost=mel.cost,
Annualized_Unsplit_Total_PAC_Cost=mel.cost_c,
Annualized_Unsplit_Total_Typical_Cost=mel.cost_t,
Annualized_Unsplit_Total_TypicalwPAC_Cost=mel.cost_tc
from melDet mel 
where mel.master_episode_id=red.Master_Episode_ID
AND mel.level=red.Level
and  mel.filter_id=red.filter_id
;

DROP TABLE melDet;




CREATE TABLE  melDet AS
SELECT
mel.filter_id,
mel.master_episode_id,
mel.level,
mel.cost,
mel.cost_c,
mel.cost_t,
mel.cost_tc
FROM master_epid_level mel
left join sub_distinct 
on mel.master_episode_id=sub_distinct.child_master_episode_id 
where mel.level<sub_distinct.association_level and  mel.claim_type='CL' AND mel.split=1 AND mel.annualized=0
or   sub_distinct.association_level is null and  mel.claim_type='CL' AND mel.split=1 AND mel.annualized=0;

/*START HERE*/
update /*+direct*/  report_episode_detail red
SET
Split_Total_Cost=mel.cost,
Split_Total_PAC_Cost=mel.cost_c,
Split_Total_Typical_Cost=mel.cost_t,
Split_Total_TypicalwPAC_Cost=mel.cost_tc
from melDet mel 
where mel.master_episode_id=red.Master_Episode_ID
AND mel.level=red.Level
and  mel.filter_id=red.filter_id
;

DROP TABLE melDet;

drop table if exists percentiles;

CREATE TABLE percentiles as 
SELECT Filter_id, Master_episode_id, Episode_ID, Level,
      PERCENTILE_CONT(.01) WITHIN GROUP(ORDER BY split_total_cost) 
      OVER (PARTITION BY Filter_id, Episode_ID, Level) AS Split_1stPercentile_Cost,
            PERCENTILE_CONT(.99) WITHIN GROUP(ORDER BY split_total_cost) 
      OVER (PARTITION BY Filter_id, Episode_ID, Level) AS Split_99thPercentile_Cost,
            PERCENTILE_CONT(.80) WITHIN GROUP(ORDER BY split_total_cost) 
      OVER (PARTITION BY Filter_id, Episode_ID, Level) AS Split_80thPercentile_Cost,
            PERCENTILE_CONT(.01) WITHIN GROUP(ORDER BY unsplit_total_cost) 
      OVER (PARTITION BY Filter_id, Episode_ID, Level) AS Unsplit_1stPercentile_Cost,
                  PERCENTILE_CONT(.99) WITHIN GROUP(ORDER BY unsplit_total_cost) 
      OVER (PARTITION BY Filter_id, Episode_ID, Level) AS Unsplit_99thPercentile_Cost,
      
            PERCENTILE_CONT(.01) WITHIN GROUP(ORDER BY Annualized_Split_Total_Cost) 
      OVER (PARTITION BY Filter_id, Episode_ID, Level) AS Annualized_Split_1stPercentile_Cost,
            PERCENTILE_CONT(.99) WITHIN GROUP(ORDER BY Annualized_Split_Total_Cost) 
      OVER (PARTITION BY Filter_id, Episode_ID, Level) AS Annualized_Split_99thPercentile_Cost,
            PERCENTILE_CONT(.80) WITHIN GROUP(ORDER BY Annualized_Split_Total_Cost) 
      OVER (PARTITION BY Filter_id, Episode_ID, Level) AS Annualized_Split_80thPercentile_Cost,
            PERCENTILE_CONT(.01) WITHIN GROUP(ORDER BY Annualized_Unsplit_Total_Cost) 
      OVER (PARTITION BY Filter_id, Episode_ID, Level) AS Annualized_Unsplit_1stPercentile_Cost,
                  PERCENTILE_CONT(.99) WITHIN GROUP(ORDER BY Annualized_Unsplit_Total_Cost) 
      OVER (PARTITION BY Filter_id, Episode_ID, Level) AS Annualized_Unsplit_99thPercentile_Cost
   FROM report_episode_detail
   ORDER BY Filter_id, Episode_ID, Level;



update /*+direct*/  report_episode_detail red
SET
Split_1stPercentile_Cost=p.Split_1stPercentile_Cost,
Split_99thPercentile_Cost=p.Split_99thPercentile_Cost,
Split_80thPercentile_Cost=p.Split_80thPercentile_Cost,
Annualized_Split_1stPercentile_Cost=p.Annualized_Split_1stPercentile_Cost,
Annualized_Split_99thPercentile_Cost=p.Annualized_Split_99thPercentile_Cost,
Annualized_Split_80thPercentile_Cost=p.Annualized_Split_80thPercentile_Cost,
Unsplit_1stPercentile_Cost=p.Unsplit_1stPercentile_Cost,
Unsplit_99thPercentile_Cost=p.Unsplit_99thPercentile_Cost,
Annualized_Unsplit_1stPercentile_Cost=p.Annualized_Unsplit_1stPercentile_Cost,
Annualized_Unsplit_99thPercentile_Cost=p.Annualized_Unsplit_99thPercentile_Cost
from percentiles p 
where  p.master_episode_id=red.master_episode_id and p.episode_id=red.episode_id
AND p.level=red.level
and p.filter_id=red.filter_id

;

drop table x;


CREATE TABLE  x as

select
BTRIM(rec.epi_id,'"')  as master_episode_id,
red.level,
rec.total_exp_cost_sa_l5,
rec.exp_cost_sa_typ_l5,
rec.exp_cost_sa_comp_l5,
rec.total_exp_cost_ra_l5,
rec.exp_cost_ra_typ_l5,
rec.exp_cost_ra_comp_l5

from ra_exp_cost rec
left join report_episode_detail red
on BTRIM(rec.epi_id,'"') =red.master_episode_id
where rec.exp_cost_sa_typ_l5 is not null
and red.level=5;

update /*+direct*/  report_episode_detail red
set
split_expected_total_cost=x.total_exp_cost_sa_l5,
split_expected_typical_other_cost=x.exp_cost_sa_typ_l5,
split_expected_pac_cost=x.exp_cost_sa_comp_l5,
unsplit_expected_total_cost=x.total_exp_cost_ra_l5,
unsplit_expected_typical_other_cost=x.exp_cost_ra_typ_l5,
unsplit_expected_pac_cost=x.exp_cost_ra_comp_l5
from x
where x.master_episode_id=red.master_episode_id
and x.level=red.level
;

drop table x;

/*START HERE*/

CREATE TABLE  x as

select
BTRIM(rec.epi_id,'"')  as master_episode_id,
red.level,
rec.total_exp_cost_sa_l1,
rec.exp_cost_sa_typ_ip_l1,
rec.exp_cost_sa_typ_other_l1,
rec.exp_cost_sa_comp_other_l1,
rec.total_exp_cost_ra_l1,
rec.exp_cost_ra_typ_ip_l1,
/*rec.exp_cost_ra_typ_other_l1,*/
rec.exp_cost_ra_comp_other_l1

from ra_exp_cost rec
left join report_episode_detail red
on  BTRIM(rec.epi_id,'"')=red.master_episode_id
where rec.exp_cost_sa_typ_ip_l1 is not null
and red.level=1;

update /*+direct*/  report_episode_detail red
set
split_expected_total_cost=x.total_exp_cost_sa_l1,
split_expected_typical_ip_cost=x.exp_cost_sa_typ_ip_l1,
split_expected_typical_other_cost=x.exp_cost_sa_typ_other_l1,
split_expected_pac_cost=x.exp_cost_sa_comp_other_l1,
unsplit_expected_total_cost=x.total_exp_cost_ra_l1,
Unsplit_Expected_Typical_IP_Cost=x.exp_cost_ra_typ_ip_l1,
/*unsplit_expected_typical_other_cost=x.exp_cost_ra_typ_other_l1,*/
unsplit_expected_pac_cost=x.exp_cost_ra_comp_other_l1
from x
where x.master_episode_id=red.master_episode_id
and x.level=red.level
;

drop table if exists x;


CREATE TABLE  x as

select
 BTRIM(rec.epi_id,'"')as master_episode_id,
red.level,
rec.total_exp_cost_sa_l3,
rec.exp_cost_sa_typ_ip_l3,
rec.exp_cost_sa_typ_other_l3,
rec.exp_cost_sa_comp_other_l3,
rec.total_exp_cost_ra_l3,
rec.exp_cost_ra_typ_ip_l3,
rec.exp_cost_ra_typ_other_l3,
rec.exp_cost_ra_comp_other_l3

from ra_exp_cost rec
left join report_episode_detail red
on  BTRIM(rec.epi_id,'"')=red.master_episode_id
where rec.exp_cost_sa_typ_ip_l3 is not null
and red.level=3;

update /*+direct*/  report_episode_detail red
set
split_expected_total_cost=x.total_exp_cost_sa_l3,
split_expected_typical_ip_cost=x.exp_cost_sa_typ_ip_l3,
split_expected_typical_other_cost=x.exp_cost_sa_typ_other_l3,
split_expected_pac_cost=x.exp_cost_sa_comp_other_l3,
unsplit_expected_total_cost=x.total_exp_cost_ra_l3,
Unsplit_Expected_Typical_IP_Cost=x.exp_cost_ra_typ_ip_l3,
unsplit_expected_typical_other_cost=x.exp_cost_ra_typ_other_l3,
unsplit_expected_pac_cost=x.exp_cost_ra_comp_other_l3
from x
where x.master_episode_id=red.master_episode_id
and x.level=red.level
;

drop table x;


CREATE TABLE  x as

select
 BTRIM(rec.epi_id,'"') as master_episode_id,
red.level,
rec.total_exp_cost_sa_l4,
rec.exp_cost_sa_typ_ip_l4,
rec.exp_cost_sa_typ_other_l4,
rec.exp_cost_sa_comp_other_l4,
rec.total_exp_cost_ra_l4,
rec.exp_cost_ra_typ_ip_l4,
rec.exp_cost_ra_typ_other_l4,
rec.exp_cost_ra_comp_other_l4

from ra_exp_cost rec
left join report_episode_detail red
on  BTRIM(rec.epi_id,'"')=red.master_episode_id
where rec.exp_cost_sa_typ_ip_l4 is not null
and red.level=4;




update /*+direct*/  report_episode_detail red
set
split_expected_total_cost=x.total_exp_cost_sa_l4,
split_expected_typical_ip_cost=x.exp_cost_sa_typ_ip_l4,
split_expected_typical_other_cost=x.exp_cost_sa_typ_other_l4,
split_expected_pac_cost=x.exp_cost_sa_comp_other_l4,
unsplit_expected_total_cost=x.total_exp_cost_ra_l4,
Unsplit_Expected_Typical_IP_Cost=x.exp_cost_ra_typ_ip_l4,
unsplit_expected_typical_other_cost=x.exp_cost_ra_typ_other_l4,
unsplit_expected_pac_cost=x.exp_cost_ra_comp_other_l4
from x
where x.master_episode_id=red.master_episode_id
and x.level=red.level
;

drop table x;


SELECT
red.filter_id,
red.episode_id,
red.episode_name,
red.episode_type,
red.level,
count(*) as episode_volume,
sum(red.split_total_cost),
sum(red.annualized_split_total_cost),
sum(red.split_expected_total_cost),
(sum(red.split_total_cost))/count(*) as average_split_costs,
(sum(red.annualized_split_total_cost))/count(*) as average_annualized_split_costs,
(sum(red.split_expected_total_cost))/count(*) as average_expected_split_costs

from report_episode_detail red
where red.episode_type <> 'System Related Failure' and filter_id=1 and episode_type = 'Chronic'
group by red.filter_id,
red.episode_id,
red.episode_name,
red.episode_type,
red.level
order by red.filter_id,
red.episode_id,
red.episode_name,
red.episode_type,
red.level;
