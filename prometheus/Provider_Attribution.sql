set search_path=epbuilder;

select 'Step: Provider_Attribution.sql' as curstep, current_timestamp as datetime;
\set AUTOCOMMIT on

drop sequence if exists pa_seq;
create sequence pa_seq START 100; 
drop table if exists provider_attribution;
CREATE TABLE  provider_attribution (
   id int  NOT NULL,
   master_episode_id  varchar(73) DEFAULT NULL ,
   master_claim_id  varchar(100) DEFAULT NULL,
   episode_id  varchar(10) DEFAULT NULL,
   episode_type  varchar(1) DEFAULT NULL,
   claim_line_type_code  varchar(12) DEFAULT NULL,
   trig_begin_date  date DEFAULT NULL,
   trig_end_date  date DEFAULT NULL,
   triggering_physician  varchar(30) DEFAULT NULL,
   triggering_facility  varchar(30) DEFAULT NULL,
   trig_claim_attr_phys  varchar(30) DEFAULT NULL,
   trig_claim_attr_fac  varchar(30) DEFAULT NULL,
   em_count_attr  varchar(30) DEFAULT NULL,
   em_cost_attr  varchar(30) DEFAULT NULL
  
) ;

CREATE TABLE  tmp_provider_attribution_em  (
   master_episode_id  varchar(73) DEFAULT NULL,
   physician_id  varchar(24) DEFAULT NULL,
   phys_count  bigint NOT NULL DEFAULT '0',
   phys_cost  decimal(62,20) DEFAULT NULL,
   max_count  binary(1) DEFAULT NULL,
   max_cost  binary(1) DEFAULT NULL
  ) ;

CREATE TABLE  tmp_prov_attr  (
   master_episode_id  varchar(73) DEFAULT NULL,
   physician_id  varchar(24) DEFAULT NULL,
   max_count  tinyint NOT NULL DEFAULT '0',
   max_cost  tinyint NOT NULL DEFAULT '0'
 
);

TRUNCATE table provider_attribution;
TRUNCATE table tmp_provider_attribution_em;
TRUNCATE table tmp_prov_attr;

INSERT /*+ direct */  INTO provider_attribution
(id, master_claim_id, master_episode_id, episode_id, episode_type, trig_begin_date, trig_end_date)
SELECT nextval('pa_seq'),master_claim_id, master_episode_id, episode_id, episode_type, trig_begin_date, trig_end_date FROM episode;

update /*+direct*/  provider_attribution
SET claim_line_type_code=cl.claim_line_type_code
from claim_line cl
where provider_attribution.master_claim_id=cl.master_claim_id;

/*INSERT /*+ direct */  the triggering provide and facility on the episode.*/

update /*+direct*/  provider_attribution 
SET triggering_physician=cl.physician_id,triggering_facility=cl.facility_id
from claim_line cl
where cl.master_claim_id=provider_attribution.master_claim_id
 and (provider_attribution.episode_type IN ('P','A') or (cl.master_claim_id=provider_attribution.master_claim_id and provider_attribution.episode_id='EX1502'));

drop table if exists test_provider_attribution1;

drop table if exists test_provider_attribution1;
create table test_provider_attribution1 as 
SELECT
tpa.master_episode_id,
cl.facility_id,
cl.begin_date,
cl.allowed_amt
FROM provider_attribution tpa
JOIN assignment a ON a.master_episode_id=tpa.master_episode_id 
JOIN claim_line cl ON cl.master_claim_id=a.master_claim_id
where cl.claim_line_type_code='IP'
AND cl.begin_date>= tpa.trig_begin_date
AND cl.begin_date<=(case when tpa.episode_type='P' then tpa.trig_end_date+2 else tpa.trig_end_date end)/*IF(tpa.episode_type='P', tpa.trig_end_date+2, tpa.trig_end_date)*/
AND tpa.claim_line_type_code='PB'
AND tpa.episode_type IN ('P','A')
or cl.claim_line_type_code='IP'
AND tpa.trig_begin_date between cl.begin_date and cl.end_date
AND tpa.claim_line_type_code='PB'
AND tpa.episode_type IN ('P','A')
group by tpa.master_episode_id , cl.facility_id,
cl.begin_date,
cl.allowed_amt
order by tpa.master_episode_id , cl.begin_date, cl.allowed_amt DESC
;

/*#grab earliest begin and highest cost*/


/*break grouping field start*/
drop table if exists test_provider_attribution2_t0;
create table test_provider_attribution2_t0 as
SELECT test_provider_attribution1.master_episode_id,        test_provider_attribution1.facility_id 
FROM test_provider_attribution1
group bY test_provider_attribution1.master_episode_id ,test_provider_attribution1.facility_id   ;

drop table if exists test_provider_attribution2_t1;
create table test_provider_attribution2_t1 as
SELECT test_provider_attribution1.master_episode_id, test_provider_attribution1.facility_id, min(test_provider_attribution1.begin_Date) begin_date
FROM test_provider_attribution1
group bY test_provider_attribution1.master_episode_id , test_provider_attribution1.facility_id, test_provider_attribution1.begin_Date
ORDER BY test_provider_attribution1.begin_Date;

drop table if exists test_provider_attribution2_t2;
create table test_provider_attribution2_t2 as
SELECT test_provider_attribution1.master_episode_id, test_provider_attribution1.facility_id,max(test_provider_attribution1.allowed_amt) allowed_amt
FROM test_provider_attribution1
group bY test_provider_attribution1.master_episode_id , test_provider_attribution1.facility_id, test_provider_attribution1.allowed_amt
ORDER BY test_provider_attribution1.allowed_amt desc;

drop table if exists test_provider_attribution2;
create table test_provider_attribution2 as
SELECT t0.master_episode_id, t0.facility_id,
t1.begin_date, t2.allowed_amt
FROM test_provider_attribution2_t0 t0
left join test_provider_attribution2_t1 t1 on t1.master_episode_id =t0.master_episode_id  and t1.facility_id=t0.facility_id
left join test_provider_attribution2_t2 t2 on t2.master_episode_id =t0.master_episode_id  and t2.facility_id=t0.facility_id
;
drop table if exists test_provider_attribution2_t0;
drop table if exists test_provider_attribution2_t1;
drop table if exists test_provider_attribution2_t2;

/*break grouping field end*/


update /*+direct*/  provider_attribution
SET trig_claim_attr_fac=cl.facility_id
from test_provider_attribution2 cl 
where cl.master_episode_id=provider_attribution.master_episode_id;


/*#grab overlapping op facility id*/

drop table if exists test_provider_attribution1;
create table test_provider_attribution1 as 
SELECT
tpa.master_episode_id,
cl.facility_id,
cl.begin_date,
cl.allowed_amt
FROM provider_attribution tpa
JOIN assignment a ON a.master_episode_id=tpa.master_episode_id 
JOIN claim_line cl ON cl.master_claim_id=a.master_claim_id
where cl.claim_line_type_code='OP'
AND tpa.trig_claim_attr_fac IS NULL
AND cl.begin_date>= tpa.trig_begin_date
AND cl.begin_date<=(case when tpa.episode_type='P' then tpa.trig_end_date+2 else tpa.trig_end_date end)/*IF(tpa.episode_type='P', tpa.trig_end_date+2, tpa.trig_end_date)*/
AND tpa.claim_line_type_code='PB'
AND tpa.episode_type IN ('P','A')

or cl.claim_line_type_code='OP'
AND tpa.trig_claim_attr_fac IS NULL
AND tpa.trig_begin_date between cl.begin_date and cl.end_date
AND tpa.claim_line_type_code='PB'
AND tpa.episode_type IN ('P','A')

group by tpa.master_episode_id , cl.facility_id,
cl.begin_date,
cl.allowed_amt
order by tpa.master_episode_id , cl.begin_date, cl.allowed_amt DESC
;


/*#grab earliest begin and highest cost*/


/*break grouping field start*/
drop table if exists test_provider_attribution2_t0;
create table test_provider_attribution2_t0 as
SELECT test_provider_attribution1.master_episode_id,        test_provider_attribution1.facility_id 
FROM test_provider_attribution1
group bY test_provider_attribution1.master_episode_id ,test_provider_attribution1.facility_id   ;


drop table if exists test_provider_attribution2_t1;
create table test_provider_attribution2_t1 as
SELECT test_provider_attribution1.master_episode_id, test_provider_attribution1.facility_id, min(test_provider_attribution1.begin_Date) begin_date
FROM test_provider_attribution1
group bY test_provider_attribution1.master_episode_id , test_provider_attribution1.facility_id, test_provider_attribution1.begin_Date
ORDER BY test_provider_attribution1.begin_Date;


drop table if exists test_provider_attribution2_t2;
create table test_provider_attribution2_t2 as
SELECT test_provider_attribution1.master_episode_id, test_provider_attribution1.facility_id,max(test_provider_attribution1.allowed_amt) allowed_amt
FROM test_provider_attribution1
group bY test_provider_attribution1.master_episode_id , test_provider_attribution1.facility_id, test_provider_attribution1.allowed_amt
ORDER BY test_provider_attribution1.allowed_amt desc;


drop table if exists test_provider_attribution2;
create table test_provider_attribution2 as
SELECT t0.master_episode_id, t0.facility_id,
t1.begin_date, t2.allowed_amt
FROM test_provider_attribution2_t0 t0
left join test_provider_attribution2_t1 t1 on t1.master_episode_id =t0.master_episode_id  and t1.facility_id=t0.facility_id
left join test_provider_attribution2_t2 t2 on t2.master_episode_id =t0.master_episode_id  and t2.facility_id=t0.facility_id;
drop table if exists test_provider_attribution2_t0;
drop table if exists test_provider_attribution2_t1;
drop table if exists test_provider_attribution2_t2;

/*break grouping field end*/

update /*+direct*/  provider_attribution
SET trig_claim_attr_fac=cl.facility_id
from test_provider_attribution2 cl where cl.master_episode_id=provider_attribution.master_episode_id;

/*Now get physician id's; We do this ONLY for procedural episodes and do NOT use a default trigger physician.*/

drop table if exists test_provider_attribution1;
create table test_provider_attribution1 as 
SELECT
tpa.master_episode_id,
clm.physician_id,
cl.allowed_amt
FROM provider_attribution tpa
JOIN assignment a ON a.master_episode_id=tpa.master_episode_id 
JOIN claims_combined cl ON cl.master_claim_id=a.master_claim_id
join claim_line clm on cl.master_claim_id=clm.master_claim_id
join code c on cl.id=c.u_c_id
Join episode_builder_5_4_003.episode_to_code ect2 on ect2.code_value=c.code_value and ect2.code_type_id=c.nomen
where tpa.episode_type ='P'
and cl.claim_line_type_code='PB'
and ect2.FUNCTION_ID='tr'
AND cl.begin_date>= tpa.trig_begin_date
AND cl.begin_date<=(case when tpa.episode_type='P' then tpa.trig_end_date+2 else tpa.trig_end_date end)/*IF(tpa.episode_type='P', tpa.trig_end_date+2, tpa.trig_end_date)*/

or tpa.episode_type ='P'
and cl.claim_line_type_code='PB'
and ect2.FUNCTION_ID='tr'
AND tpa.trig_begin_date between cl.begin_date and cl.end_date

group by tpa.master_episode_id , clm.physician_id,
cl.allowed_amt
order by tpa.master_episode_id , cl.allowed_amt 
;


/*#grab earliest begin and highest cost*/


/*break grouping field start*/
drop table if exists test_provider_attribution2_t0;
create table test_provider_attribution2_t0 as
SELECT test_provider_attribution1.master_episode_id,        test_provider_attribution1.physician_id 
FROM test_provider_attribution1
group bY test_provider_attribution1.master_episode_id ,test_provider_attribution1.physician_id  ;


drop table if exists test_provider_attribution2_t2;
create table test_provider_attribution2_t2 as
SELECT test_provider_attribution1.master_episode_id, test_provider_attribution1.physician_id,max(test_provider_attribution1.allowed_amt) allowed_amt
FROM test_provider_attribution1
group bY test_provider_attribution1.master_episode_id , test_provider_attribution1.physician_id, test_provider_attribution1.allowed_amt
ORDER BY test_provider_attribution1.allowed_amt desc;

drop table if exists test_provider_attribution2;
create table test_provider_attribution2 as
SELECT t0.master_episode_id, t0.physician_id,
t2.allowed_amt
FROM test_provider_attribution2_t0 t0
left join test_provider_attribution2_t2 t2 on t2.master_episode_id =t0.master_episode_id  and t2.physician_id=t0.physician_id;

drop table if exists test_provider_attribution2_t0;
drop table if exists test_provider_attribution2_t2;

/*break grouping field end*/

update /*+direct*/  provider_attribution
SET trig_claim_attr_phys=cl.physician_id
from test_provider_attribution2 cl where cl.master_episode_id=provider_attribution.master_episode_id;

drop table if exists test_provider_attribution1;
drop table if exists test_provider_attribution2;
/*INSERT   the max counts and costs for each episode into the tmp table */
/* CREATE TABLE tmp_provider_attribution_em AS */

INSERT /*+ direct */  INTO tmp_provider_attribution_em
(master_episode_id, physician_id, phys_count, phys_cost)
SELECT
tpa.master_episode_id,
clm.physician_id,
count(clm.physician_id),
sum(cl.allowed_amt)
FROM provider_attribution tpa
JOIN assignment a ON a.master_episode_id=tpa.master_episode_id
JOIN claims_combined cl ON cl.master_claim_id=a.master_claim_id
join claim_line clm on cl.master_claim_id=clm.master_claim_id
AND cl.claim_line_type_code='PB'
AND tpa.episode_type<>'P'
AND clm.physician_id IS NOT NULL
JOIN code c ON c.u_c_id=cl.id
JOIN episode_builder_5_4_003.global_em_201603 gem ON c.code_value=gem.VALUE
AND c.nomen in ('CPT','HCPC')
GROUP BY tpa.master_episode_id, clm.physician_id
ORDER BY tpa.master_episode_id, count(clm.physician_id) DESC, sum(cl.allowed_amt) DESC;




drop table if exists tmp_prov_attr_t2;
create table tmp_prov_attr_t2 as 
SELECT master_episode_id, max(phys_cost) 
FROM tmp_provider_attribution_em 
GROUP BY master_episode_id;

INSERT /*+ direct */  INTO tmp_prov_attr
(master_episode_id,
physician_id,
max_count,
max_cost)
SELECT 
t1.master_episode_id,
t1.physician_id,
0 AS max_count,
1 AS max_cost 
FROM tmp_provider_attribution_em t1
left join tmp_prov_attr_t2 t2 on t2.master_episode_id=t1.master_episode_id;



INSERT /*+ direct */  INTO tmp_prov_attr
(master_episode_id,
physician_id,
max_count,
max_cost)
SELECT 
t1.master_episode_id,
t1.physician_id,
1 AS max_count,
0 AS max_cost 
FROM tmp_provider_attribution_em t1
left join tmp_prov_attr_t2 t2 on t2.master_episode_id=t1.master_episode_id;
drop table if exists tmp_prov_attr_t2;




/* finally get the physician with the highest count who has the 
highest cost if there's more than one with the same max count... */



/*break group field start*/
drop table if exists upd_t0;
create table upd_t0 as
SELECT 
t1.master_episode_id
FROM tmp_provider_attribution_em t1
JOIN tmp_prov_attr t2 ON t1.master_episode_id=t2.master_episode_id
AND t1.physician_id=t2.physician_id
AND t2.max_count=1
GROUP BY t1.master_episode_id ;


drop table if exists upd_t1;
create table upd_t1 as
SELECT 
t1.master_episode_id,
 max(phys_cost)  maxCost
FROM tmp_provider_attribution_em t1
JOIN tmp_prov_attr t2 ON t1.master_episode_id=t2.master_episode_id
AND t1.physician_id=t2.physician_id
AND t2.max_count=1
GROUP BY t1.master_episode_id ;


drop table if exists upd_t3;
create table upd_t3 as
SELECT 
t1.master_episode_id, 
min(t1.physician_id ) physician_id
FROM tmp_provider_attribution_em t1
JOIN tmp_prov_attr t2 ON t1.master_episode_id=t2.master_episode_id
AND t1.physician_id=t2.physician_id
AND t2.max_count=1
GROUP BY t1.master_episode_id ;


drop table if exists upd_t2;
create table upd_t2 as
SELECT 
t1.master_episode_id,
min(t1.phys_count) phys_count
FROM tmp_provider_attribution_em t1
JOIN tmp_prov_attr t2 ON t1.master_episode_id=t2.master_episode_id
AND t1.physician_id=t2.physician_id
AND t2.max_count=1
GROUP BY t1.master_episode_id ;

drop table if exists upd_t;
create table upd_t as
SELECT 
t0.master_episode_id,
t3.physician_id as em_count_attr,
t2.phys_count,
t1.maxCost phys_cost
from upd_t0 t0
left join upd_t1 t1 on t1.master_episode_id=t0.master_episode_id
left join upd_t2 t2 on t2.master_episode_id=t0.master_episode_id
left join upd_t3 t3 on t3.master_episode_id=t0.master_episode_id;

update /*+direct*/  provider_attribution 
set em_count_attr=final.em_count_attr
from upd_t final
where provider_attribution.master_episode_id=final.master_episode_id;

drop table if exists upd_t;
drop table if exists upd_t0;
drop table if exists upd_t1;
drop table if exists upd_t2;
drop table if exists upd_t3;

/*break grouping fields end*/


/* finally get the physician with the highest cost who has the 
highest count if there's more than one with the same max cost... */



/*break group fields start*/
drop table if exists upd_t0;
create table upd_t0 as
SELECT 
t1.master_episode_id
FROM tmp_provider_attribution_em t1
JOIN tmp_prov_attr t2 ON t1.master_episode_id=t2.master_episode_id
AND t1.physician_id=t2.physician_id
AND t2.max_cost=1
GROUP BY t1.master_episode_id ;


drop table if exists upd_t1;
create table upd_t1 as
SELECT 
t1.master_episode_id, max(phys_count) phys_count
FROM tmp_provider_attribution_em t1
JOIN tmp_prov_attr t2 ON t1.master_episode_id=t2.master_episode_id
AND t1.physician_id=t2.physician_id
AND t2.max_cost=1
GROUP BY t1.master_episode_id ;


drop table if exists upd_t2;
create table upd_t2 as
SELECT 
t1.master_episode_id, min(phys_cost) phys_cost
FROM tmp_provider_attribution_em t1
JOIN tmp_prov_attr t2 ON t1.master_episode_id=t2.master_episode_id
AND t1.physician_id=t2.physician_id
AND t2.max_cost=1
GROUP BY t1.master_episode_id ;

drop table if exists upd_t3;
create table upd_t3 as
SELECT 
t1.master_episode_id, min(t1.physician_id) physician_id
FROM tmp_provider_attribution_em t1
JOIN tmp_prov_attr t2 ON t1.master_episode_id=t2.master_episode_id
AND t1.physician_id=t2.physician_id
AND t2.max_cost=1
GROUP BY t1.master_episode_id ;

drop table if exists upd_t;
create table upd_t as
SELECT 
t0.master_episode_id,
t3.physician_id as em_cost_attr,
t1.phys_count,
t2.phys_cost
from upd_t0 t0
left join upd_t1 t1 on t1.master_episode_id=t0.master_episode_id
left join upd_t2 t2 on t2.master_episode_id=t0.master_episode_id
left join upd_t3 t3 on t3.master_episode_id=t0.master_episode_id;

update /*+direct*/  provider_attribution 
SET em_cost_attr=final.em_cost_attr
from upd_t final
where provider_attribution.master_episode_id=final.master_episode_id;

drop table if exists upd_t;
drop table if exists upd_t0;
drop table if exists upd_t1;
drop table if exists upd_t2;
drop table if exists upd_t3;
/*break group fields end*/

drop table if exists episode_w_pa;


create table episode_w_pa as
select
episode.master_episode_id,

case when episode.episode_type = 'P' and  trig_claim_attr_phys is not null then trig_claim_attr_phys 
     when episode.episode_type = 'P' and  trig_claim_attr_phys is  null then triggering_physician 
     when episode.episode_type = 'A' then em_count_attr 
     when episode.episode_type = 'C' then em_count_attr 
     when episode.episode_type = 'X' then em_count_attr end as attrib_default_physician,

     
case when episode.episode_type in ('P','A') and  trig_claim_attr_fac is not null then trig_claim_attr_fac 
     when episode.episode_type in ('P','A') and  trig_claim_attr_fac is  null then  triggering_facility 
     when episode.episode_id = 'EX1502' and  trig_claim_attr_fac is not null then trig_claim_attr_fac 
     when episode.episode_id = 'EX1502' and  trig_claim_attr_fac is  null then   triggering_facility end  as attrib_default_facility

from episode
left join provider_attribution 
on episode.master_episode_id=provider_attribution.master_episode_id;

update /*+direct*/  episode
SET attrib_default_physician=cl.attrib_default_physician,
attrib_default_facility=cl.attrib_default_facility
from episode_w_pa cl where cl.master_episode_id=episode.master_episode_id;

drop table if exists episode_w_pa;