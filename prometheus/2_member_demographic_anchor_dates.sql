set search_path=epbuilder;

DROP TABLE IF EXISTS member_sub_population2 CASCADE ;
CREATE TABLE member_sub_population2(
    member_id varchar(100) DEFAULT NULL ,
    year INTEGER DEFAULT NULL,    
    sub_population varchar(20) DEFAULT NULL,
    gender VARCHAR(2) DEFAULT NULL,
    birth_year INTEGER DEFAULT NULL,
  age_group VARCHAR(20) DEFAULT NULL,
    zip_code VARCHAR(12) DEFAULT NULL,
        county VARCHAR(100) DEFAULT NULL,
            mcregion VARCHAR(100) DEFAULT NULL,
  PPS varchar(100),
   MCO varchar(100),
    HH varchar(100),
   PCP varchar(100),
   exclusive numeric(2)
);

/* what about members with a sub pop in 1 year but no record in another?*/

insert /*+ direct */  INTO member_sub_population2
SELECT DISTINCT member_id as member_id, year as year, 'General' as sub_population
FROM enrolled_month
group by member_id, year;

/*insert /*+ direct */  'HIV' to msp. 69,472 members insert /*+ direct */ ed in 2 seconds.*/





insert /*+ direct */  INTO member_sub_population2
SELECT distinct(member_id),VBP_RPT_YEAR as year,  'HIV' as sub_population
FROM hiv_recip ;


/*insert /*+ direct */  'DD' to msp. 52,302 members insert /*+ direct */ ed in 1 seconds.*/
insert /*+ direct */  INTO member_sub_population2
SELECT distinct(member_id), VBP_RPT_YEAR as year,  'DD' as sub_population
FROM dd_recip;

/*insert /*+ direct */  'HARP' to msp. 206,090 members insert /*+ direct */ ed in 2 seconds.*/


insert /*+ direct */  INTO member_sub_population2
SELECT distinct(member_id),VBP_RPT_YEAR as year,  'HARP' as sub_population
FROM harp_recip ;

/*insert /*+ direct */  'MLTC' to msp. 13,945 members insert /*+ direct */ ed in 2 seconds.*/
insert /*+ direct */  INTO member_sub_population2
SELECT distinct(member_id), VBP_RPT_YEAR as year,  'MLTC' as sub_population
FROM mltc_recip;

/*step 3-MSP table - creation of General population*/



insert /*+ direct */  into member_sub_population2 
select
a.member_id   , a.year ,a.sub_population || ' & ' ||b.sub_population as sub_population, b.gender ,  b.birth_year ,  b.age_group
--count(*) --14510
from
(select * from member_sub_population2 where sub_population= 'HARP') a
inner join
(select * from member_sub_population2 where sub_population= 'HIV') b
on
a.member_id=b.member_id;

/*remove general sub pop where any other subpop exists for the member for a year*/



drop table if exists sub_pop_drop;
create table sub_pop_drop
as select distinct
member_id, year
from member_sub_population2
where sub_population<>'General';


update /*+ direct */  member_sub_population2
SET sub_population = case when im.member_id is  not null and  sub_population='General' then 'DROP' else sub_population end
FROM sub_pop_drop im
where member_sub_population2.member_id = im.member_id and member_sub_population2.year=im.year;


DELETE FROM member_sub_population2
WHERE sub_population='DROP' ;

drop table if exists sub_pop_drop;
/*step 4-MSP table - age logic-*/

/*update /*+ direct */  gender and birth_year according to rows in member.*/


update /*+ direct */  member_sub_population2
SET gender = im.gender_code,
    birth_year = im.birth_year,
    zip_code=im.zip_code
FROM member im
where member_sub_population2.member_id = im.member_id;


update /*+ direct */  member_sub_population2
set    county=ny_zip_to_county.county
from  ny_zip_to_county
where member_sub_population2.zip_code=ny_zip_to_county.zipcode;

update /*+ direct */  member_sub_population2
set  
    mcregion=county_mcregion.mcregion
from   county_mcregion
where member_sub_population2.county=county_mcregion.county;



/* PPS INFO update /*+ direct */ */


update /*+ direct */  member_sub_population2
set PPS= p.PPS_NAME
from PPS_table p
where p.mbr_id=member_sub_population2.member_id and p.cal_year=member_sub_population2.year;

select * from pps_table;

-- step 8 -----MCO update /*+ direct */ ----

update /*+ direct */  member_sub_population2
set MCO= p.MCO
from MCO_table p
where p.member_id=member_sub_population2.member_id and p.VBP_RPT_YEAR=member_sub_population2.year;

update /*+ direct */  member_sub_population2
set PPS= 'NO-PPS'
where PPS is null;

update /*+ direct */  member_sub_population2
set MCO= 'NO-MCO'
where MCO is null;


-- step 9----------------HH update /*+ direct */ ---------------------
update /*+ direct */  member_sub_population2
set HH= p.HH
from HH_table p
where p.member_id=member_sub_population2.member_id  and p.VBP_RPT_YEAR=member_sub_population2.year;


-- step 10----------------PCP update /*+ direct */ ---------------------

update /*+ direct */  member_sub_population2
set HH= 'NO-HH'
where HH is null;

update /*+ direct */  member_sub_population2
set PCP= p.PCP
from PCP_table p
where p.member_id=member_sub_population2.member_id and p.VBP_RPT_YEAR=member_sub_population2.year;

update /*+ direct */  member_sub_population2
set PCP= 'NO-PCP'
where PCP is null;

/* NEED TO REMOVE BY YEAR*/



DELETE FROM member_sub_population2
WHERE sub_population='MLTC' AND member_sub_population2.member_id||member_sub_population2.year IN (SELECT member_id||year FROM member_sub_population2 WHERE sub_population not in ('General', 'MLTC'));

/*Remove members who are DD but also in HARP and AIDS/HIV. 601 members removed in 2 seconds.*/
DELETE FROM member_sub_population2
WHERE sub_population='DD' AND member_sub_population2.member_id||member_sub_population2.year IN (SELECT member_id||year FROM member_sub_population2 WHERE sub_population IN ('HIV', 'HARP'));



/*step 5-creation of MSP_EXCLUSIVE table*/

/*Create the member table which has mututally sub-populations with the relationship of AIDS/HIV trumps HARP trumps DD trumps MLTC.*/
DROP TABLE IF EXISTS member_sub_population2_exclusive;

CREATE TABLE member_sub_population2_exclusive AS
SELECT * FROM member_sub_population2;

/*Remove members who are in other groups other than MLTC. 4,727 members removed in 2 seconds.*/
DELETE FROM member_sub_population2_exclusive
WHERE sub_population='MLTC' AND member_sub_population2_exclusive.member_id||member_sub_population2_exclusive.year IN (SELECT member_id||year FROM member_sub_population2_exclusive WHERE sub_population not in ('General', 'MLTC'));

/*Remove members who are DD but also in HARP and AIDS/HIV. 601 members removed in 2 seconds.*/
DELETE FROM member_sub_population2_exclusive
WHERE sub_population='DD' AND member_sub_population2_exclusive.member_id||member_sub_population2_exclusive.year IN (SELECT member_id||year FROM member_sub_population2_exclusive WHERE sub_population IN ('HIV', 'HARP'));

/*Remove members who are HARP but also in AIDS/HIV.  14,510 members removed in 2 seconds.*/
DELETE FROM member_sub_population2_exclusive
WHERE sub_population='HARP' AND member_sub_population2_exclusive.member_id||member_sub_population2_exclusive.year IN (SELECT member_id||year FROM member_sub_population2_exclusive WHERE sub_population IN ('HIV'));

/* AB - Remove members from HIV but also in HARP & HIV  */
DELETE  FROM member_sub_population2_exclusive
WHERE
sub_population= 'HIV'
AND member_sub_population2_exclusive.member_id||member_sub_population2_exclusive.year IN
(select member_id||year from member_sub_population2_exclusive where sub_population in ( 'HARP & HIV' ));

update /*+ direct */  member_sub_population2
set exclusive=1
from member_sub_population2_exclusive
where member_sub_population2_exclusive.member_id=member_sub_population2.member_id and member_sub_population2_exclusive.sub_population=member_sub_population2.sub_population
and member_sub_population2_exclusive.year=member_sub_population2.year;
