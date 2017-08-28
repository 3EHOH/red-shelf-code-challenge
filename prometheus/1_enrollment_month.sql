set search_path=epbuilder;

drop table if exists run_month_year;

create table run_month_year as
select 1 as month, extract(year from data_start_date) as year ,to_date('01'||extract(year from data_start_date),'MMYYYY') as date from run where 1>= extract(month from data_start_date) and extract(year from data_start_date)<= extract(year from  data_end_date) UNION ALL
select 2 as month, extract(year from data_start_date) as year ,to_date('02'||extract(year from data_start_date),'MMYYYY') as date  from run where 2>= extract(month from data_start_date) and extract(year from data_start_date)<= extract(year from  data_end_date) UNION ALL
select 3 as month, extract(year from data_start_date) as year ,to_date('03'||extract(year from data_start_date),'MMYYYY') as date  from run where 3>= extract(month from data_start_date) and extract(year from data_start_date)<= extract(year from  data_end_date) UNION ALL
select 4 as month, extract(year from data_start_date) as year ,to_date('04'||extract(year from data_start_date),'MMYYYY') as date  from run where 4>= extract(month from data_start_date) and extract(year from data_start_date)<= extract(year from  data_end_date) UNION ALL
select 5 as month, extract(year from data_start_date) as year ,to_date('05'||extract(year from data_start_date),'MMYYYY') as date  from run where 5>= extract(month from data_start_date) and extract(year from data_start_date)<= extract(year from  data_end_date) UNION ALL
select 6 as month, extract(year from data_start_date) as year ,to_date('06'||extract(year from data_start_date),'MMYYYY') as date  from run where 6>= extract(month from data_start_date) and extract(year from data_start_date)<= extract(year from  data_end_date) UNION ALL
select 7 as month, extract(year from data_start_date) as year ,to_date('07'||extract(year from data_start_date),'MMYYYY') as date  from run where 7>= extract(month from data_start_date) and extract(year from data_start_date)<= extract(year from  data_end_date) UNION ALL
select 8 as month, extract(year from data_start_date) as year ,to_date('08'||extract(year from data_start_date),'MMYYYY') as date  from run where 8>= extract(month from data_start_date) and extract(year from data_start_date)<= extract(year from  data_end_date) UNION ALL
select 9 as month, extract(year from data_start_date) as year ,to_date('09'||extract(year from data_start_date),'MMYYYY') as date  from run where 9>= extract(month from data_start_date) and extract(year from data_start_date)<= extract(year from  data_end_date) UNION ALL
select 10 as month, extract(year from data_start_date) as year ,to_date('10'||extract(year from data_start_date),'MMYYYY') as date  from run where 10>= extract(month from data_start_date) and extract(year from data_start_date)<= extract(year from  data_end_date) UNION ALL
select 11 as month, extract(year from data_start_date) as year ,to_date('11'||extract(year from data_start_date),'MMYYYY') as date  from run where 11>= extract(month from data_start_date) and extract(year from data_start_date)<= extract(year from  data_end_date) UNION ALL
select 12 as month, extract(year from data_start_date) as year ,to_date('12'||extract(year from data_start_date),'MMYYYY') as date  from run where 12>= extract(month from data_start_date) and extract(year from data_start_date)<= extract(year from  data_end_date) UNION ALL

select 1 as month, extract(year from data_start_date)+1 as year ,to_date('01'||extract(year from data_start_date)+1,'MMYYYY') as date  from run where 1>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 2 as month, extract(year from data_start_date)+1 as year ,to_date('02'||extract(year from data_start_date)+1,'MMYYYY') as date  from run where 2>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 3 as month, extract(year from data_start_date)+1 as year ,to_date('03'||extract(year from data_start_date)+1,'MMYYYY') as date  from run where 3>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 4 as month, extract(year from data_start_date)+1 as year ,to_date('04'||extract(year from data_start_date)+1,'MMYYYY') as date  from run where 4>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 5 as month, extract(year from data_start_date)+1 as year ,to_date('05'||extract(year from data_start_date)+1,'MMYYYY') as date  from run where 5>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 6 as month, extract(year from data_start_date)+1 as year ,to_date('06'||extract(year from data_start_date)+1,'MMYYYY') as date  from run where 6>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 7 as month, extract(year from data_start_date)+1 as year ,to_date('07'||extract(year from data_start_date)+1,'MMYYYY') as date  from run where 7>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 8 as month, extract(year from data_start_date)+1 as year ,to_date('08'||extract(year from data_start_date)+1,'MMYYYY') as date  from run where 8>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 9 as month, extract(year from data_start_date)+1 as year ,to_date('09'||extract(year from data_start_date)+1,'MMYYYY') as date  from run where 9>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 10 as month, extract(year from data_start_date)+1 as year ,to_date('10'||extract(year from data_start_date)+1,'MMYYYY') as date  from run where 10>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 11 as month, extract(year from data_start_date)+1 as year ,to_date('11'||extract(year from data_start_date)+1,'MMYYYY') as date  from run where 11>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 12 as month, extract(year from data_start_date)+1 as year ,to_date('12'||extract(year from data_start_date)+1,'MMYYYY') as date  from run where 12>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date)  UNION ALL


select 1 as month, extract(year from data_start_date)+2 as year ,to_date('01'||extract(year from data_start_date)+2,'MMYYYY') as date  from run where 1>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 2 as month, extract(year from data_start_date)+2 as year ,to_date('02'||extract(year from data_start_date)+2,'MMYYYY') as date  from run where 2>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 3 as month, extract(year from data_start_date)+2 as year ,to_date('03'||extract(year from data_start_date)+2,'MMYYYY') as date  from run where 3>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 4 as month, extract(year from data_start_date)+2 as year ,to_date('04'||extract(year from data_start_date)+2,'MMYYYY') as date  from run where 4>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 5 as month, extract(year from data_start_date)+2 as year ,to_date('05'||extract(year from data_start_date)+2,'MMYYYY') as date  from run where 5>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 6 as month, extract(year from data_start_date)+2 as year ,to_date('06'||extract(year from data_start_date)+2,'MMYYYY') as date  from run where 6>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 7 as month, extract(year from data_start_date)+2 as year ,to_date('07'||extract(year from data_start_date)+2,'MMYYYY') as date  from run where 7>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 8 as month, extract(year from data_start_date)+2 as year ,to_date('08'||extract(year from data_start_date)+2,'MMYYYY') as date  from run where 8>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 9 as month, extract(year from data_start_date)+2 as year ,to_date('09'||extract(year from data_start_date)+2,'MMYYYY') as date  from run where 9>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 10 as month, extract(year from data_start_date)+2 as year ,to_date('10'||extract(year from data_start_date)+2,'MMYYYY') as date  from run where 10>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 11 as month, extract(year from data_start_date)+2 as year ,to_date('11'||extract(year from data_start_date)+2,'MMYYYY') as date  from run where 11>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 12 as month, extract(year from data_start_date)+2 as year ,to_date('12'||extract(year from data_start_date)+2,'MMYYYY') as date  from run where 12>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date)  UNION ALL


select 1 as month, extract(year from data_start_date)+3 as year ,to_date('01'||extract(year from data_start_date)+3,'MMYYYY') as date  from run where 1>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 2 as month, extract(year from data_start_date)+3 as year ,to_date('02'||extract(year from data_start_date)+3,'MMYYYY') as date  from run where 2>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 3 as month, extract(year from data_start_date)+3 as year ,to_date('03'||extract(year from data_start_date)+3,'MMYYYY') as date  from run where 3>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 4 as month, extract(year from data_start_date)+3 as year ,to_date('04'||extract(year from data_start_date)+3,'MMYYYY') as date  from run where 4>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 5 as month, extract(year from data_start_date)+3 as year ,to_date('05'||extract(year from data_start_date)+3,'MMYYYY') as date  from run where 5>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 6 as month, extract(year from data_start_date)+3 as year ,to_date('06'||extract(year from data_start_date)+3,'MMYYYY') as date  from run where 6>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 7 as month, extract(year from data_start_date)+3 as year ,to_date('07'||extract(year from data_start_date)+3,'MMYYYY') as date  from run where 7>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 8 as month, extract(year from data_start_date)+3 as year ,to_date('08'||extract(year from data_start_date)+3,'MMYYYY') as date  from run where 8>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 9 as month, extract(year from data_start_date)+3 as year ,to_date('09'||extract(year from data_start_date)+3,'MMYYYY') as date  from run where 9>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 10 as month, extract(year from data_start_date)+3 as year ,to_date('10'||extract(year from data_start_date)+3,'MMYYYY') as date  from run where 10>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 11 as month, extract(year from data_start_date)+3 as year ,to_date('11'||extract(year from data_start_date)+3,'MMYYYY') as date  from run where 11>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date) UNION ALL
select 12 as month, extract(year from data_start_date)+3 as year ,to_date('12'||extract(year from data_start_date)+3,'MMYYYY') as date  from run where 12>= extract(month from data_start_date) and extract(year from data_start_date)+1<= extract(year from  data_end_date)  


;

drop table if exists enrollment_raw;

create table enrollment_raw as
select
run_month_year.year,
run_month_year.month,
run_month_year.date,
enrollment.member_id,
enrollment.insurance_product,enrollment.begin_date , enrollment.end_date ,
member.birth_year,
member.gender_code as sex,
member.date_of_death as dod
from enrollment
left join member
on enrollment.member_id=member.member_id
cross join run_month_year
where enrollment.isGap<>'1' 
and run_month_year.date between enrollment.begin_date and enrollment.end_date 
order by enrollment.member_id, year, month;

drop table if exists enrollment_raw_year_1;
drop table if exists enrollment_raw_year_2;
create table enrollment_raw_year_1 as
select
enrollment_raw.year,
enrollment_raw.month,
enrollment_raw.member_id,

enrollment_raw.birth_year, 
enrollment_raw.sex,
enrollment_raw.dod
from enrollment_raw
cross join run
where enrollment_raw.date between run.data_start_date and (run.data_start_date+365) 
group by enrollment_raw.year,
enrollment_raw.month,
enrollment_raw.member_id,

enrollment_raw.birth_year, 
enrollment_raw.sex,
enrollment_raw.dod;


create table enrollment_raw_year_2 as
select
enrollment_raw.year,
enrollment_raw.month,
enrollment_raw.member_id,
enrollment_raw.birth_year, 
enrollment_raw.sex,
enrollment_raw.dod
from enrollment_raw
cross join run
where enrollment_raw.date between (run.data_end_date-365) and run.data_end_date
group by
enrollment_raw.year,
enrollment_raw.month,
enrollment_raw.member_id,

enrollment_raw.birth_year, 
enrollment_raw.sex,
enrollment_raw.dod;

drop table if exists enrolled_month;

CREATE TABLE enrolled_month AS
SELECT
	member_id,
	year,
	count(distinct month) AS enrolled_month
FROM
	enrollment_raw
GROUP BY
	member_id,
	year
;


drop table if exists run_month_year;
