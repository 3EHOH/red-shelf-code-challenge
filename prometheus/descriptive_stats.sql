/*Highlight all content and then hit Run*/
drop table if exists Descriptive_Stats;

CREATE TABLE `Descriptive_Stats` (
  `Field` varchar(100) DEFAULT NULL,
  `Data` varchar(100) DEFAULT NULL
) ;

/*Min DAte*/
insert into Descriptive_Stats
(Field, Data)
SELECT
'Earliest Claim Begin Date',
MIN(claim_line.`begin_date`)
from claim_line
cross join run
where begin_date between data_start_date and data_end_date;

/*Max Date*/
insert into Descriptive_Stats
(Field, Data)
SELECT
'Latest Claim Begin Date',
MAX(claim_line.`begin_date`)
from claim_line
cross join run
where begin_date between data_start_date and data_end_date;;

/*Member w/ Claims*/
insert into Descriptive_Stats
(Field, Data)
SELECT
'Total Members With Claims',
FORMAT(COUNT(DISTINCT member_id), 0)
from claim_line
cross join run
where begin_date between data_start_date and data_end_date;;

/*Total Allowed Amount*/
insert into Descriptive_Stats
(Field, Data)
SELECT
'Total Medical Claims Allowed Amount In Dataset',
CONCAT("$", FORMAT(SUM(allowed_amt), 2))
from claim_line
cross join run
where begin_date between data_start_date and data_end_date;;

/*Total Costs in Episodes*/
insert into Descriptive_Stats
(Field, Data)
SELECT
'Total Medical Episode Costs In Dataset',
CONCAT("$", FORMAT(SUM(allowed_amt), 2))
from claim_line
cross join run
where  `assigned_count` > 0 and begin_date between data_start_date and data_end_date;

;

/*Total RX Allowed Amount*/
insert into Descriptive_Stats
(Field, Data)
SELECT
'Total Pharmacy Costs In Dataset',
CONCAT("$", FORMAT(SUM(allowed_amt), 2))
from claim_line_rx
cross join run
where rx_fill_date between data_start_date and data_end_date;;

/*Total RX Costs in Episodes*/
insert into Descriptive_Stats
(Field, Data)
SELECT
'Total Pharmacy Costs Assigned to Episodes',
CONCAT("$", FORMAT(SUM(allowed_amt), 2))
from claim_line_rx
cross join run
where rx_fill_date between data_start_date and data_end_date
and `assigned_count` <> 0;

/*Percent costs by episode*/
insert into Descriptive_Stats
(Field, Data)
SELECT
'Percentage of Medical Costs Grouped by Episodes',
CONCAT(FORMAT(SUM(case when `assigned_count` <> 0 then allowed_amt end)/SUM(allowed_amt)*100, 2), "%")
from claim_line
cross join run
where begin_date between data_start_date and data_end_date;;

/*Percent RX costs by episode*/
insert into Descriptive_Stats
(Field, Data)
SELECT
'Percentage of Pharmacy Costs Grouped by Episodes',
CONCAT(FORMAT(SUM(case when `assigned_count` <> 0 then allowed_amt end)/SUM(allowed_amt)*100, 2), "%")
from claim_line_rx
cross join run
where rx_fill_date between data_start_date and data_end_date;;

/*Total Costs in Episodes*/
insert into Descriptive_Stats
(Field, Data)
SELECT
'Total Costs Assigned to Episodes',
CONCAT("$", FORMAT(SUM(allowed_amt), 2))
from claims_combined
cross join run
where begin_date between data_start_date and data_end_date
and `assigned_count` <> 0;

/*Percent costs by episode*/
insert into Descriptive_Stats
(Field, Data)
SELECT
'Percentage of Total Costs Grouped by Episodes',
CONCAT(FORMAT(SUM(case when `assigned_count` <> 0 then allowed_amt end)/SUM(allowed_amt)*100, 2), "%")
from claims_combined
cross join run
where begin_date between data_start_date and data_end_date;;

