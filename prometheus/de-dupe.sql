drop table if exists ra_cost_use_2;
drop table if exists ra_cost_use_backup;

# Create our temp table
CREATE TABLE ra_cost_use_2 (
  cepi_id varchar(255),
  epi_id varchar(75) DEFAULT NULL,
  epi_number varchar(6) DEFAULT NULL,
  name varchar(25) DEFAULT NULL,
  value int(1) DEFAULT NULL
) ;

# Create our data backup table
CREATE TABLE ra_cost_use_backup (
  epi_id varchar(75) DEFAULT NULL,
  epi_number varchar(6) DEFAULT NULL,
  name varchar(25) DEFAULT NULL,
  value int(1) DEFAULT NULL
) ;

# Backup data
insert into ra_cost_use_backup select * from ra_cost_use;

# Create our de-duped dataset in #2 table
insert into ra_cost_use_2
select
distinct(concat(epi_id,epi_number,name,value)),
epi_id,
epi_number,
name,
value
from ra_cost_use;

#Truncate the original table
truncate ra_cost_use;

# Put the de-duped data in the original table for the R script works
insert into ra_cost_use
select
epi_id,
epi_number,
name,
value
from ra_cost_use_2;


