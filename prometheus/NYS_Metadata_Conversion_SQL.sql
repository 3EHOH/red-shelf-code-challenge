/* 
PROMETHEUS Analytics Changes for NYS
Metadata update SQL for 5.4.002 to 5.4.002 NYS
*/


/*
Sick Care time window changed from 90 days to End of Study
*/
UPDATE episode
SET BOUND_LENGTH=0, END_OF_STUDY=1
WHERE EPISODE_ID='ES9901';

/*
URI added as episode trigger for SICKCR
*/
INSERT INTO episode_lookback_trigger
(EPISODE_ID, LOOKBACK_EPISODE_ID)
VALUES 
('ES9901','EA0303');

/*
RHNTS added as episode trigger for SICKCR
*/
INSERT INTO episode_lookback_trigger
(EPISODE_ID, LOOKBACK_EPISODE_ID)
VALUES 
('ES9901','EC0301');

/*
TONSIL added as episode trigger for SICKCR
*/
INSERT INTO episode_lookback_trigger
(EPISODE_ID, LOOKBACK_EPISODE_ID)
VALUES 
('ES9901','EP0301');

/*
For PREGN, change CSECT association from Complication to Typical
*/
UPDATE episode_association
SET ASSOCIATION='Typical'
WHERE PRIMARY_EPISODE_ID='EX1401'
AND SECONDARY_EPISODE_ID='EP1404';

/*
For RHNTS, remove URI association
*/
DELETE FROM episode_association
WHERE PRIMARY_EPISODE_ID='EC0301'
AND SECONDARY_EPISODE_ID='EA0303'; 

/*
For HCV, remove URI association
*/
DELETE FROM episode_association
WHERE PRIMARY_EPISODE_ID='EX0701'
AND SECONDARY_EPISODE_ID='EA0303'; 

/*
For ASTHMA, remove URI association
*/
DELETE FROM episode_association
WHERE PRIMARY_EPISODE_ID='EC0401'
AND SECONDARY_EPISODE_ID='EA0303'; 

/*
For COPD, remove URI association
*/
DELETE FROM episode_association
WHERE PRIMARY_EPISODE_ID='EC0402'
AND SECONDARY_EPISODE_ID='EA0303';

/* 
for SICKCR add URI association
*/
INSERT INTO episode_association
(ASSOCIATION, 
LEVEL, 
PRIMARY_EPISODE_ID, 
SECONDARY_EPISODE_ID, 
START_DAY, 
END_DAY) 
VALUES
(
'Typical',
5,
'ES9901',
'EA0303',
'Default',
'Default'
);

/* 
for SICKCR add RHNTS association
*/
INSERT INTO episode_association
(ASSOCIATION, 
LEVEL, 
PRIMARY_EPISODE_ID, 
SECONDARY_EPISODE_ID, 
START_DAY, 
END_DAY) 
VALUES
(
'Typical',
5,
'ES9901',
'EC0301',
'Default',
'Default'
);

/* 
for SICKCR add TONSIL association
*/
INSERT INTO episode_association
(ASSOCIATION, 
LEVEL, 
PRIMARY_EPISODE_ID, 
SECONDARY_EPISODE_ID, 
START_DAY, 
END_DAY) 
VALUES
(
'Typical',
5,
'ES9901',
'EP0301',
'Default',
'Default'
);






