/* insert into `play_table`(idplay_table, play_tablecol, play_tablecol1) values ('2','test', 'test2' ); */

DROP TABLE IF EXISTS `episode_information`;


CREATE TABLE `episode_information` (
  `EPISODE_ID` varchar(6) NOT NULL,
  `NAME` varchar(6) DEFAULT NULL,
  `TYPE` varchar(45) NOT NULL,
  `STATUS` varchar(45) DEFAULT NULL,
  `DESCRIPTION` varchar(37) DEFAULT NULL,
  `CREATED_DATE` datetime NOT NULL,
  `MODIFIED_DATE` datetime DEFAULT NULL,
  `USERS_USER_ID` varchar(45) DEFAULT NULL,
  `MDC_CATEGORY` varchar(2) NOT NULL,
  `PARM_SET` int(11) DEFAULT '1',
  `TRIGGER_TYPE` int(11) DEFAULT NULL,
  `TRIGGER_NUMBER` int(11) DEFAULT NULL,
  `SEPARATION_MIN` int(11) DEFAULT NULL,
  `SEPARATION_MAX` int(11) DEFAULT NULL,
  `BOUND_OFFSET` int(11) DEFAULT NULL,
  `BOUND_LENGTH` int(11) DEFAULT NULL,
  `CONDITION_MIN` int(11) DEFAULT NULL,
  `placeHolder` varchar(2) DEFAULT NULL,
  `VERSION` varchar(45) DEFAULT NULL,
  `END_OF_STUDY` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`EPISODE_ID`)
) DEFAULT CHARSET=utf8;

