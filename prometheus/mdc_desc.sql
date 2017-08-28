-- MySQL dump 10.13  Distrib 5.6.22, for Linux (x86_64)
--
-- Host: localhost    Database: ECR_Template
-- ------------------------------------------------------
-- Server version	5.6.22

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `mdc_desc`
--

DROP TABLE IF EXISTS `mdc_desc`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `mdc_desc` (
  `mdc` varchar(5) NOT NULL DEFAULT '',
  `mdc_description` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`mdc`)
);
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `mdc_desc`
--

LOCK TABLES `mdc_desc` WRITE;
/*!40000 ALTER TABLE `mdc_desc` DISABLE KEYS */;
INSERT INTO `mdc_desc` VALUES ('00','Pre-MDC'),('01','Nervous System'),('02','Eye'),('03','Ear, Nose, Mouth And Throat'),('04','Respiratory System'),('05','Circulatory System'),('06','Digestive System'),('07','Hepatobiliary System And Pancreas'),('08','Musculoskeletal System And Connective Tissue'),('09','Skin, Subcutaneous Tissue And Breast'),('10','Endocrine, Nutritional And Metabolic System'),('11','Kidney And Urinary Tract'),('12','Male Reproductive System'),('13','Female Reproductive System'),('14','Pregnancy, Childbirth And Puerperium'),('15','Newborn And Other Neonates (Perinatal Period)'),('16','Blood and Blood Forming Organs and Immunological Disorders'),('17','Myeloproliferative DDs (Poorly Differentiated Neoplasms)'),('18','Infectious and Parasitic DDs'),('19','Mental Diseases and Disorders'),('20','Alcohol/Drug Use or Induced Mental Disorders'),('21','Injuries, Poison And Toxic Effect of Drugs'),('22','Burns'),('23','Factors Influencing Health Status'),('24','Multiple Significant Trauma'),('25','Human Immunodeficiency Virus Infection'),('99','No MDC');
/*!40000 ALTER TABLE `mdc_desc` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2015-03-16 14:12:20
