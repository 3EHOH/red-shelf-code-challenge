-- MySQL dump 10.13  Distrib 5.6.22, for Linux (x86_64)
--
-- Host: localhost    Database: CHC_2
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
-- Table structure for table `episode_information`
--

DROP TABLE IF EXISTS `episode_information`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `episode_information` (
  `ep_type` varchar(50) DEFAULT NULL,
  `Acronym` varchar(50) DEFAULT NULL,
  `EPISODES` varchar(255) DEFAULT NULL,
  `eps_length` varchar(50) DEFAULT NULL,
  `ep_level` int(3) DEFAULT NULL,
  `Lvl_Complete` int(3) DEFAULT NULL,
  `Def_Length` int(3) DEFAULT NULL
);
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `episode_information`
--

LOCK TABLES `episode_information` WRITE;
/*!40000 ALTER TABLE `episode_information` DISABLE KEYS */;
INSERT INTO `episode_information` VALUES ('Acute Medical','AMI','AMI','<180',1,4,1),('Acute Medical','PNE','Pneumonia','<180',1,4,1),('Acute Medical','STR','Stroke','<180',1,4,1),('Acute Medical','URI','Acute URI / Acute Sinusitis','<180',1,4,1),('Acute Medical','HIPLFR','Hip / Pelvic Fracture','<180',1,4,1),('Acute Medical','DIVERT','Diverticulitis','<180',1,4,1),('Chronic','CAD','CAD','>180',1,5,0),('Chronic','RHNTS','Allergic Rhinitis / Chronic Sinusitis','>180',1,5,0),('Chronic','LBP','Low Back Pain / Radiculitis','>180',1,5,0),('Chronic','OSTEOA','Osteoarthritis','>180',1,5,0),('Chronic','DEPRSN','Depression','>180',1,5,0),('Chronic','BIPLR','Bipolar Disorders','>180',1,5,0),('Chronic','CHF','CHF','>180',1,5,0),('Chronic','HTN','Hypertension','>180',1,5,0),('Chronic','COPD','COPD','>180',1,5,0),('Chronic','ASTHMA','Asthma','>180',1,5,0),('Chronic','GERD','GERD','>180',1,5,0),('Chronic','DIAB','Diabetes','>180',1,5,0),('Chronic','ARRBLK','Arrhythmias / Heart Block','>180',1,5,0),('Chronic','GLCOMA','Glaucoma','>180',1,5,0),('Other','PREGN','Pregnancy','>180',1,5,1),('Other','CLNCAN','Colon Cancer','>180',1,5,1),('Other','RCLCAN','Rectal Cancer','>180',1,5,1),('Other','BRSTCA','Breast Cancer','>180',1,5,1),('Other','LNGCAN','Lung Cancer','>180',1,5,1),('Other','PREVNT','Preventive care','>180',1,5,1),('Other','PRSTCA','Prostate Cancer','>180',1,5,1),('Procedural','CXCABG','Complex CABG','<180',1,3,1),('Procedural','HYST','Hysterectomy','<180',1,3,1),('Procedural','CSECT','C-Section','<180',1,3,1),('Procedural','VAGDEL','Vaginal Delivery','<180',1,3,1),('Procedural','PCMDFR','Pacemakers/defibrillators','<180',1,3,1),('Procedural','BARI','Bariatric Surgery','<180',1,3,1),('Procedural','CTRTSU','Cataract Surgery','<180',1,3,1),('Procedural','TONSIL','Tonsillectomy','<180',1,3,1),('Procedural','LBRLAM','Spinal Fusion / Laminectomy','<180',1,3,1),('Procedural','BSTBIO','Breast Biopsy','<180',1,3,1),('Procedural','MSTCMY','Mastectomy for Breast Cancer','<180',1,3,1),('Procedural','PCI','PCI','<180',1,3,1),('Procedural','TURP','TURP (Transurethral prostate resection)','<180',1,3,1),('Procedural','SHLDRP','Shoulder Replacement','<180',1,3,1),('Procedural','PRSCMY','Prostatectomy','<180',1,3,1),('Procedural','EGD','Upper GI endoscopy','<180',1,3,1),('Procedural','COLOS','Colonoscopy','<180',1,3,1),('Procedural','COLON','Colon Resection','<180',1,3,1),('Procedural','GBSURG','GB Surgery','<180',1,3,1),('Procedural','HIPRPL','Hip Replacement / Hip Revision','<180',1,3,1),('Procedural','KNRPL','Knee Replacement / Knee Revision','<180',1,3,1),('Procedural','KNARTH','Knee Arthroscopy','<180',1,3,1);
/*!40000 ALTER TABLE `episode_information` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2015-03-17 14:22:41
