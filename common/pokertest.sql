-- MySQL dump 10.13  Distrib 5.5.44, for debian-linux-gnu (x86_64)
--
-- Host: localhost    Database: pokertest
-- ------------------------------------------------------
-- Server version	5.5.44-0ubuntu0.14.04.1

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
-- Table structure for table `a1`
--

DROP TABLE IF EXISTS `a1`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `a1` (
  `id` tinyint(2) NOT NULL AUTO_INCREMENT,
  `name` char(2) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `a1`
--

LOCK TABLES `a1` WRITE;
/*!40000 ALTER TABLE `a1` DISABLE KEYS */;
INSERT INTO `a1` VALUES (1,'pj');
/*!40000 ALTER TABLE `a1` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `a1_map_b1`
--

DROP TABLE IF EXISTS `a1_map_b1`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `a1_map_b1` (
  `id` tinyint(2) NOT NULL AUTO_INCREMENT,
  `a1id` tinyint(2) NOT NULL,
  `b1id` tinyint(2) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `a1id` (`a1id`),
  KEY `b1id` (`b1id`),
  CONSTRAINT `a1_map_b1_ibfk_1` FOREIGN KEY (`a1id`) REFERENCES `a1` (`id`),
  CONSTRAINT `a1_map_b1_ibfk_2` FOREIGN KEY (`b1id`) REFERENCES `b1` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `a1_map_b1`
--

LOCK TABLES `a1_map_b1` WRITE;
/*!40000 ALTER TABLE `a1_map_b1` DISABLE KEYS */;
INSERT INTO `a1_map_b1` VALUES (1,1,1);
/*!40000 ALTER TABLE `a1_map_b1` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `b1`
--

DROP TABLE IF EXISTS `b1`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `b1` (
  `id` tinyint(2) NOT NULL AUTO_INCREMENT,
  `name` char(2) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `b1`
--

LOCK TABLES `b1` WRITE;
/*!40000 ALTER TABLE `b1` DISABLE KEYS */;
INSERT INTO `b1` VALUES (1,'ad');
/*!40000 ALTER TABLE `b1` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2015-08-08 19:19:27
