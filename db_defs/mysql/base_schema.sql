-- MySQL dump 10.13  Distrib 5.7.15, for Linux (x86_64)
--
-- Host: localhost    Database: panopticon
-- ------------------------------------------------------
-- Server version	5.7.15-log

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
-- Table structure for table `ansible_groups`
--

DROP TABLE IF EXISTS `ansible_groups`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ansible_groups` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(40) NOT NULL,
  `parent` int(10) unsigned DEFAULT NULL,
  `group_vars` varchar(512) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `parent` (`parent`),
  CONSTRAINT `ansible_groups_ibfk_1` FOREIGN KEY (`parent`) REFERENCES `ansible_groups` (`id`) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Temporary view structure for view `current_package_versions`
--

DROP TABLE IF EXISTS `current_package_versions`;
/*!50001 DROP VIEW IF EXISTS `current_package_versions`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE VIEW `current_package_versions` AS SELECT 
 1 AS `package_id`,
 1 AS `package_name`,
 1 AS `package_version`,
 1 AS `package_type`,
 1 AS `package_contents`,
 1 AS `event_date`*/;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `host`
--

DROP TABLE IF EXISTS `host`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `host` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(30) NOT NULL,
  `domain` varchar(40) NOT NULL,
  `os_name` varchar(20) DEFAULT NULL,
  `os_version` varchar(40) DEFAULT NULL,
  `dist_name` varchar(15) DEFAULT NULL,
  `dist_ver` varchar(40) DEFAULT NULL,
  `last_checkin` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `last_update` timestamp NULL DEFAULT '1970-01-01 05:00:00',
  `host_vars` varchar(512) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=19 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `host_group_relations`
--

DROP TABLE IF EXISTS `host_group_relations`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `host_group_relations` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `host_id_fk` int(10) unsigned NOT NULL,
  `group_id_fk` int(10) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  KEY `host_id_fk` (`host_id_fk`),
  KEY `group_id_fk` (`group_id_fk`),
  CONSTRAINT `host_group_relations_ibfk_1` FOREIGN KEY (`host_id_fk`) REFERENCES `host` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `host_group_relations_ibfk_2` FOREIGN KEY (`group_id_fk`) REFERENCES `ansible_groups` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Temporary view structure for view `host_package_versions`
--

DROP TABLE IF EXISTS `host_package_versions`;
/*!50001 DROP VIEW IF EXISTS `host_package_versions`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE VIEW `host_package_versions` AS SELECT 
 1 AS `name`,
 1 AS `domain`,
 1 AS `os_name`,
 1 AS `os_version`,
 1 AS `dist_name`,
 1 AS `dist_ver`,
 1 AS `package_name`,
 1 AS `event_date`,
 1 AS `contents`,
 1 AS `version`,
 1 AS `machine_update_date`*/;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `host_update_history`
--

DROP TABLE IF EXISTS `host_update_history`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `host_update_history` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `host_id` int(10) unsigned NOT NULL,
  `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `package_history_id` int(10) unsigned NOT NULL,
  `package_id` int(10) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  KEY `package_history_id` (`package_history_id`),
  KEY `package_id` (`package_id`),
  KEY `host_id` (`host_id`),
  CONSTRAINT `host_update_history_ibfk_1` FOREIGN KEY (`package_history_id`) REFERENCES `package_history` (`id`) ON DELETE NO ACTION ON UPDATE CASCADE,
  CONSTRAINT `host_update_history_ibfk_2` FOREIGN KEY (`package_id`) REFERENCES `package` (`id`) ON DELETE NO ACTION ON UPDATE CASCADE,
  CONSTRAINT `host_update_history_ibfk_3` FOREIGN KEY (`host_id`) REFERENCES `host` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=39171 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `package`
--

DROP TABLE IF EXISTS `package`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `package` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `package_name` varchar(80) NOT NULL,
  `package_type` varchar(20) NOT NULL,
  `contents` varchar(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3936 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `package_history`
--

DROP TABLE IF EXISTS `package_history`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `package_history` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `package_id` int(10) unsigned NOT NULL,
  `event_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `version` varchar(160) DEFAULT NULL,
  `event_type` varchar(20) DEFAULT NULL,
  `from_repository` varchar(40) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `package_id` (`package_id`),
  CONSTRAINT `package_history_ibfk_1` FOREIGN KEY (`package_id`) REFERENCES `package` (`id`) ON DELETE NO ACTION ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=38956 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Final view structure for view `current_package_versions`
--

/*!50001 DROP VIEW IF EXISTS `current_package_versions`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `current_package_versions` AS select `p`.`id` AS `package_id`,`p`.`package_name` AS `package_name`,`ph`.`version` AS `package_version`,`p`.`package_type` AS `package_type`,`p`.`contents` AS `package_contents`,`ph`.`event_date` AS `event_date` from ((`panopticon`.`package` `p` left join (select `panopticon`.`package_history`.`package_id` AS `package_id`,max(`panopticon`.`package_history`.`event_date`) AS `ev` from `panopticon`.`package_history` group by `panopticon`.`package_history`.`package_id`) `pv` on((`p`.`id` = `pv`.`package_id`))) left join `panopticon`.`package_history` `ph` on(((`ph`.`package_id` = `pv`.`package_id`) and (`ph`.`event_date` = `pv`.`ev`)))) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `host_package_versions`
--

/*!50001 DROP VIEW IF EXISTS `host_package_versions`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `host_package_versions` AS select `h`.`name` AS `name`,`h`.`domain` AS `domain`,`h`.`os_name` AS `os_name`,`h`.`os_version` AS `os_version`,`h`.`dist_name` AS `dist_name`,`h`.`dist_ver` AS `dist_ver`,`p`.`package_name` AS `package_name`,`ph`.`event_date` AS `event_date`,`p`.`contents` AS `contents`,`ph`.`version` AS `version`,`huh`.`updated` AS `machine_update_date` from ((((((select `huh`.`package_id` AS `package_id`,`huh`.`host_id` AS `host_id`,max(`huh`.`id`) AS `newest_update_id` from (`panopticon`.`host_update_history` `huh` left join `panopticon`.`package` `p` on((`huh`.`package_id` = `p`.`id`))) group by `huh`.`package_id`,`huh`.`host_id`)) `hv` left join `panopticon`.`host_update_history` `huh` on((`huh`.`id` = `hv`.`newest_update_id`))) left join `panopticon`.`package` `p` on((`p`.`id` = `hv`.`package_id`))) left join `panopticon`.`host` `h` on((`hv`.`host_id` = `h`.`id`))) left join `panopticon`.`package_history` `ph` on((`huh`.`package_history_id` = `ph`.`id`))) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2016-09-09 15:55:29
