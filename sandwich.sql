-- phpMyAdmin SQL Dump
-- version 5.2.1deb3
-- https://www.phpmyadmin.net/
--
-- Host: localhost:3306
-- Generation Time: Feb 05, 2025 at 04:55 AM
-- Server version: 10.11.8-MariaDB-0ubuntu0.24.04.1
-- PHP Version: 8.3.6

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `sandwich`
--

-- --------------------------------------------------------

--
-- Table structure for table `block`
--

CREATE TABLE `block` (
  `slot` bigint(20) NOT NULL,
  `timestamp` bigint(20) NOT NULL,
  `tx_count` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Table structure for table `sandwich`
--

CREATE TABLE `sandwich` (
  `id` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Stand-in structure for view `sandwich_view`
-- (See below for the actual view)
--
CREATE TABLE `sandwich_view` (
`tx_hash` varchar(89)
,`signer` varchar(45)
,`slot` bigint(20)
,`order_in_block` int(11)
,`sandwich_id` int(11)
,`outer_program` varchar(45)
,`inner_program` varchar(45)
,`amm` varchar(45)
,`subject` varchar(45)
,`input_amount` varchar(45)
,`input_mint` varchar(45)
,`output_amount` varchar(45)
,`output_mint` varchar(45)
,`swap_type` enum('FRONTRUN','VICTIM','BACKRUN')
);

-- --------------------------------------------------------

--
-- Table structure for table `swap`
--

CREATE TABLE `swap` (
  `id` int(11) NOT NULL,
  `sandwich_id` int(11) NOT NULL,
  `outer_program` varchar(45) DEFAULT NULL COMMENT 'wrapper program of the swap',
  `inner_program` varchar(45) NOT NULL COMMENT 'facilitator program of the swap',
  `amm` varchar(45) NOT NULL COMMENT 'market pubkey',
  `subject` varchar(45) NOT NULL COMMENT 'beneficial owner of the tokens swapped',
  `input_mint` varchar(45) NOT NULL,
  `output_mint` varchar(45) NOT NULL,
  `input_amount` varchar(45) NOT NULL,
  `output_amount` varchar(45) NOT NULL,
  `tx_id` int(11) NOT NULL,
  `swap_type` enum('FRONTRUN','VICTIM','BACKRUN') NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Stand-in structure for view `swaps_by_wrapper`
-- (See below for the actual view)
--
CREATE TABLE `swaps_by_wrapper` (
`outer_program` varchar(45)
,`swap_type` enum('FRONTRUN','VICTIM','BACKRUN')
,`count(*)` bigint(21)
);

-- --------------------------------------------------------

--
-- Table structure for table `transaction`
--

CREATE TABLE `transaction` (
  `id` int(11) NOT NULL,
  `tx_hash` varchar(89) NOT NULL,
  `signer` varchar(45) NOT NULL,
  `slot` bigint(20) NOT NULL,
  `order_in_block` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- --------------------------------------------------------

--
-- Structure for view `sandwich_view`
--
DROP TABLE IF EXISTS `sandwich_view`;

CREATE ALGORITHM=UNDEFINED DEFINER=`sandwich_admin`@`%` SQL SECURITY DEFINER VIEW `sandwich_view`  AS SELECT `t`.`tx_hash` AS `tx_hash`, `t`.`signer` AS `signer`, `t`.`slot` AS `slot`, `t`.`order_in_block` AS `order_in_block`, `s`.`sandwich_id` AS `sandwich_id`, `s`.`outer_program` AS `outer_program`, `s`.`inner_program` AS `inner_program`, `s`.`amm` AS `amm`, `s`.`subject` AS `subject`, `s`.`input_amount` AS `input_amount`, `s`.`input_mint` AS `input_mint`, `s`.`output_amount` AS `output_amount`, `s`.`output_mint` AS `output_mint`, `s`.`swap_type` AS `swap_type` FROM ((`swap` `s` join `transaction` `t`) join `block` `b`) WHERE `s`.`tx_id` = `t`.`id` AND `t`.`slot` = `b`.`slot` ORDER BY `s`.`sandwich_id` ASC, `s`.`tx_id` ASC ;

-- --------------------------------------------------------

--
-- Structure for view `swaps_by_wrapper`
--
DROP TABLE IF EXISTS `swaps_by_wrapper`;

CREATE ALGORITHM=UNDEFINED DEFINER=`sandwich_admin`@`%` SQL SECURITY DEFINER VIEW `swaps_by_wrapper`  AS SELECT `sandwich_view`.`outer_program` AS `outer_program`, `sandwich_view`.`swap_type` AS `swap_type`, count(0) AS `count(*)` FROM `sandwich_view` GROUP BY `sandwich_view`.`outer_program`, `sandwich_view`.`swap_type` ORDER BY `sandwich_view`.`swap_type` ASC, count(0) ASC ;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `block`
--
ALTER TABLE `block`
  ADD PRIMARY KEY (`slot`);

--
-- Indexes for table `sandwich`
--
ALTER TABLE `sandwich`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `swap`
--
ALTER TABLE `swap`
  ADD PRIMARY KEY (`id`),
  ADD KEY `outer_program` (`outer_program`),
  ADD KEY `inner_program` (`inner_program`),
  ADD KEY `amm` (`amm`),
  ADD KEY `subject` (`subject`),
  ADD KEY `input_mint` (`input_mint`),
  ADD KEY `output_mint` (`output_mint`),
  ADD KEY `input_amount` (`input_amount`),
  ADD KEY `output_amount` (`output_amount`),
  ADD KEY `tx_id` (`tx_id`),
  ADD KEY `sandwich_id` (`sandwich_id`);

--
-- Indexes for table `transaction`
--
ALTER TABLE `transaction`
  ADD PRIMARY KEY (`id`),
  ADD KEY `slot` (`slot`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `sandwich`
--
ALTER TABLE `sandwich`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `swap`
--
ALTER TABLE `swap`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `transaction`
--
ALTER TABLE `transaction`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- Constraints for dumped tables
--

--
-- Constraints for table `swap`
--
ALTER TABLE `swap`
  ADD CONSTRAINT `swap_ibfk_1` FOREIGN KEY (`tx_id`) REFERENCES `transaction` (`id`),
  ADD CONSTRAINT `swap_ibfk_2` FOREIGN KEY (`sandwich_id`) REFERENCES `sandwich` (`id`);

--
-- Constraints for table `transaction`
--
ALTER TABLE `transaction`
  ADD CONSTRAINT `transaction_ibfk_1` FOREIGN KEY (`slot`) REFERENCES `block` (`slot`);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
