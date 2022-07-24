--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

CREATE DATABASE IF NOT EXISTS employees_0000 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS employees_0001 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS employees_0002 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS employees_0003 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `employees_0000`.`student_0000`
(
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
    `uid` BIGINT(20) UNSIGNED NOT NULL,
    `name` VARCHAR(255) NOT NULL,
    `score` DECIMAL(6,2) DEFAULT '0',
    `nickname` VARCHAR(255) DEFAULT NULL,
    `gender` TINYINT(4) NULL,
    `birth_year` SMALLINT(5) UNSIGNED DEFAULT '0',
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `modified_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_uid` (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `employees_0001`.`student_0000`
(
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
    `uid` BIGINT(20) UNSIGNED NOT NULL,
    `name` VARCHAR(255) NOT NULL,
    `score` DECIMAL(6,2) DEFAULT '0',
    `nickname` VARCHAR(255) DEFAULT NULL,
    `gender` TINYINT(4) NULL,
    `birth_year` SMALLINT(5) UNSIGNED DEFAULT '0',
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `modified_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_uid` (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `employees_0002`.`student_0000`
(
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
    `uid` BIGINT(20) UNSIGNED NOT NULL,
    `name` VARCHAR(255) NOT NULL,
    `score` DECIMAL(6,2) DEFAULT '0',
    `nickname` VARCHAR(255) DEFAULT NULL,
    `gender` TINYINT(4) NULL,
    `birth_year` SMALLINT(5) UNSIGNED DEFAULT '0',
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `modified_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_uid` (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `employees_0003`.`student_0000`
(
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
    `uid` BIGINT(20) UNSIGNED NOT NULL,
    `name` VARCHAR(255) NOT NULL,
    `score` DECIMAL(6,2) DEFAULT '0',
    `nickname` VARCHAR(255) DEFAULT NULL,
    `gender` TINYINT(4) NULL,
    `birth_year` SMALLINT(5) UNSIGNED DEFAULT '0',
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `modified_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_uid` (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO employees_0000.student_0000(id,uid,name,score,nickname,gender,birth_year,created_at,modified_at) VALUES (1, 1, 'arana', 95, 'Awesome Arana', 0, 2021, NOW(), NOW());
