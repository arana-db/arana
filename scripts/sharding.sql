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

CREATE DATABASE IF NOT EXISTS employees_0000_r CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- employees_0000, student_0000~student_0007
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
    UNIQUE KEY `uk_uid` (`uid`),
    KEY `nickname` (`nickname`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
CREATE TABLE IF NOT EXISTS `employees_0000`.`student_0001` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000`.`student_0002` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000`.`student_0003` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000`.`student_0004` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000`.`student_0005` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000`.`student_0006` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000`.`student_0007` LIKE `employees_0000`.`student_0000`;

-- employees_0001, student_0008~student_0015
CREATE TABLE IF NOT EXISTS `employees_0001`.`student_0008` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0001`.`student_0009` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0001`.`student_0010` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0001`.`student_0011` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0001`.`student_0012` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0001`.`student_0013` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0001`.`student_0014` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0001`.`student_0015` LIKE `employees_0000`.`student_0000`;

-- employees_0002, student_0016~student_0023
CREATE TABLE IF NOT EXISTS `employees_0002`.`student_0016` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0002`.`student_0017` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0002`.`student_0018` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0002`.`student_0019` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0002`.`student_0020` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0002`.`student_0021` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0002`.`student_0022` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0002`.`student_0023` LIKE `employees_0000`.`student_0000`;

-- employees_0003, student_0024~student_0031
CREATE TABLE IF NOT EXISTS `employees_0003`.`student_0024` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0003`.`student_0025` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0003`.`student_0026` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0003`.`student_0027` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0003`.`student_0028` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0003`.`student_0029` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0003`.`student_0030` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0003`.`student_0031` LIKE `employees_0000`.`student_0000`;

-- employees_0000_r, student_0000~student_0007
CREATE TABLE IF NOT EXISTS `employees_0000_r`.`student_0000` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000_r`.`student_0001` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000_r`.`student_0002` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000_r`.`student_0003` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000_r`.`student_0004` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000_r`.`student_0005` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000_r`.`student_0006` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000_r`.`student_0007` LIKE `employees_0000`.`student_0000`;

-- employees_0000, __shadow_student_0000~__shadow_student_0007
CREATE TABLE IF NOT EXISTS `employees_0000`.`__shadow_student_0000` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000`.`__shadow_student_0001` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000`.`__shadow_student_0002` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000`.`__shadow_student_0003` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000`.`__shadow_student_0004` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000`.`__shadow_student_0005` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000`.`__shadow_student_0006` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000`.`__shadow_student_0007` LIKE `employees_0000`.`student_0000`;

-- employees_0001, __shadow_student_0008~__shadow_student_0015
CREATE TABLE IF NOT EXISTS `employees_0001`.`__shadow_student_0008` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0001`.`__shadow_student_0009` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0001`.`__shadow_student_0010` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0001`.`__shadow_student_0011` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0001`.`__shadow_student_0012` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0001`.`__shadow_student_0013` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0001`.`__shadow_student_0014` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0001`.`__shadow_student_0015` LIKE `employees_0000`.`student_0000`;

-- employees_0002, __shadow_student_0016~__shadow_student_0023
CREATE TABLE IF NOT EXISTS `employees_0002`.`__shadow_student_0016` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0002`.`__shadow_student_0017` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0002`.`__shadow_student_0018` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0002`.`__shadow_student_0019` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0002`.`__shadow_student_0020` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0002`.`__shadow_student_0021` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0002`.`__shadow_student_0022` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0002`.`__shadow_student_0023` LIKE `employees_0000`.`student_0000`;

-- employees_0003, __shadow_student_0024~__shadow_student_0031
CREATE TABLE IF NOT EXISTS `employees_0003`.`__shadow_student_0024` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0003`.`__shadow_student_0025` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0003`.`__shadow_student_0026` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0003`.`__shadow_student_0027` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0003`.`__shadow_student_0028` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0003`.`__shadow_student_0029` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0003`.`__shadow_student_0030` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0003`.`__shadow_student_0031` LIKE `employees_0000`.`student_0000`;

-- employees_0000_r, __shadow_student_0000~__shadow_student_0007
CREATE TABLE IF NOT EXISTS `employees_0000_r`.`__shadow_student_0000` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000_r`.`__shadow_student_0001` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000_r`.`__shadow_student_0002` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000_r`.`__shadow_student_0003` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000_r`.`__shadow_student_0004` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000_r`.`__shadow_student_0005` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000_r`.`__shadow_student_0006` LIKE `employees_0000`.`student_0000`;
CREATE TABLE IF NOT EXISTS `employees_0000_r`.`__shadow_student_0007` LIKE `employees_0000`.`student_0000`;

INSERT INTO employees_0000.student_0001(id,uid,name,score,nickname,gender,birth_year,created_at,modified_at) VALUES (1, 1, 'arana', 95, 'Awesome Arana', 0, 2021, NOW(), NOW());
INSERT INTO employees_0000.__shadow_student_0002(id,uid,name,score,nickname,gender,birth_year,created_at,modified_at) VALUES (2, 2, 'hanmeimei', 97, 'Shadow Arana', 0, 2021, NOW(), NOW());
