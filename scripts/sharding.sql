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

CREATE DATABASE IF NOT EXISTS employees;
USE employees;

DELIMITER //
CREATE PROCEDURE sp_create_tab()
BEGIN
    SET @str = ' (
`id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
`uid` BIGINT(20) UNSIGNED NOT NULL,
`name` VARCHAR(255) NOT NULL,
`score` DECIMAL(6,2) DEFAULT ''0'',
`nickname` VARCHAR(255) DEFAULT NULL,
`gender` TINYINT(4) NULL,
`birth_year` SMALLINT(5) UNSIGNED DEFAULT ''0'',
`created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
`modified_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (`id`),
UNIQUE KEY `uk_uid` (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
';

    SET @j = 0;
    WHILE @j < 32
        DO
            SET @table = CONCAT('student_', LPAD(@j, 4, '0'));
            SET @ddl = CONCAT('CREATE TABLE IF NOT EXISTS ', @table, @str);
            PREPARE ddl FROM @ddl;
            EXECUTE ddl;
            SET @j = @j + 1;
        END WHILE;
END
//

DELIMITER ;
CALL sp_create_tab;
DROP PROCEDURE sp_create_tab;

insert into student_0001 values (1, 1, 'scott', 95, 'nc_scott', 0, 16, now(), now());
