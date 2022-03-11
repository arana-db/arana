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