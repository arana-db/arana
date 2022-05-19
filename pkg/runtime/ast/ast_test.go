/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ast

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	var (
		stmt Statement
		err  error
	)

	for _, sql := range []string{
		"select connection_id()",
		"select 1",
		"select * from student as foo where `name` = if(1>2, 1, 2)",
		"select * from employees limit 1",
		`SELECT CONCAT("'", user, "'@'",host,"'") FROM mysql.user`,
		"select * from student where uid = abs(-11)",
		"select * from student where uid = 1 limit 3 offset ?",
		"select case count(*) when 0 then -3.14 else 2.17 end as xxx from student where uid in (-1,-2,-3)",
		"select * from tb_user a where (uid >= ? AND uid <= ?)",
		"SELECT (2021 - birth_year) as AGE, count(1) as amount from student where uid between 1 and 10 group by (2021-birth_year)",
		"select * from student where uid = !0",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err = Parse(sql)
			assert.NoError(t, err)
			t.Log("stmt:", stmt)
		})
	}

	// 1. select statement
	stmt, err = Parse("select * from student as foo where `name` = if(1>2, 1, 2) order by age")
	assert.NoError(t, err, "parse+conv ast failed")
	t.Logf("stmt:%+v", stmt)

	// 2. delete statement
	deleteStmt, err := Parse("delete from student as foo where `name` = if(1>2, 1, 2)")
	assert.NoError(t, err, "parse+conv ast failed")
	t.Logf("stmt:%+v", deleteStmt)

	// 3. insert statements
	insertStmtWithSetClause, err := Parse("insert into sink set a=77, b='88'")
	assert.NoError(t, err, "parse+conv ast failed")
	t.Logf("stmt:%+v", insertStmtWithSetClause)

	insertStmtWithValues, err := Parse("insert into sink values(1, '2')")
	assert.NoError(t, err, "parse+conv ast failed")
	t.Logf("stmt:%+v", insertStmtWithValues)

	insertStmtWithOnDuplicateUpdates, err := Parse(
		"insert into sink (a, b) values(1, '2') on duplicate key update a=a+1",
	)
	assert.NoError(t, err, "parse+conv ast failed")
	t.Logf("stmt:%+v", insertStmtWithOnDuplicateUpdates)

	// 4. update statement
	updateStmt, err := Parse(
		"update source set a=a+1, b=b+2 where a>1 order by a limit 5",
	)
	assert.NoError(t, err, "parse+conv ast failed")
	t.Logf("stmt:%+v", updateStmt)
}

func TestParse_UnionStmt(t *testing.T) {
	type tt struct {
		input  string
		expect string
	}

	for _, next := range []tt{
		{"select 1 union select 2", "SELECT 1 UNION SELECT 2"},
		{"select 1 union distinct select 2", "SELECT 1 UNION SELECT 2"},
		{"select 1 union all select 2", "SELECT 1 UNION ALL SELECT 2"},
		{"select id,uid,name,nickname from student where uid in (?,?,?) union all select id,uid,name,nickname from tb_user where uid in (?,?,?)", "SELECT `id`,`uid`,`name`,`nickname` FROM `student` WHERE `uid` IN (?,?,?) UNION ALL SELECT `id`,`uid`,`name`,`nickname` FROM `tb_user` WHERE `uid` IN (?,?,?)"},
	} {
		t.Run(next.input, func(t *testing.T) {
			stmt, err := Parse(next.input)
			assert.NoError(t, err, "should parse ok")
			assert.IsType(t, (*UnionSelectStatement)(nil), stmt, "should be union statement")

			actual, err := RestoreToString(RestoreDefault, stmt.(Restorer))
			assert.NoError(t, err, "should restore ok")
			assert.Equal(t, next.expect, actual)
		})

	}

}

func TestParse_SelectStmt(t *testing.T) {
	type tt struct {
		input  string
		expect string
	}

	for _, next := range []tt{
		{"select * from a left join b on a.k = b.k", "SELECT * FROM `a` LEFT JOIN `b` ON `a`.`k` = `b`.`k`"},
		{"select * from foo as a left join bar as b on a.k = b.k", "SELECT * FROM `foo` AS `a` LEFT JOIN `bar` AS `b` ON `a`.`k` = `b`.`k`"},
		{"select @@version", "SELECT @@version"},
		{"select * from student for update", "SELECT * FROM `student` FOR UPDATE"},
		{"select connection_id()", "SELECT CONNECTION_ID()"},
		{`SELECT CONCAT("'", user, "'@'",host,"'") FROM mysql.user`, "SELECT CONCAT('\\'',`user`,'\\'@\\'',`host`,'\\'') FROM `mysql`.`user`"},
		{"select * from student where uid = abs(-11)", "SELECT * FROM `student` WHERE `uid` = ABS(-11)"},
		{"select * from student where uid = 1 limit 3 offset ?", "SELECT * FROM `student` WHERE `uid` = 1 LIMIT ?,3"},
		//{"select case count(*) when 0 then -3.14 else 2.17 end as xxx from student where uid in (-1,-2,-3)", "SELECT CASE COUNT(*) WHEN 0 THEN -3.14 ELSE 2.17 END AS `xxx` FROM `student` WHERE `uid` IN (-1,-2,-3)"},
		{"select * from tb_user a where (uid >= ? AND uid <= ?)", "SELECT * FROM `tb_user` AS `a` WHERE (`uid` >= ? AND `uid` <= ?)"},
		{"SELECT (2021 - birth_year) as AGE, count(1) as amount from student where uid between 1 and 10 group by (2021-birth_year)", "SELECT (2021-`birth_year`) AS `AGE`,COUNT(1) AS `amount` FROM `student` WHERE `uid` BETWEEN 1 AND 10 GROUP BY (2021-`birth_year`)"},
		{"select * from student where uid = !0", "SELECT * FROM `student` WHERE `uid` = !0"},
		{"select convert(col using 'utf8')", "SELECT CONVERT(`col` USING utf8)"},
		{"select convert('foo' using utf8mb4)", "SELECT CONVERT('foo' USING utf8mb4)"},
		{"select convert(3.14,signed)", "SELECT CONVERT(3.14, SIGNED)"},
		{"select cast(3.14 as signed)", "SELECT CAST(3.14 AS SIGNED)"},
		{"select cast(3.14 as decimal(6,2))", "SELECT CAST(3.14 AS DECIMAL(6,2))"},
		{"select cast(3.14 as char(6))", "SELECT CAST(3.14 AS CHAR(6))"},
		//{"select cast('foo' as nchar(1))", "SELECT CAST('foo' AS NCHAR(1))"},
		{"select * from student force index(uk_uid) where uid in (1,2,3)", "SELECT * FROM `student` FORCE INDEX(`uk_uid`) WHERE `uid` IN (1,2,3)"},
		{"select * from student PARTITION (foo,bar) as foobar", "SELECT * FROM `student` PARTITION (`foo`,`bar`) AS `foobar`"},
		{"select IF(sum(gender),1,0)+1 as xy from tb_user where uid in (7777, 10099) or uid between 10000 and 10004", "SELECT IF(SUM(`gender`),1,0)+1 AS `xy` FROM `tb_user` WHERE `uid` IN (7777,10099) OR `uid` BETWEEN 10000 AND 10004"},
		{"select * from tb_user where uid is not null and uid = 10001", "SELECT * FROM `tb_user` WHERE `uid` IS NOT NULL AND `uid` = 10001"},
		{"select * from student where uid = case when 2>1 then ? end", "SELECT * FROM `student` WHERE `uid` = CASE WHEN 2 > 1 THEN ? END"},
		{"select * from student where uid = case when 2<>2 then ? end", "SELECT * FROM `student` WHERE `uid` = CASE WHEN 2 <> 2 THEN ? END"},
		{"select * from student where uid = case when 1=2 then 1 else ? end", "SELECT * FROM `student` WHERE `uid` = CASE WHEN 1 = 2 THEN 1 ELSE ? END"},
		{"select * from student where uid = case when 1=2 then 1 when 1=1 then 33 else 31 end", "SELECT * FROM `student` WHERE `uid` = CASE WHEN 1 = 2 THEN 1 WHEN 1 = 1 THEN 33 ELSE 31 END"},
		{"select * from student where uid = ABS(case when IF(1=2,true,false) then 1 else ? end)", "SELECT * FROM `student` WHERE `uid` = ABS(CASE WHEN IF(1 = 2,1,0) THEN 1 ELSE ? END)"}, // FIXME: use true/false instead of 1/0
		{"select * from student where uid = ABS(1-1+(case when IF(1=?,2,1)-1 then 1 else ? end))", "SELECT * FROM `student` WHERE `uid` = ABS(1-1+(CASE WHEN IF(1 = ?,2,1)-1 THEN 1 ELSE ? END))"},
		{"select * from student where uid = case (4%5) when 1 then 1 when 4 then ? else 0 end", "SELECT * FROM `student` WHERE `uid` = CASE (4%5) WHEN 1 THEN 1 WHEN 4 THEN ? ELSE 0 END"},
		//{"select birth_year,gender,count(*) as cnt from student where uid between 1 and 100 group by birth_year,gender having count(*)>5", "SELECT `birth_year`,`gender`,COUNT(*) AS `cnt` FROM `student` WHERE `uid` BETWEEN 1 AND 100 GROUP BY `birth_year`,`gender` HAVING COUNT(*) > 5"},
		{`select * from (select id,uid from student where uid in(1,?,?)) as aaa`, "SELECT * FROM (SELECT `id`,`uid` FROM `student` WHERE `uid` IN (1,?,?)) AS `aaa`"},
		//{"select count(*) from student where aaa.uid = 1", "SELECT COUNT(*) FROM `student` WHERE `aaa`.`uid` = 1"},
		{`select * from (select id,uid from student where uid in(1,2,3) union all select id,uid from student where uid in (?,?)) as aaa where aaa.uid=?`, "SELECT * FROM (SELECT `id`,`uid` FROM `student` WHERE `uid` IN (1,2,3) UNION ALL SELECT `id`,`uid` FROM `student` WHERE `uid` IN (?,?)) AS `aaa` WHERE `aaa`.`uid` = ?"},
		{"select * from student where not uid = 1", "SELECT * FROM `student` WHERE not `uid` = 1"},
		{"select * from student where name not regexp '^Ch+'", "SELECT * FROM `student` WHERE `name` NOT REGEXP '^Ch+'"},
		{"select date_add(NOW(), interval 1 hour)", "SELECT DATE_ADD(NOW(),INTERVAL 1 HOUR)"},
		{"select distinct gender from student where uid in (1,2,3,4)", "SELECT DISTINCT `gender` FROM `student` WHERE `uid` IN (1,2,3,4)"},
		{"select distinct(gender) from student where uid in (1,2,3,4)", "SELECT DISTINCT (`gender`) FROM `student` WHERE `uid` IN (1,2,3,4)"},
		{"select * from foo inner join bar on foo.x = bar.y", "SELECT * FROM `foo` INNER JOIN `bar` ON `foo`.`x` = `bar`.`y`"},
		{"select * from foo left outer join bar on foo.x = bar.y", "SELECT * FROM `foo` LEFT JOIN `bar` ON `foo`.`x` = `bar`.`y`"},
		{"select null as pkid", "SELECT NULL AS `pkid`"},
	} {
		t.Run(next.input, func(t *testing.T) {
			stmt, err := Parse(next.input)
			assert.NoError(t, err, "should parse ok")
			assert.IsType(t, (*SelectStatement)(nil), stmt, "should be select statement")

			actual, err := RestoreToString(RestoreDefault, stmt.(Restorer))
			assert.NoError(t, err, "should restore ok")
			assert.Equal(t, next.expect, actual)
		})
	}

}

func TestParse_DeleteStmt(t *testing.T) {
	type tt struct {
		input  string
		expect string
	}

	for _, it := range []tt{
		{"delete from student where id = 1 limit 1", "DELETE FROM `student` WHERE `id` = 1 LIMIT 1"},
		{"delete low_priority quick ignore from student where id = 1", "DELETE LOW_PRIORITY QUICK IGNORE FROM `student` WHERE `id` = 1"},
	} {
		t.Run(it.input, func(t *testing.T) {
			stmt, err := Parse(it.input)
			assert.NoError(t, err)
			assert.IsType(t, (*DeleteStatement)(nil), stmt, "should be delete statement")

			actual, err := RestoreToString(RestoreDefault, stmt.(Restorer))
			assert.NoError(t, err, "should restore ok")
			assert.Equal(t, it.expect, actual)
		})
	}
}

func TestParse_DescribeStatement(t *testing.T) {
	type tt struct {
		input  string
		expect string
	}

	for _, it := range []tt{
		{"desc foobar", "DESC `foobar`"},
	} {
		t.Run(it.input, func(t *testing.T) {
			stmt, err := Parse(it.input)
			assert.NoError(t, err)
			assert.IsType(t, (*DescribeStatement)(nil), stmt, "should be describe statement")

			actual, err := RestoreToString(RestoreDefault, stmt.(Restorer))
			assert.NoError(t, err, "should restore ok")
			assert.Equal(t, it.expect, actual)
		})
	}
}

func TestParse_ShowStatement(t *testing.T) {
	type tt struct {
		input     string
		expectTyp interface{}
		expect    string
	}

	for _, it := range []tt{
		{"show databases", (*ShowDatabases)(nil), "SHOW DATABASES"},
		{"show databases like '%foo%'", (*ShowDatabases)(nil), "SHOW DATABASES LIKE '%foo%'"},
		{"show databases where name = 'foobar'", (*ShowDatabases)(nil), "SHOW DATABASES WHERE `name` = 'foobar'"},
		{"show tables", (*ShowTables)(nil), "SHOW TABLES"},
		{"show tables like '%foo%'", (*ShowTables)(nil), "SHOW TABLES LIKE '%foo%'"},
		{"show tables where name = 'foo'", (*ShowTables)(nil), "SHOW TABLES WHERE `name` = 'foo'"},
		{"sHow indexes from foo", (*ShowIndex)(nil), "SHOW INDEXES FROM `foo`"},
		{"show columns from foo", (*ShowColumns)(nil), "SHOW COLUMNS FROM `foo`"},
		{"sHoW full columns from foo", (*ShowColumns)(nil), "SHOW FULL COLUMNS FROM `foo`"},
		{"show extended full columns from foo", (*ShowColumns)(nil), "SHOW EXTENDED FULL COLUMNS FROM `foo`"},
		{"show create table `foo`", (*ShowCreate)(nil), "SHOW CREATE TABLE `foo`"},
	} {
		t.Run(it.input, func(t *testing.T) {
			stmt, err := Parse(it.input)
			assert.NoError(t, err)
			assert.IsTypef(t, it.expectTyp, stmt, "should be %T", it.expectTyp)

			actual, err := RestoreToString(RestoreDefault, stmt.(Restorer))
			assert.NoError(t, err, "should restore ok")
			assert.Equal(t, it.expect, actual)
		})
	}

}

func TestParse_ExplainStmt(t *testing.T) {
	stmt, err := Parse("explain select * from student where uid = 1")
	assert.NoError(t, err)
	assert.IsType(t, (*ExplainStatement)(nil), stmt)
	s := MustRestoreToString(RestoreDefault, stmt)
	assert.Equal(t, "EXPLAIN SELECT * FROM `student` WHERE `uid` = 1", s)
}

func TestParseMore(t *testing.T) {
	tbls := []string{
		// check convert and cast literal
		"show tables",
		"show tables like '%student%'",
		"show databases",
		"show create table student",
		"show indexes from student",
		"show indexes from student where Column_name='a'",
		"show full columns from student",
		"show full columns from student like 'PRI'",

		"explain select * from student where uid = 1",
		"explain delete from student where uid = 1",
		"explain insert into student(id,uid,name) values(1,1,'fake_name')",

		"insert ignore into `fake_db`.`tb_user`(uid,nickname) values(?,?),(?,?),(?,?)",
		"insert LOW_PRIORITY into `tb_user` set gender=0,nickname = ?, uid = ?, name = 'foobar' on duplicate key update gender=gender+1,gmt_create=now(),gmt_modified=now()",
		"REPLACE INTO student(uid,name,nickname,gender,birth_year) VALUES (33,'fake_name_127_33','nickname_127_33',1,1990),(44,'fake_name_127_44','nickname_127_44',1,1990)",

		"desc student",
		"describe student",
		"explain student",
		"explain select 1",
	}

	for _, sql := range tbls {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			assert.NoError(t, err)
		})
	}
}

func TestParse_UpdateStmt(t *testing.T) {
	type tt struct {
		input  string
		expect string
	}

	for _, it := range []tt{
		{"update `student` set version=version+1,modified_at=NOW() where id = 1", "UPDATE `student` SET `version` = `version`+1, `modified_at` = NOW() WHERE `id` = 1"},
		{"update low_priority student set nickname = ? where id = 1 limit 1", "UPDATE LOW_PRIORITY `student` SET `nickname` = ? WHERE `id` = 1 LIMIT 1"},
	} {
		t.Run(it.input, func(t *testing.T) {
			stmt, err := Parse(it.input)
			assert.NoError(t, err)
			assert.IsTypef(t, (*UpdateStatement)(nil), stmt, "should be update statement")

			actual, err := RestoreToString(RestoreDefault, stmt.(Restorer))
			assert.NoError(t, err, "should restore ok")
			assert.Equal(t, it.expect, actual)
		})

	}
}

func TestParse_InsertStmt(t *testing.T) {
	type tt struct {
		input  string
		expect string
	}

	for _, it := range []tt{
		{"insert into student value (?,?)", "INSERT INTO `student` VALUES (?, ?)"},
		{
			"insert into student set id=1,name='foo'",
			"INSERT INTO `student` SET `id` = 1, `name` = 'foo'",
		},
		{
			"iNsErt into student(id,name) values(?,?),(?,?)",
			"INSERT INTO `student`(`id`, `name`) VALUES (?, ?),(?, ?)",
		},
		{
			"insert into student(id,name) values(1,'foo'),(2,'bar') on duplicate key update version=version+1,modified_at=NOW()",
			"INSERT INTO `student`(`id`, `name`) VALUES (1, 'foo'),(2, 'bar') ON DUPLICATE KEY UPDATE `version` = `version`+1, `modified_at` = NOW()",
		},
	} {
		t.Run(it.input, func(t *testing.T) {
			stmt, err := Parse(it.input)
			assert.NoError(t, err)
			assert.IsTypef(t, (*InsertStatement)(nil), stmt, "should be insert statement")

			actual, err := RestoreToString(RestoreDefault, stmt.(Restorer))
			assert.NoError(t, err, "should restore ok")
			assert.Equal(t, it.expect, actual)
		})
	}

}

func TestRestoreCount(t *testing.T) {
	stmt := MustParse("select count(1)")
	sel := stmt.(*SelectStatement)
	var sb strings.Builder
	_ = sel.Restore(RestoreDefault, &sb, nil)
	assert.Equal(t, "SELECT COUNT(1)", sb.String())
}

func TestQuote(t *testing.T) {
	stmt := MustParse("select `a``bc`")
	sel := stmt.(*SelectStatement)
	var sb strings.Builder
	_ = sel.Restore(RestoreDefault, &sb, nil)
	t.Log(sb.String())
	assert.Equal(t, "SELECT `a``bc`", sb.String())
}

func TestParse_AlterTableStmt(t *testing.T) {
	type tt struct {
		input  string
		expect string
	}

	for _, it := range []tt{
		{
			"alter table student drop nickname",
			"ALTER TABLE `student` DROP COLUMN `nickname`",
		},
		{
			"alter table student add dept_id int not null default 0 after uid",
			"ALTER TABLE `student` ADD COLUMN `dept_id` INT(11) NOT NULL DEFAULT 0 AFTER `uid`",
		},
		{
			"alter table student add index idx_name (name)",
			"ALTER TABLE `student` ADD INDEX idx_name(`name`)",
		},
		{
			"alter table student change id uid bigint not null",
			"ALTER TABLE `student` CHANGE COLUMN `id` `uid` BIGINT(20) NOT NULL",
		},
		{
			"alter table student modify uid bigint not null default 0",
			"ALTER TABLE `student` MODIFY COLUMN `uid` BIGINT(20) NOT NULL DEFAULT 0",
		},
		{
			"alter table student rename to students",
			"ALTER TABLE `student` RENAME AS `students`",
		},
		{
			"alter table student rename column name to nickname, rename column nickname to name",
			"ALTER TABLE `student` RENAME COLUMN `name` TO `nickname`, RENAME COLUMN `nickname` TO `name`",
		},
	} {
		t.Run(it.input, func(t *testing.T) {
			stmt, err := Parse(it.input)
			assert.NoError(t, err)
			assert.IsTypef(t, (*AlterTableStatement)(nil), stmt, "should be alter table statement")

			actual, err := RestoreToString(RestoreDefault, stmt.(Restorer))
			assert.NoError(t, err, "should restore ok")
			assert.Equal(t, it.expect, actual)
		})
	}

}
