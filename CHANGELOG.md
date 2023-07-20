# Changelog

All notable changes to this project will be documented in this file.

## [unreleased]

### Bug Fixes

- Incorrect behavior for intdiv operator (#432)
- #392 (#440)
- Compatibility with mysql8.0 (#450)
- When params is null then return error (#467)
- Config import bug (#475)
- Eval router bug (#478)
- Add writer flag to help admin init (#507)
- Fix cluster/group crud api (#509)
- When args of CONCAT have any NULL, it should return null (#504)
- Add jaeger into docker compose file (#525)
- Missing import functions (#530)
- Admin api bugs (#541)
- Refactor space function. (#577)
- Bugs update tenant in admin api (#587)
- Watch event bug in config center (#589)
- Not exist db (#617)
- Handle database switch correctly (#618)
- Confusing schema with different tenants (#620)
- Fix workflow event spell error (#637)
- Process join ast correctly (#630)
- Handle empty query correctly (#654)
- Connection leak when concurrent updates (#669)
- Select from table not exist cause panic (#693)

### Documentation

- Updated features (#441)
- Add contributors list (#594)

### Features

- Bump parser to v0.2.4 and support multiple stmts hints (#402)
- Add group sequence implementation (#400)
- Filter prefix table name (#413)
- Add analyze table (#409)
- Set variable (#417)
- Upgrade parser to v0.2.5 (#422)
- Add runtime-level config watcher skeleton (#421)
- Redefine object fields for arana-UI (#414)
- Support hint trace (#408)
- Add staticcheck (#423)
- Add skeleton of next-generation mysql function engine (#428)
- Support slow log (#430)
- Solo slow logger (#438)
- Add function ceil and floor (#444)
- Add changelog github workflow action (#451)
- Docker integration with arana ui (#471)
- Support SHOW MASTER STATUS (#462)
- Add tenant&cluster&group config support (#464)
- Add sharding_rule table config support (#480)
- Support_SHOW-REPLICAS (#476)
- Support MySQL math function PI & TRUNCATE (#486)
- Support the 'show processlist' (#492)
- Add CAST function placeholder. (#474)
- Md5 (#497)
- Add sqrt and exp (#498)
- Length (#508)
- Support MySQL SHA1 & SHA function (#510)
- Support MySQL Cast Charset function (#512)
- Add service registry (#431)
- Cast unsign func (#511)
- Support the func concast_ws (#506)
- Sync variables between arana and mysql (#496)
- Support Mysql Upper Function (#515)
- Add table open api (#516)
- Support MySQL Lower function (#526)
- Support show replica status SQL statement. (arana#494) (#523)
- Add mysql right function (#528)
- Support repeat function (#529)
- Support MySQL CHAR_LENGTH function.#501 (#544)
- Support MySQL POWER and ROUND function.#456 (#547)
- Implement new visitor based shard computer (#548)
- Add rand func (#562)
- Support MySQL STRCMP function.  (#550)
- Support MySQL LEFT function (#558)
- Support MySQL RPAD function (#565)
- Add sin func (#561)
- Support kill statement (#575)
- Better proto.Value implementation (#567)
- Integrate admin with jwt (#574)
- Implement basic multiple query (#585)
- Support MySQL CAST_DECIMAL function (#572)
- Add versioned function (#601)
- Support MySQL CAST_TIME/CAST_DATE/CAST_DATETIME function (#606)
- XA transaction basic shelf construction (#579)
- Add support for RENAME TABLE statement (#624)
- Add service discovery api (#623)
- Add like function & filter show tables dataset (#625)
- Add ListAllTables api and replace the password with nil (#636)
- Update better test file for liker function (#638)
- Supplement liker function integration test (#640)
- Update tenants/users (#635)
- Support show users from tenant_name (#655)
- Show nodes from tenant_name (#657)
- Dev hint transform (#682)
- Support show sequence from xxx (#661)
- Add support for REPAIR sql stmt (#680)
- Support show database rule from (#689)
- Auto clean transaction log (#671)
- Multiple sharding keys implementation (#681)

### Miscellaneous Tasks

- Add editorconfig and line lint (#420)
- Trigger chglog on push mastter (#472)
- Add timeout for golangci-lint reviewdog (#473)
- Rename function2 pkg (#521)
- Perf div and float precision (#610)
- Clean arana project log output (#662)
- Add git-cliff configs

### Performance

- Perfect MySQL `create index`  grammar (#584)

### Refactor

- Improve admin router (#407)
- Issue #435 (#439)
- Add standalone config service for admin module (#524)

### Styling

- Use root path (#445)

## [0.1.0-rc1] - 2022-09-01

### Bug Fixes

- License header
- Mysql8.0 handshake failed
- Add license header
- Rename temp variable
- Remove unnecessary package
- Add a empty line
- Idle_timeout unmarshal
- Typo
- Print stack info when log
- Rename kylin to arana
- Replace &bytes.Buffer{} to strings.Builder
- Delete connection from localTransactionMap when commit or rollback
- UnmarshalText
- ComQuery write row not correct
- Github action fail
- Use ssh protocol
- Ci mysql
- Ci env
- Mysql bash -P
- Typo
- Replace vitess atomic helper to uber atomic helper
- Remove sync2/atomic
- Run linter fail
- Fix reviews
- Lint
- Translate comments
- Use correct package style
- When client deprecate eof packet, should write ok packet instead (#41)
- Support multi listener, and update readme.md (#45)
- Point to int (#64)
- Restore function args correctly (#69)
- Client exit ,but do not send com_quit #58 (#79)
- Update github.com/arana-db/parser v0.1.1
- Parse dbname from USE_DATABASE correctly (#100)
- Default not work in parse config (#111)
- Wrong branch name in docker-image workflow (#148)
- Show databases set the column type to varchar (#150)
- Route single shard correctly (#170)
- Upgrade readme. (#191)
- Fix readme dingtalk group image link. (#192)
- Execute union plan correctly (#201)
- Update readme content. (#215)
- #219 (#220)
- (#226)
- Fix drop index need to send each shard (#231)
- #206 #183 show open tables (#208)
- #228 (#232)
- #240 (#245)
- Remove some vitess types (#255)
- Remove some unnecessary ut to resolve CI failure (#270)
- #256 (#269)
- Show index (#275)
- #262 (#271)
- Loop Reconnecting (#335)
- Change sql types and multi tables (#348)
- Shadow_show not exists (#349)
- Show table status from db get rand db, issue#347 (#351)
- Render table_not_found error correctly (#372)

### Documentation

- Add codecov in readme (#85)

### Features

- Add integration test case for ComQuery
- Complete filter mechanism
- Action mysql
- Add basic runtime api skeleton
- Fix format
- Update makefile (#29)
- Change mysql port (#30)
- Support binary protocol from arana to physics db (#33)
- Add basic shard skeleton (#35)
- Add simple optimizer skeleton (#43)
- Add write read split (#46)
- Etcd as configuration center (#61)
- Implement simple sharding over runtime API (#74)
- Rename organization to arana-db (#95)
- Integrate config v2 and implement simple tx backbone (#90)
- Parse more ast (#105)
- Select db by read/write weight in namespace  (#91)
- Implement insert and improve select (#106)
- Implement sharding update (#107)
- Implement delete from sharding table (#118)
- Add testcontainer as integration testing tool (#119)
- Add encode interface in proto row (#117)
- Show databases (#127)
- Shard computer implement by javascript (#141)
- Add show tables (#146)
- Add binary & text encode (#140)
- Drop table (#152)
- Local config file refinement (#156)
- Standalone integration test, require nothing (#159)
- Support non-db login (#161)
- Support transform (#153)
- Merge union to aggregate plan (#180)
- Simple join (#181)
- Alter table statement  (#184)
- Show index (#189)
- Use new dataset api with streaming style (#196)
- Add new shard algorithm. (#195)
- Make configurations as real sharding tables across multiple databases (#209)
- On duplicate key update (#214)
- Support drop index ast (#207)
- Add unset methods for limit ast node (#221)
- Implement group/reduce dataset (#223)
- Support sence in integration test (#227)
- Peekable and parallel dataset (#233)
- Show create table (#244)
- Add orderedDataset (#250)
- Add error log (#251)
- Bump golang to 1.18 (#261)
- Add select order by feature. (#260)
- Drop trigger (#258)
- New logo (#274)
- Create index (#254)
- Implement simple hint backbone (#276)
- Support "show topology from xxx" fix: #249 (#284)
- Expose vconn for runtime and tx (#286)
- Integration_test with yaml config (#282)
- Add develop environment (#293)
- Support show status (#309)
- Max_allowed_packet is set by the user (#312)
- Support "show collation" SQL statement. (#307)
- Enhance weak columns for order-by (#329)
- Hint fullscan (#315)
- Feat: add shadow rule (#303)
- Support route hint (#304)
- Show table status (#333)
- Hidden the arana system table (#337)
- Enhance advance aggregate query (#350)
- Add more feature for create index (#367)
- Show_table_status ast node (#369)
- Add show warnings (#376)
- Use etcd in docker compose by default (#382)
- Implement complex select with orderby (#379)

### Miscellaneous Tasks

- Remove useless import moduler
- Reformat code by imports-formatter
- Tidy mod
- Add pre-commit file (#37)
- Fix typo (#38)
- Fix code license header format. (#108)
- Adjust some actions and remove useless submodules (#109)
- Add auto-build docker image (#147)
- Change port of docker-compose, add local server example (#171)
- Fix some small issue. (#216)
- Fix mod 118 (#265)
- Fix arana dependency order. (#264)
- Listen capabilities fix:#317 (#328)
- Add unit test. (#323)

### Performance

- Adapter V2 config for master slave split (#60)

### Refactor

- Mixin query/exec plans (#55)
- Adjust project structure for docker and command-lines (#144)
- Move optimizers and plans to standalone folder (#294)
- Remove unused filters (#362)

### Release

- Support commit-chglog tool. (#391)
- Add change log for 0.1.0 . (#394)

### Styling

- Ci bash create user,db

<!-- generated by git-cliff -->
