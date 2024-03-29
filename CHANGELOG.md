# Change Log (http://keepachangelog.com/)
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).


## [Unreleased][unreleased]


## [2.7.3] - 2024-01-31
### Fixed
 - Disable `hive.merge.tezfiles` for FITS format, as we lack the InputFormat. (Pau Tallada)


## [2.7.2] - 2023-02-28
### Fixed
 - Set reply-to header to sender address in contact form to workaround relay. (Pau Tallada)


## [2.7.1] - 2022-12-22
### Added
 - Add warning note about Parquet download delays. (Pau Tallada)


## [2.7.0] - 2022-01-10
### Added
- Implement Parquet format. (Pau Tallada)
- Patch comments and column names in Parquet format. (Pau Tallada)


## [2.6.1] - 2021-11-17
### Fixed
- Filter out any non-printable characters from FITS header comments. (Pau Tallada)


## [2.6.0] - 2021-01-11
### Changed
- Set collection and key delimiter for complex types in CSV format. (Pau Tallada) 
- Return column types on syntax check. (Pau Tallada)


## [2.5.0] - 2020-06-17
### Added
- Add the templates for the uwsgi and systemd control files. (Pau Tallada)

### Changed
- Adapt to HDP 3.1 services and APIs. (Pau Tallada)


## [2.4.0] - 2020-04-02
### Added
- Assert table access privileges at syntax check using statement AST. (Pau Tallada)


## [2.3.3] - 2020-02-12
### Changed
- Add user info to failed query email template. (Pau Tallada)

### Fixed
- Fix gevent and websockets debugging. (Pau Tallada)

### Removed
- Remove Opbeat component. (Pau Tallada)


## [2.3.2] - 2019-11-18
### Changed
- Update `pyhive` dependency to upstream. (Pau Tallada)
- Replace `psycopg2` dependency for `psycopg2-binary`. (Pau Tallada)

### Fixed
- Gracefully handle when query progress cannot be parsed. (Pau Tallada)
- Reapply 10k row limit for interactive queries. (Pau Tallada)


## [2.3.1] - 2018-05-07
### Fixed
- Fixed statistics for string or multiple partitions. (Pau Tallada)


## [2.3.0] - 2018-02-22
### Added
- Add base template for HTML emails with logo and footer. (Pau Tallada)
- Add check syntax REST endpoint. (Pau Tallada)

### Changed
- Moved old documentation. (Pau Tallada)

### Fixed
- Handle `21000` SQL error as a syntax error. (Pau Tallada)
- Relax locking requirements for `Group` when modifying `ACL`s. (Pau Tallada)
- Fix pending file movement. (Pau Tallada)
- Fix typo in ASDF format documentation. (Pau Tallada)
- Use configured Hive port instead of default. (Pau Tallada)

### Removed
- Remove legacy websockets syntax check call. (Pau Tallada)


## [2.2.3] - 2017-07-26
### Added
- Notify superuser upon catalog failure. (Pau Tallada)
- Show all users in admin view. (Pau Tallada)

### Changed
- Rename email template `user_registered` to `welcome_user`. (Pau Tallada)
- Notify superusers upon new user registration. (Pau Tallada)
- Update hive progress update mechanism. (Pau Tallada)
- Detect half-open connections using `ping`/`pong` messages. (Pau Tallada)

### Fixed
- Fix trigger when resolving ACL. (Pau Tallada)
- Fix ACL email template typo. (Pau Tallada)
- Return resultset in a list to preserve column order. (Pau Tallada)


## [2.2.2] - 2017-05-08
### Added
- Add `DELETED` state to query. (Pau Tallada)

### Fixed
- Manually add 'exp' to token payload to fix `itsdangerous` misbehaviour. (Pau Tallada)


## [2.2.1] - 2016-10-19
### Added
- Validate email address also on password reset. (Pau Tallada)

### Changed
- Tiny change in acknowledgement headers. (Pau Tallada)

### Fixed
- Fix regresion on user register. (Pau Tallada)
- Fix `File` downloads. (Pau Tallada)


## [2.2.0] - 2016-10-17
### Added
- Add `is_private` column to `Group`. (Pau Tallada)
- Notify admins when users request membership. (Pau Tallada)
- Notify users when their memberships have changed. (Pau Tallada)

### Changed
- Simplify `Privilege` system. (Pau Tallada)
- Rename and move `QueryCallback` endpoint. (Pau Tallada)

### Fixed
- Empty lists are not enough for required parameters. (Pau Tallada)
- Do not send `statusdir` parameter to  WebHCat. (Pau Tallada)
- Reorder queries to handle deferred columns. (Pau Tallada)


## [2.1.0] - 2016-10-11
### Added
- Add `citation` and `distribution` fields. (Pau Tallada)
- Add tracking for `/acl` endpoint. (Pau Tallada)

### Fixed
- Handle deferred columns. (Pau Tallada)


## [2.0.0] - 2016-10-05
### Added
- Add `/contact` endpoint. (Pau Tallada)
- Lock rows when sending mails. (Pau Tallada)
- Add comments to CSV, FITS and ASDF headers. (Pau Tallada)
- Integrate with OpBeat. (Pau Tallada)
- Add request tracking for analytics. (Pau Tallada)
- Add `acl` endpoint. (Pau Tallada)
- Notify users on membership changes. (Pau Tallada)

### Changed
- Warn user when resultsets are limited. (Pau Tallada)
- Refactor mail templates. (Pau Tallada)
- Replace websockets implementation. (Pau Tallada)
- Refactor data model. (Pau Tallada)
- Do not send an email upon account activation. (Pau Tallada)

### Fixed
- Notify the user when a query has finished. (Pau Tallada)


## [1.1.0] - 2016-09-05
### Added
- Return total number of rows of a catalog. (Pau Tallada)
- Token authentication and authorization. (Pau Tallada)
- Send interactive queries to custom `YARN` queue. (Pau Tallada)
- Transaction emails on register and password reset. (Pau Tallada)
- Generate download links with tokens for files and datasets. (Pau Tallada)
- Initial transactional mail templates. (Pau Tallada)
- Capcha validation on password reset and user registration. (Pau Tallada)
- Allow cancelling of a running query. (Pau Tallada)

### Changed
- Generate download links with tokens for query results. (Pau Tallada)
- Add `ts_last_updated` with a trigger to ACL table. (Pau Tallada)
- Retrieve redirect URL from frontend in user registration. (Pau Tallada)
- Track new and completed queries on `queries` websocket. (Pau Tallada)
- Refactor all authentication code. (Pau Tallada)

### Fixed
- Return partitioned table statistics. (Pau Tallada)
- Store query start and finish time in UTC. (Pau Tallada)
- Return partitions as virtual columns. (Pau Tallada)
- Handle only failed jobs in hive progress. (Pau Tallada)
- Authenticate web sockets connections. (Pau Tallada)


## [1.0.0] - 2016-08-19
### Added
- Implement a new data model. (Pau Tallada)
- Enhance password handling using `passwlib`. (Pau Tallada)

### Fixed
- Speed up downloads using prefetching greenlets. (Pau Tallada)


## [0.1.0] - 2016-06-17
### Added
- Implement a prototype API. (Pau Tallada)
