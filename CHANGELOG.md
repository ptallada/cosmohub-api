# Change Log (http://keepachangelog.com/)
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).


## [Unreleased][unreleased]


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
