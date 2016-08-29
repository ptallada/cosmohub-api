# Change Log (http://keepachangelog.com/)
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).


## [Unreleased][unreleased]
### Added
- Return total number of rows of a catalog. (Pau Tallada)
- Token authentication and authorization. (Pau Tallada)
- Send interactive queries to custom `YARN` queue. (Pau Tallada)
- Transaction emails on register and password reset. (Pau Tallada)
- Generate download links with tokens for files and datasets. (Pau Tallada)
- Initial transactional mail templates. (Pau Tallada)
- Capcha validation on password reset and user registration. (Pau Tallada)

### Changed
- Generate download links with tokens for query results. (Pau Tallada)

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
