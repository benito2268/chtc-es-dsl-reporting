## CHTC Elasticsearch DSL Reporting

### Install Required Packages
* `pip install elasticsearch<8 elasticsearch_dsl<8 tabulate`

### Command Line Options
* `-s / --start <YYYY-MM-DD>` Report from this date forward
* `-e / --end <YYYY-MM-DD>` Report until this date (defaults to now)
* `-h / --host <hostname>` Elasticsearch server hostname (defaults to `http://localhost`)
* `-p / --port <port>` Elasticsearch server port number (defaults to 9200)
* `-i / --index <index_name>` Elasticsearch index to search from (defaults to `chtc-schedd-*`)
