# CosmoHub WebSocket API
This document describes the message-passing interface for all the CosmoHub WebSockets endpoints.

## /sockets/catalog
This service offers the user the ability to perform the following operations on a catalog:
 * __Syntax check__: Check the syntax and retrieve the columns returned by an arbitrary query.
 * __Execute query__: Run a query asynchronously, while receiving periodic progress reports.
 * __Cancel query__: Abort the execution of an in-progress query.

After connecting to the service, the user can submit an operation request using a JSON message. If the message sent is invalid or some unrecoverable error happens during the processing, the connection will be immediately closed. Concurrent execution of multiple operations is **not supported**.

### State Machine
The service is implemented as state machine with 3 different states: `SYNTAX`, `READY` and `RUNNING`. The starting state is `READY`.
The diagram below illustrates the different states and the transitions between them, triggered by the user requests.

```
                           START
                             |
       +----- syntax -----v  v  +----- query ------v       +-----+
+------+                  +--+--+                  +-------+     |
|SYNTAX|                  |READY| <--- cancel ---+ |RUNNING|  progress
+------+                  +-----+                  +-------+     |
       ^----- syntax -----+     ^----- query ------+       ^-----+
```

### Examples
#### Check syntax of a valid query
##### Request
```json
{
    "type" : "syntax",
    "data" : {
        "sql" : "SELECT ra_gal, dec_gal, (sdss_g_true - sdss_r_true) AS gr FROM micecatv2_0_2"
    }
}
```
##### Response
```json
{
    "type" : "syntax",
    "data" : {
        "columns" : [ "ra", "dec", "gr" ]
    }
}
```

#### Check syntax of an invalid query
##### Request
```json
{
    "type" : "syntax",
    "data" : {
        "sql" : "THIS IS NOT A VALID SQL STATEMENT"
    }
}
```
##### Response
```json
{
    "type" : "syntax",
    "error" : {
        "message" : "ParseException line 1:21 cannot recognize input near 'THIS' 'IS' 'NOT' in from source 0"
    }
}
```
#### Execute a query
##### Request
```json
{
    "type" : "query",
    "data" : {
        "sql" : "SELECT ra_gal, dec_gal, (sdss_g_true - sdss_r_true) AS gr FROM micecatv2_0_2"
    }
}
```
##### Progress response
Returns a tuple with 4 integer corresponding to the number of jobs:
 - completed successfully
 - currently running
 - failed attempts
 - pending execution
```json
{
    "type" : "progress",
    "data" : {
        "progress" : [ 25, 5, 2, 50 ]
    }
}
```
##### Response
```json
{
    "type" : "query",
    "data" : {
        "resultset" : {
            "ra_gal" : [ 12.7621, 23.6741, 34.7447 ],
            "dec_gal" : [ 327.663, 112.990, 212.587 ],
            "gr" : [ 1.4527, 0.08729, 1.0298 ]
        }
    }
}
```

#### Cancel a running query
##### Request
```json
{
    "type" : "cancel"
}
```
## /sockets/queries
This service offers continuous updates of the status of the queries.

After establishing the connection, the user will receive periodic status updates for her running queries. Each status update is a list of dictionaries, each one of them contains the query ID and its completed percentage.

##### Periodic response
```json
{
    "type" : "progress",
    "data" : [
        {
            "query_id" : 42,
            "percent_completed" : 85 
        }
    ]
}
```
