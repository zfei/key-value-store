# Key Value Store
An in-RAM Cassandra-like distributed key-value store with replicas and consistency level support.

## Usage
### Configuration
`log4j.properties` and `settings.json` should both appear in `dist/conf` folder, the first one is log4j config file, and the second one is for the standalone server configuration. Here is a sample server config file:
```json
{
    "numReplicas": 3,
    "servers": [
        {
            "host": "localhost",
            "port": 50023,
            "delays": [50, 5000, 1000, 2000]
        },
        {
            "host": "localhost",
            "port": 50002,
            "delays": [5000, 50, 1000, 1000]
        },
        {
            "host": "localhost",
            "port": 50003,
            "delays": [6000, 1000, 50, 2000]
        },
        {
            "host": "localhost",
            "port": 50004,
            "delays": [3000, 1000, 99000, 50]
        }
    ]
}
```
### Execution
After configuration, execute the following to bring up 4 standalone servers:
```bash
cd dist
java -Dlog4j.configuration="conf/log4j.properties" -cp kvstore-standalone.jar me.zfei.kvstore.Standalone SERVER_INDEX
```
If we are to use the sample configuration, we'll just need to execute the following:
```bash
cd dist
java -Dlog4j.configuration="conf/log4j.properties" -cp kvstore-standalone.jar me.zfei.kvstore.Standalone 0
java -Dlog4j.configuration="conf/log4j.properties" -cp kvstore-standalone.jar me.zfei.kvstore.Standalone 1
java -Dlog4j.configuration="conf/log4j.properties" -cp kvstore-standalone.jar me.zfei.kvstore.Standalone 2
java -Dlog4j.configuration="conf/log4j.properties" -cp kvstore-standalone.jar me.zfei.kvstore.Standalone 3
```
each in a separate terminal.

Each standalone instance acts as both client and server. You can interact with each of them to bring change to the entire cluster.

## Commands
There are four types of commands `delete`, `get`, `insert` and `update`, and the format is as follows:
```bash
delete Key # delete the Key from all replicas
get Key Level # get the value for the Key, using specified consistency level 
insert Key Value Level # add a new key
update Key Value Level # modify
```
Key and Value have to be String, and Level is either `ONE` or `ALL`.

Additionally, there are two utilities:
```bash
show-all # list all key-value pairs on the standalone datastore
search Key # list specific key-value pairs across the cluster
```

## Operations & Implementation
### P2P Network
All nodes know each other by bootstrapping from the config file. After that, they will build a Distributed Hash Table with a Consistent Hashing Function to determine whether to store/lookup a specific entry.
### Replicas
Key value pairs will be replicated in several nodes, the number of which can be configured in the config file.
### Consistency Level
When consistency level is set to `ONE`, client returns as long as one node responds; when it's set to `ALL`, the client has to wait till all nodes respond to the query.
### Read repair with last-write-win strategy
Each time when `get` is invoked, the client will spawn another thread in background to check whether all responding nodes have the same result. If not, the latest result will be sent to other nodes to overwrite conflicting values.
### Randomized delay
Delays between nodes are independently configurable in the config file. It's uniformly distributed between 0 to 2 * avgDelay.