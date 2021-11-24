# flink mv

Simple market value calculation of positions as price quotes arrive

## Set up

### Nats.io Setup

Run nats.io

docker run -p 4222:4222 nats -js

Run set up - setup.StreamSetup

### Flink Setup

Download flink and untar it somewhere. Set FLINK_HOME variable, add it to your path, e.g.

```
export FLINK_HOME=/some/path/tools/flink-1.11.4
export PATH=$PATH:$FLINK_HOME/bin
```

Configure for more task slots

* edit $FLINK_HOME/lib/flink-conf.yaml
* set taskmanager.numberOfTaskSlots to 4

Start the cluster

```
start-cluster.sh
```


Console at http://localhost:8081/#/overview



### Deploy the Conflator App

To run the conflator job, set the main class appropriately in the pom then build the jar.

```
<transformers>
    <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
        <mainClass>org.ds.flinkmv.application.QuoteConflator</mainClass>
    </transformer>
</transformers>
```

```
mvn package
```

Run the flink app with parallelism 4

```
flink run -p 4 target/flinkmv-1.0-SNAPSHOT.jar
```

### Misc

Produce quotes - producers.QuotesProducer

Run the conflator (as a stand alone demo) - application.QuoteConflator


Note: In IntelliJ, to run our program, we add the contents of the 
$FLINK_HOME/lib as module dependencies to the project via 
File > ProjectStructure > Modules > Dependencies > Plus