# flink mv

Simple market value calculation of positions as price quotes arrive

## Set up

Run nats.io

docker run -p 4222:4222 nats -js

Run set up - setup.StreamSetup

Produce quotes - producers.QuotesProducer

Run the conflator (as a stand alone demo) - application.QuoteConflator


Note: In IntelliJ, to run our program, we add the contents of the 
$FLINK_HOME/lib as module dependencies to the project via 
File > ProjectStructure > Modules > Dependencies > Plus