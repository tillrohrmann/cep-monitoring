# cep-monitoring
Apache Flink CEP program to monitor rack temperatures in a data center

The Flink program monitors an incoming stream of monitor events from a data center.
The input stream contains events about the temperature of the individual racks.
Whenever two temperature events occur within a given interval which exceed a certain threshold temperature, a warning will be raised.
If the system should detect three temperature events which all exceed the threshold temperature and where the last event has a temperature which is 1.2 times higher than the first event, a critical alert will be generated.

The repository consists of two modules, `data-generator` and `monitoring`.
The former module contains a Flink program to generate the monitoring event stream and writing it to Kafka.
The latter module contains the CEP program which monitors the given event stream for racks with a problematic temperatures.
