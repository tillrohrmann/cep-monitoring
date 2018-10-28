# cep-monitoring
Apache Flink CEP program to monitor rack temperatures in a data center

The Flink program monitors an incoming stream of monitor events from a data center.
The input stream contains events about the temperature and power consumption of the individual racks.
Whenever two temperature events occur within a given interval which exceed a certain threshold temperature, a warning will be raised.
If the system should detect two temperature warnings for the same rack and with increasing temperatures, the system will generate an alert for this rack.