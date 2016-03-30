/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.stsffap.cep.monitoring;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema;
import org.apache.flink.util.Collector;
import org.stsffap.cep.monitoring.sources.MonitoringEventSource;
import org.stsffap.cep.monitoring.types.MonitoringEvent;
import org.stsffap.cep.monitoring.types.TemperatueEvent;
import org.stsffap.cep.monitoring.types.TemperatureAlert;
import org.stsffap.cep.monitoring.types.TemperatureWarning;

import java.util.Map;
import java.util.Properties;

public class CEPMonitoring {
    private static final double TEMPERATURE_THRESHOLD = 100;

    public static void main(String[] args) throws Exception {
        final boolean useKafka = false;
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        TypeInformation<MonitoringEvent> monitorEventTypeInformation = TypeExtractor.getForClass(MonitoringEvent.class);

        final DeserializationSchema<MonitoringEvent> deserializationSchema = new TypeInformationSerializationSchema<MonitoringEvent>(
                monitorEventTypeInformation,
                env.getConfig());

        DataStream<MonitoringEvent> inputEventStream;

        if (useKafka) {
            final String topic = parameterTool.get("topic");
            final Properties properties = new Properties();

            properties.setProperty("bootstrap.servers", parameterTool.get("bootstrap.servers"));
            properties.setProperty("group.id", parameterTool.get("group.id"));

            inputEventStream = env
                    .addSource(
                        new FlinkKafkaConsumer09<>(
                                topic,
                                deserializationSchema,
                                properties
                        )
                    )
                    .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());
        } else {
            inputEventStream = env
                    .addSource(new MonitoringEventSource())
                    .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());
        }

        Pattern<MonitoringEvent, ?> warningPattern = Pattern.<MonitoringEvent>begin("first")
                .subtype(TemperatueEvent.class)
                .where(temperatureEvent -> temperatureEvent.getTemperature() >= TEMPERATURE_THRESHOLD)
                .next("second")
                .subtype(TemperatueEvent.class)
                .where(temperatueEvent -> temperatueEvent.getTemperature() >= TEMPERATURE_THRESHOLD)
                .within(Time.seconds(10));

        PatternStream<MonitoringEvent> tempPatternStream = CEP.pattern(
                inputEventStream.keyBy("rackID"),
                warningPattern);

        DataStream<TemperatureWarning> warnings = tempPatternStream.select(
            new PatternSelectFunction<MonitoringEvent, TemperatureWarning>() {
                @Override
                public TemperatureWarning select(Map<String, MonitoringEvent> map) throws Exception {
                    TemperatueEvent first = (TemperatueEvent) map.get("first");
                    TemperatueEvent second = (TemperatueEvent) map.get("second");

                    return new TemperatureWarning(first.getRackID(), (first.getTemperature() + second.getTemperature()) / 2);
                }
            }
        );

        warnings.print();

        Pattern<TemperatureWarning, ?> alertPattern = Pattern.<TemperatureWarning>begin("first")
                .followedBy("second")
                .within(Time.seconds(100));

        PatternStream<TemperatureWarning> alertPatternStream = CEP.pattern(
                warnings.keyBy("rackID"),
                alertPattern);

        DataStream<TemperatureAlert> alerts = alertPatternStream.flatSelect(new PatternFlatSelectFunction<TemperatureWarning, TemperatureAlert>() {
            @Override
            public void flatSelect(Map<String, TemperatureWarning> pattern, Collector<TemperatureAlert> out) throws Exception {
                TemperatureWarning first = pattern.get("first");
                TemperatureWarning second = pattern.get("second");

                if (first.getAverageTemperature() < second.getAverageTemperature()) {
                    out.collect(new TemperatureAlert(first.getRackID()));
                }
            }
        });

        alerts.print();

        env.execute("CEP monitoring job");
    }
}
