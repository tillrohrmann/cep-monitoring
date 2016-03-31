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

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.stsffap.cep.monitoring.sources.MonitoringEventSource;
import org.stsffap.cep.monitoring.types.MonitoringEvent;
import org.stsffap.cep.monitoring.types.TemperatueEvent;
import org.stsffap.cep.monitoring.types.TemperatureAlert;
import org.stsffap.cep.monitoring.types.TemperatureWarning;

public class CEPMonitoring {
    private static final double TEMPERATURE_THRESHOLD = 100;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<MonitoringEvent> inputEventStream = env
                .addSource(new MonitoringEventSource())
                .assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

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
            map -> {
                TemperatueEvent first = (TemperatueEvent) map.get("first");
                TemperatueEvent second = (TemperatueEvent) map.get("second");

                return new TemperatureWarning(first.getRackID(), (first.getTemperature() + second.getTemperature()) / 2);
            }
        );

        warnings.print();

        Pattern<TemperatureWarning, ?> alertPattern = Pattern.<TemperatureWarning>begin("first")
                .next("second")
                .within(Time.seconds(20));

        PatternStream<TemperatureWarning> alertPatternStream = CEP.pattern(
                warnings.keyBy("rackID"),
                alertPattern);

        DataStream<TemperatureAlert> alerts = alertPatternStream.flatSelect(
            (pattern, out) -> {
                TemperatureWarning first = pattern.get("first");
                TemperatureWarning second = pattern.get("second");

                if (first.getAverageTemperature() < second.getAverageTemperature()) {
                    out.collect(new TemperatureAlert(first.getRackID()));
                }
            });

        alerts.print();

        env.execute("CEP monitoring job");
    }
}
