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

package org.stsffap.cep.monitoring.dataGenerator;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema;
import types.MonitoringEvent;

public class DataGenerator {
    private static final int MAX_RACK_ID = 1000;
    private static final long PAUSE = 100;
    private static final double TEMPERATURE_RATIO = 0.5;
    private static final double POWER_STD = 10;
    private static final double POWER_MEAN = 100;
    private static final double TEMP_STD = 20;
    private static final double TEMP_MEAN = 80;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        final String brokerList = parameterTool.get("brokerList");
        final String topicId = parameterTool.get("topicId");

        DataStream<MonitoringEvent> inputEventStream = env.addSource(new MonitoringEventSource(
                MAX_RACK_ID,
                PAUSE,
                TEMPERATURE_RATIO,
                POWER_STD,
                POWER_MEAN,
                TEMP_STD,
                TEMP_MEAN
        ));

        TypeInformation<MonitoringEvent> monitoringEventTypeInformation = TypeExtractor.getForClass(MonitoringEvent.class);

        inputEventStream.addSink(new FlinkKafkaProducer09<MonitoringEvent>(
                brokerList,
                topicId,
                new TypeInformationSerializationSchema<MonitoringEvent>(monitoringEventTypeInformation, env.getConfig())
        ));

        env.execute("CEP DataGenerator");
    }
}
