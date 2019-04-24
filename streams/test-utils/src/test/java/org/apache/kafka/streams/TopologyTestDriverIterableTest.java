/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TopologyTestDriverIterableTest {
    private final static String SOURCE_TOPIC_1 = "source-topic-1";
    private final static String SOURCE_TOPIC_2 = "source-topic-2";
    private final static String SINK_TOPIC_1 = "sink-topic-1";
    private final static String SINK_TOPIC_2 = "sink-topic-2";

    private final ConsumerRecordFactory<byte[], byte[]> consumerRecordFactory = new ConsumerRecordFactory<>(
        new ByteArraySerializer(),
        new ByteArraySerializer());

    private final Headers headers = new RecordHeaders(new Header[]{new RecordHeader("key", "value".getBytes())});

    private final byte[] key1 = new byte[0];
    private final byte[] value1 = new byte[0];
    private final long timestamp1 = 42L;
    private final ConsumerRecord<byte[], byte[]> consumerRecord1 = consumerRecordFactory.create(SOURCE_TOPIC_1, key1, value1, headers, timestamp1);

    private final byte[] key2 = new byte[0];
    private final byte[] value2 = new byte[0];
    private final long timestamp2 = 43L;
    private final ConsumerRecord<byte[], byte[]> consumerRecord2 = consumerRecordFactory.create(SOURCE_TOPIC_2, key2, value2, timestamp2);

    private TopologyTestDriver testDriver;
    private final Properties config = mkProperties(mkMap(
        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "test-TopologyTestDriver"),
        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
        mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath())
    ));
    private KeyValueStore<String, Long> store;

    private final StringDeserializer stringDeserializer = new StringDeserializer();
    private final LongDeserializer longDeserializer = new LongDeserializer();
    private final ConsumerRecordFactory<String, Long> recordFactory = new ConsumerRecordFactory<>(
        new StringSerializer(),
        new LongSerializer());



    private Topology setupSourceSinkTopology() {
        final Topology topology = new Topology();

        final String sourceName = "source";

        topology.addSource(sourceName, SOURCE_TOPIC_1);
        topology.addSink("sink", SINK_TOPIC_1, sourceName);

        return topology;
    }

    static int countBefore=0;
    static int countAfter=0;
    @Test
    public void shouldReturnIterableForMultipleRecords() {
        testDriver = new TopologyTestDriver(setupSourceSinkTopology(), config);
        final byte[] key1 = "key1".getBytes();
        final byte[] value1 = "value1".getBytes();
        final ConsumerRecord<byte[], byte[]> consumerRecord1 = consumerRecordFactory
            .create(SOURCE_TOPIC_1, key1, value1, new RecordHeaders(), 0L);
        final byte[] key2 = "key2".getBytes();
        final byte[] value2 = "value2".getBytes();
        final ConsumerRecord<byte[], byte[]> consumerRecord2 = consumerRecordFactory
            .create(SOURCE_TOPIC_1, key2, value2, new RecordHeaders(), 0L);

        testDriver.pipeInput(consumerRecord1);
        testDriver.pipeInput(consumerRecord2);

        final Iterable<ProducerRecord<byte[], byte[]>> iterableOutput = testDriver.iterableOutput(SINK_TOPIC_1);
        final Iterator<ProducerRecord<byte[], byte[]>> output = iterableOutput.iterator();


        final ProducerRecord outputRecord1 = output.next();

        assertEquals(key1, outputRecord1.key());
        assertEquals(value1, outputRecord1.value());
        assertEquals(SINK_TOPIC_1, outputRecord1.topic());

        final ProducerRecord outputRecord2 = output.next();

        assertEquals(key2, outputRecord2.key());
        assertEquals(value2, outputRecord2.value());
        assertEquals(SINK_TOPIC_1, outputRecord2.topic());
        assertFalse(output.hasNext());

        countBefore=0;
        System.out.println("Before pipeInput");
        iterableOutput.forEach(value -> {
            System.out.println(value);
            countBefore++;
        });
        assertEquals(2, countBefore);

        testDriver.pipeInput(consumerRecord2);

        countAfter=0;
        System.out.println("After pipeInput");
        iterableOutput.forEach(value -> {
            System.out.println(value);
            countAfter++;
        });
        assertEquals(3, countAfter);
    }

    @Test
    public void shouldReturnIterableForMultipleRecordsWithSerDes() {
        testDriver = new TopologyTestDriver(setupSourceSinkTopology(), config);

        final ConsumerRecordFactory<String, Long> consumerRecordFactory = new ConsumerRecordFactory<>(
            new StringSerializer(),
            new LongSerializer());

        final String key1 = "key1";
        final Long value1 = 12345L;
        final ConsumerRecord<byte[], byte[]> consumerRecord1 = consumerRecordFactory
            .create(SOURCE_TOPIC_1, key1, value1, new RecordHeaders(), 0L);
        final String key2 = "key2";
        final Long value2 = 6789L;
        final ConsumerRecord<byte[], byte[]> consumerRecord2 = consumerRecordFactory
            .create(SOURCE_TOPIC_1, key2, value2, new RecordHeaders(), 0L);

        testDriver.pipeInput(consumerRecord1);
        testDriver.pipeInput(consumerRecord2);

        final Iterable<ProducerRecord<String, Long>> iterableOutput = testDriver
                .iterableOutput(SINK_TOPIC_1, stringDeserializer, longDeserializer);
        final Iterator<ProducerRecord<String, Long>> output = testDriver
            .iterableOutput(SINK_TOPIC_1, stringDeserializer, longDeserializer).iterator();

        final ProducerRecord outputRecord1 = output.next();

        assertEquals(key1, outputRecord1.key());
        assertEquals(value1, outputRecord1.value());
        assertEquals(SINK_TOPIC_1, outputRecord1.topic());

        final ProducerRecord outputRecord2 = output.next();

        assertEquals(key2, outputRecord2.key());
        assertEquals(value2, outputRecord2.value());
        assertEquals(SINK_TOPIC_1, outputRecord2.topic());
        assertFalse(output.hasNext());

        countBefore=0;
        System.out.println("Before pipeInput");
        iterableOutput.forEach(value -> {
            System.out.println(value);
            countBefore++;
        });
        assertEquals(2, countBefore);

        testDriver.pipeInput(consumerRecord2);

        countAfter=0;
        System.out.println("After pipeInput");
        iterableOutput.forEach(value -> {
            System.out.println(value);
            countAfter++;
        });
        //This is now 2, not 3 as without serdes
        assertEquals(3, countAfter);

    }

}
