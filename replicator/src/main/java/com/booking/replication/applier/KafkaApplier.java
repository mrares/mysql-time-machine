package com.booking.replication.applier;

import static com.codahale.metrics.MetricRegistry.name;
import static org.apache.kafka.common.protocol.Protocol.BROKER;

import com.booking.replication.Configuration;
import com.booking.replication.Metrics;
import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.pipeline.PipelineOrchestrator;

import com.google.code.or.binlog.impl.event.FormatDescriptionEvent;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.XidEvent;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by raynald on 08/06/16.
 */

// TODO: Kafka Metrics: record-send-rate

public class KafkaApplier implements Applier {
    private final com.booking.replication.Configuration replicatorConfiguration;
    private static long totalEventsCounter = 0;
    private static long totalRowsCounter = 0;
    private static long totalOutlierCounter = 0;
    private Properties props;
    private KafkaProducer<String, String> producer;
    private ProducerRecord<String, String> message;
    private static List<String> topicList;

    private AtomicBoolean exceptionFlag = new AtomicBoolean(false);
    private static final Meter kafka_messages = Metrics.registry.meter(name("Kafka", "producerToBroker"));
    private static final Counter exception_counters = Metrics.registry.counter(name("Kafka", "exceptionCounter"));
    private static final Timer closureTimer = Metrics.registry.timer(name("Kafka", "producerCloseTimer"));
    private static final HashMap<String, MutableLong> stats = new HashMap<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaApplier.class);

    public KafkaApplier(String broker, List<String> topics, Configuration configuration) {
        replicatorConfiguration = configuration;

        /**
         * kafka.producer.Producer provides the ability to batch multiple produce requests (producer.type=async),
         * before serializing and dispatching them to the appropriate kafka broker partition. The size of the batch
         * can be controlled by a few config parameters. As events enter a queue, they are buffered in a queue, until
         * either queue.time or batch.size is reached. A background thread (kafka.producer.async.ProducerSendThread)
         * dequeues the batch of data and lets the kafka.producer.DefaultEventHandler serialize and send the data to
         * the appropriate kafka broker partition.
         */

        // Below is the new version of Configuration
        props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("acks", "all"); // Default 1
        props.put("retries", 1); // Default value: 0
        props.put("batch.size", 16384); // Default value: 16384
        props.put("linger.ms", 20); // Default 0, Artificial delay
        props.put("buffer.memory", 33554432); // Default value: 33554432
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("metric.reporters", "com.booking.replication.applier.KafkaMetricsCollector");
        producer = new KafkaProducer<>(props);
        topicList = topics;
    }

    @Override
    public void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedSingleRowEvent, PipelineOrchestrator caller) {
        totalEventsCounter ++;
        for (AugmentedRow row : augmentedSingleRowEvent.getSingleRowEvents()) {
            if (exceptionFlag.get()) {
                throw new RuntimeException("Error found in Producer");
            }
            if (row.getTableName() == null) {
                LOGGER.error("tableName not exists");
                throw new RuntimeException("tableName does not exist");
            }

            String topic = row.getTableName();
            if (topicList.contains(topic)) {
                message = new ProducerRecord<>(topic, row.toJson());
                totalRowsCounter++;
                producer.send(message, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception sendException) {
                        if (sendException != null) {
                            LOGGER.error("Error producing to topic " + recordMetadata.topic());
                            sendException.printStackTrace();
                            exceptionFlag.set(true);
                            exception_counters.inc();
                        }
                    }
                });
                if (totalRowsCounter % 500 == 0) {
                    LOGGER.debug("500 lines have been sent to Kafka broker...");
                }
                kafka_messages.mark();
            } else {
                totalOutlierCounter ++;
                if (totalOutlierCounter % 500 == 0) {
                    LOGGER.warn("Over 500 non-supported topics, for example: " + topic);
                }
            }
        }
    }

    @Override
    public void applyCommitQueryEvent(QueryEvent event) {

    }

    @Override
    public void applyXidEvent(XidEvent event) {

    }

    @Override
    public void applyRotateEvent(RotateEvent event) {

    }

    @Override
    public void applyAugmentedSchemaChangeEvent(AugmentedSchemaChangeEvent augmentedSchemaChangeEvent, PipelineOrchestrator caller) {

    }

    @Override
    public void forceFlush() {

    }

    @Override
    public void applyFormatDescriptionEvent(FormatDescriptionEvent event) {

    }

    @Override
    public void waitUntilAllRowsAreCommitted() {
        final Timer.Context context = closureTimer.time();
        producer.close();
        context.stop();
        LOGGER.warn("New producer");
        producer = new KafkaProducer<>(props);
    }
}
