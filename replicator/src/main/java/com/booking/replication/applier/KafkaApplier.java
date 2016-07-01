package com.booking.replication.applier;

import static com.codahale.metrics.MetricRegistry.name;

import com.booking.replication.Metrics;
import com.booking.replication.augmenter.AugmentedRow;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.pipeline.BinlogPositionInfo;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.util.JsonBuilder;

import com.google.code.or.binlog.impl.event.FormatDescriptionEvent;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.XidEvent;
import com.google.common.collect.Lists;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaApplier implements Applier {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaApplier.class);

    private final Properties producerConfig;
    private final List<String> tableList;
    private final String topic;

    private KafkaProducer<String, String> producer;

    private BinlogPositionInfo currentBinlog;
    private final Integer partitions;

    private AtomicBoolean exceptionFlag = new AtomicBoolean(false);

    private static final Counter exception_counters = Metrics.registry.counter(name("Kafka", "exceptionCounter"));
    private static final Counter eventSkipCounter = Metrics.registry.counter(name("Kafka", "eventSkipCounter"));
    private static final Meter kafka_messages = Metrics.registry.meter(name("Kafka", "producerToBroker"));
    private static final Timer closureTimer = Metrics.registry.timer(name("Kafka", "producerCloseTimer"));
    private static final HashMap<String, MutableLong> stats = new HashMap<>();

    static Properties consumerConfig(String group, String servers) {
        Properties props = new Properties();
        props.put("group.id", group);
        props.put("bootstrap.servers", servers);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "none");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    static Properties producerConfig(String broker) {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("acks", "all"); // Default 1
        props.put("retries", 3); // Default value: 0
        props.put("batch.size", 16384); // Default value: 16384
        props.put("linger.ms", 20); // Default 0, Artificial delay
        props.put("buffer.memory", 33554432); // Default value: 33554432
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("metric.reporters", "com.booking.replication.applier.KafkaMetricsCollector");
        return props;
    }

    private Map<Integer, BinlogPositionInfo> perPartitionLastPosition = new HashMap<>();

    public KafkaApplier(
            String broker,
            String topic,
            List<String> tablesToReplicate,
            BinlogPositionInfo startBinlogPosition
    ) {
        this.topic = topic;
        currentBinlog = startBinlogPosition;
        tableList = tablesToReplicate;

        /**
         * kafka.producer.Producer provides the ability to batch multiple produce requests (producer.type=async),
         * before serializing and dispatching them to the appropriate kafka broker partition. The size of the batch
         * can be controlled by a few config parameters. As events enter a queue, they are buffered in a queue, until
         * either queue.time or batch.size is reached. A background thread (kafka.producer.async.ProducerSendThread)
         * dequeues the batch of data and lets the kafka.producer.DefaultEventHandler serialize and send the data to
         * the appropriate kafka broker partition.
         */
        producerConfig = producerConfig(broker);
        producer = new KafkaProducer<>(producerConfig);
        partitions = producer.partitionsFor(topic).size();

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig("KafkaApplierGroup", broker));

        Map<TopicPartition, Long> partitions = new HashMap<>();

        for (PartitionInfo pi : consumer.partitionsFor(topic)) {
            partitions.put(new TopicPartition(pi.topic(), pi.partition()), null);
        }

        consumer.assign(Lists.newArrayList(partitions.keySet()));

        for (TopicPartition tp : partitions.keySet()) {
            consumer.seekToEnd(tp);
            long partitionPosition = consumer.position(tp);
            LOGGER.info("Last Kafka offset for partition " +  tp.partition() + " is: " + partitionPosition);
            if (partitionPosition > 0) {
                consumer.seek(tp, partitionPosition - 1);
                if (consumer.position(tp) != partitionPosition - 1) {
                    LOGGER.error("Seek failed!");
                }
                perPartitionLastPosition.put(tp.partition(), null);
            } else {
                perPartitionLastPosition.put(
                        tp.partition(),
                        new BinlogPositionInfo(currentBinlog.getBinlogFilename(), currentBinlog.getBinlogPosition())
                );
            }
        }

        try {
            while (perPartitionLastPosition.values().contains(null)) {
                ConsumerRecords<String, String> vals = consumer.poll(100);
                for (ConsumerRecord<String, String> record : vals) {
                    try {
                        // We might be consuming more than one events in some weird cases, we want the first one...
                        if (perPartitionLastPosition.get(record.partition()) != null) {
                            continue;
                        }
                        String binlog = record.key().split("\\|")[0];
                        Long position = Long.parseLong(record.key().split("\\|")[1]);

                        if (position == -1L) {
                            KafkaRotateEvent ev = om.readValue(record.value(), KafkaRotateEvent.class);
                            perPartitionLastPosition.put(record.partition(), ev.binlogPositionInfo);
                        } else {
                            perPartitionLastPosition.put(record.partition(), new BinlogPositionInfo(binlog, position));
                        }
                    } catch (Exception e) {
                        LOGGER.error("Failed to deserialize row: \n" + record.value());
                        e.printStackTrace();
                        System.exit(2);
                    }
                }
            }
        } catch (OffsetOutOfRangeException e) {
            LOGGER.error("Kafka offset out of range: " + e.getMessage());
            LOGGER.error("This will happen when restarting the applier after kafka evicted the last item in a partition");
            LOGGER.error("You need to drop the kafka queue and start from offset 0");
            System.exit(2);
        }

        for ( int partition: perPartitionLastPosition.keySet()) {
            LOGGER.info(String.format("Last Binlog Position for partition %s: %s %s", partition,
                    perPartitionLastPosition.get(partition).getBinlogFilename(),
                    perPartitionLastPosition.get(partition).getBinlogPosition())
            );
        }

    }

    private long totalOutlierCounter = 0;
    private long lastPos = 0;

    @Override
    public void applyAugmentedRowsEvent(AugmentedRowsEvent augmentedSingleRowEvent, PipelineOrchestrator caller) {
        int partition = Math.abs(augmentedSingleRowEvent.getMysqlTableName().hashCode()) % partitions;

        BinlogPositionInfo lastPosition = perPartitionLastPosition.get(partition);
        currentBinlog.setBinlogPosition(augmentedSingleRowEvent.getSingleRowEvents().get(0).getEventV4Header().getPosition());

        if (lastPosition.greaterThan(currentBinlog) || lastPosition.equals(currentBinlog)) {
            eventSkipCounter.inc();
            return;
        }

        long eventPos = lastPos;
        for (AugmentedRow row : augmentedSingleRowEvent.getSingleRowEvents()) {
            if (exceptionFlag.get()) {
                throw new RuntimeException("Error found in Producer");
            }

            eventPos = row.getEventV4Header().getPosition();

            if (lastPos >= eventPos) {
                throw new RuntimeException(String.format(
                        "Position of current event is lower than of a previously submitted event, this should never happen!\n%s >= %s\n%s",
                        lastPos, eventPos,
                        JsonBuilder.dataEventToJson(row)
                ));
            }

            if (row.getTableName() == null) {
                LOGGER.error("tableName not exists");
                throw new RuntimeException("tableName does not exist");
            }

            String tableName = row.getTableName();
            if (tableList.contains(tableName)) {
                ProducerRecord<String, String> message = new ProducerRecord<>(
                        topic,
                        partition,
                        String.format("%s|%s", augmentedSingleRowEvent.getBinlogFileName(), String.valueOf(eventPos)),
                        row.toJson()
                );
                producer.send(message, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception sendException) {
                        if (sendException != null) {
                            LOGGER.error("Error producing to Kafka broker");
                            sendException.printStackTrace();
                            exceptionFlag.set(true);
                            exception_counters.inc();
                        }
                    }
                });
                kafka_messages.mark();
            } else {
                totalOutlierCounter ++;
                if (totalOutlierCounter % 500 == 0) {
                    LOGGER.warn("Over 500 non-supported topics, for example: " + tableName);
                }
            }
        }
        lastPos = eventPos;
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

    public static class KafkaRotateEvent {
        KafkaRotateEvent() {}

        KafkaRotateEvent(BinlogPositionInfo binlogFile) {
            binlogPositionInfo = binlogFile;
        }

        public BinlogPositionInfo binlogPositionInfo;
    }

    private static final ObjectMapper om = new ObjectMapper();

    @Override
    public void waitUntilAllRowsAreCommitted(RotateEvent event) {
        currentBinlog = new BinlogPositionInfo(event.getBinlogFileName().toString(), 4L);

        final Timer.Context context = closureTimer.time();
        for (PartitionInfo tp :producer.partitionsFor(topic)) {
            BinlogPositionInfo pos = perPartitionLastPosition.get(tp.partition());

            if (pos.greaterThan(currentBinlog) || pos.equals(currentBinlog)) {
                continue;
            }

            ProducerRecord<String, String> message = null;
            try {
                message = new ProducerRecord<>(
                        topic,
                        tp.partition(),
                        String.format("%s|%s", event.getBinlogFileName().toString(), -1L),
                        om.writeValueAsString(new KafkaRotateEvent(
                            new BinlogPositionInfo(
                                    event.getBinlogFileName().toString(),
                                    4L
                            )
                        ))
                    );
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                System.exit(-2);
            }

            producer.send(message);
        }


        producer.close();
        context.stop();

        producer = new KafkaProducer<>(producerConfig);
        lastPos = 0L;
    }
}
