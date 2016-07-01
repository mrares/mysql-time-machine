package com.booking.replication.applier;

import com.booking.replication.Constants;

import com.booking.replication.applier.hbase.HBaseApplierWriter;
import com.booking.replication.augmenter.AugmentedRowsEvent;
import com.booking.replication.augmenter.AugmentedSchemaChangeEvent;
import com.booking.replication.pipeline.PipelineOrchestrator;
import com.booking.replication.schema.HBaseSchemaManager;

import com.google.code.or.binlog.impl.event.FormatDescriptionEvent;
import com.google.code.or.binlog.impl.event.QueryEvent;
import com.google.code.or.binlog.impl.event.RotateEvent;
import com.google.code.or.binlog.impl.event.XidEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class abstracts the HBase store.
 * <p>
 * Conventions used:
 *
 *      1. Each replication chain is replicated to a namespace "${chain_name}_replication".
 *
 *      2. All table names are converted to low-caps. For example My_Schema.My_Table will be replicated
 *         to 'my_schema:my_table'
 * </p>
 */
public class HBaseApplier implements Applier {

    // TODO: move configuration vars to Configuration
    private static final int POOL_SIZE = 30;

    private static final int UUID_BUFFER_SIZE = 1000; // <- max number of rows in one uuid buffer

    private static final int BUFFER_FLUSH_INTERVAL = 60000; // <- force buffer flush every 60 sec

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseApplier.class);

    private final HBaseSchemaManager hbaseSchemaManager;

    private final HBaseApplierWriter hbaseApplierWriter;

    private long timeOfLastFlush = 0;

    private final com.booking.replication.Configuration configuration;

    /**
     * HBaseApplier constructor.
     */
    public HBaseApplier(
        com.booking.replication.Configuration config
    ) {
        configuration = config;

        hbaseApplierWriter =
            new HBaseApplierWriter(
                POOL_SIZE,
                configuration
            );

        hbaseSchemaManager = new HBaseSchemaManager(configuration.getHBaseQuorum());
    }


    @Override
    public void applyCommitQueryEvent(QueryEvent event) {
        markCurrentTransactionForCommit();
    }

    @Override
    public void applyXidEvent(XidEvent event) {
        // TODO: add transactionID to storage
        // long transactionID = event.getXid();
        markCurrentTransactionForCommit();
    }

    @Override
    public void applyRotateEvent(RotateEvent event) {
        LOGGER.info("binlog rotate ["
                + event.getBinlogFilename()
                + "], flushing buffer of "
                + hbaseApplierWriter.rowsBufferedInCurrentTask.get()
                + " rows before moving to the next binlog file.");
        LOGGER.info("Stats snapshot: ");
        markAndSubmit(); // mark current as ready; flush all;
    }

    @Override
    public void applyAugmentedSchemaChangeEvent(
            AugmentedSchemaChangeEvent event,
            PipelineOrchestrator caller) {
        hbaseSchemaManager.writeSchemaSnapshotToHBase(event, configuration);
    }

    /**
     * Core logic of the applier. Processes data events and writes to HBase.
     *
     * @param augmentedRowsEvent Rows event
     * @param pipeline Pipeline instance
     */
    @Override
    public void applyAugmentedRowsEvent(
            final AugmentedRowsEvent augmentedRowsEvent,
            final PipelineOrchestrator pipeline) {

        String hbaseNamespace = getHBaseNamespace(pipeline);
        if (hbaseNamespace == null) {
            return;
        }

        //HBasePreparedAugmentedRowsEvent hBasePreparedAugmentedRowsEvent =
        //        new HBasePreparedAugmentedRowsEvent(hbaseNamespace, augmentedRowsEvent);

        // buffer
        hbaseApplierWriter.pushToCurrentTaskBuffer(augmentedRowsEvent);

        // flush on buffer size or time limit
        long currentTime = System.currentTimeMillis();
        long tdiff = currentTime - timeOfLastFlush;

        boolean forceFlush = (tdiff > BUFFER_FLUSH_INTERVAL);
        if ((hbaseApplierWriter.rowsBufferedInCurrentTask.get() >= UUID_BUFFER_SIZE) || forceFlush) {
            markAndSubmit();
        }
    }

    private String getHBaseNamespace(PipelineOrchestrator pipeline) {

        // get database name from event
        String mySqlDbName = configuration.getReplicantSchemaName();
        String currentTransactionDB = pipeline.currentTransactionMetadata
                .getFirstMapEventInTransaction()
                .getDatabaseName()
                .toString();

        String hbaseNamespace = null;
        if (currentTransactionDB != null) {
            if (currentTransactionDB.equals(mySqlDbName)) {
                hbaseNamespace = mySqlDbName.toLowerCase();
            } else if (currentTransactionDB.equals(Constants.BLACKLISTED_DB)) {
                return null;
            } else {
                LOGGER.error("Invalid database name: " + currentTransactionDB);
            }
        } else {
            LOGGER.error("CurrentTransactionDB can not be null");
        }
        return hbaseNamespace;
    }

    @Override
    public void forceFlush() {
        markAndSubmit();
    }

    private void markAndSubmit() {
        markCurrentTaskAsReadyToGo();
        submitAllTasksThatAreReadyToGo();
        timeOfLastFlush = System.currentTimeMillis();
    }

    public void resubmitIfThereAreFailedTasks() {
        hbaseApplierWriter.markAllTasksAsReadyToGo();
        submitAllTasksThatAreReadyToGo();
        hbaseApplierWriter.updateTaskStatuses();
        timeOfLastFlush = System.currentTimeMillis();
    }

    // mark current uuid buffer as READY_FOR_PICK_UP and create new uuid buffer
    private void markCurrentTaskAsReadyToGo() {
        hbaseApplierWriter.markCurrentTaskAsReadyAndCreateNewUuidBuffer();
    }

    private void submitAllTasksThatAreReadyToGo() {
        // Submit all tasks that are ready for pick up
        hbaseApplierWriter.submitTasksThatAreReadyForPickUp();
    }

    @Override
    public void applyFormatDescriptionEvent(FormatDescriptionEvent event) {
        LOGGER.info("Processing file " + event.getBinlogFilename());
        hbaseApplierWriter.initBuffers();
    }

    @Override
    public void waitUntilAllRowsAreCommitted(RotateEvent event) {
        boolean wait = true;

        while (wait) {
            if (hbaseApplierWriter.areAllTasksDone()) {
                LOGGER.debug("All tasks have completed!");
                wait = false;
            } else {
                resubmitIfThereAreFailedTasks();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void markCurrentTransactionForCommit() {
        hbaseApplierWriter.markCurrentTransactionForCommit();
    }

}
