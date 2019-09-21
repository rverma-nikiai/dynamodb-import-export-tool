/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.dynamodb.bootstrap.exception.NullReadCapacityException;
import com.amazonaws.dynamodb.bootstrap.exception.SectionOutOfRangeException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The interface that parses the arguments, and begins to transfer data from one
 * DynamoDB table to another
 */
public class CommandLineInterface {

    /**
     * Logger for the DynamoDBBootstrapWorker.
     */
    private static final Logger LOGGER = LogManager
            .getLogger(CommandLineInterface.class);

    /**
     * Main class to begin transferring data from one DynamoDB table to another
     * DynamoDB table.
     *
     * @param args
     */
    public static void main(String[] args) {
        CommandLineArgs params = new CommandLineArgs();
        JCommander cmd = new JCommander(params);

        try {
            // parse given arguments
            cmd.parse(args);
        } catch (ParameterException e) {
            LOGGER.error(e);
            JCommander.getConsole().println(e.getMessage());
            cmd.usage();
            System.exit(1);
        }

        // show usage information if help flag exists
        if (params.getHelp()) {
            cmd.usage();
            return;
        }
        final String sourceEndpoint = params.getSourceEndpoint();
        final String destinationEndpoint = params.getDestinationEndpoint();
        final String destinationTable = params.getDestinationTable();
        final String sourceTable = params.getSourceTable();
        final double readThroughputRatio = params.getReadThroughputRatio();
        final double writeThroughputRatio = params.getWriteThroughputRatio();
        final int maxWriteThreads = params.getMaxWriteThreads();
        final boolean consistentScan = params.getConsistentScan();

        final ClientConfiguration sourceConfig = new ClientConfiguration().withMaxConnections(BootstrapConstants.MAX_CONN_SIZE);
        final ClientConfiguration destinationConfig = new ClientConfiguration().withMaxConnections(BootstrapConstants.MAX_CONN_SIZE);

        final AmazonDynamoDB sourceClient = AmazonDynamoDBClient.builder()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(sourceEndpoint, "ap-southeast-1"))
                .withCredentials(new AWSStaticCredentialsProvider(params.getSourceCredentials()))
                .withClientConfiguration(sourceConfig).build();
        final AmazonDynamoDB destinationClient = AmazonDynamoDBClient.builder()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(destinationEndpoint, "ap-south-1"))
                .withCredentials(new AWSStaticCredentialsProvider(params.getDestinationCredentials()))
                .withClientConfiguration(destinationConfig)
                .build();

        TableDescription readTableDescription = sourceClient.describeTable(
                sourceTable).getTable();
        TableDescription writeTableDescription = destinationClient
                .describeTable(destinationTable).getTable();

        final double readThroughput = calculateThroughput(readTableDescription,
                readThroughputRatio, true);
        double writeThroughput;
        if (readTableDescription.getBillingModeSummary().getBillingMode().equals(BillingMode.PAY_PER_REQUEST.toString())) {
            writeThroughput = 2 * readThroughput;
        } else {
            writeThroughput = calculateThroughput(
                    writeTableDescription, writeThroughputRatio, false);
        }

        int numSegments = 10;
        try {
            numSegments = DynamoDBBootstrapWorker
                    .getNumberOfSegments(readTableDescription,readThroughput);
        } catch (NullReadCapacityException e) {
            LOGGER.warn("Number of segments not specified - defaulting to "
                    + numSegments, e);
        }

        try {
            ExecutorService sourceExec = getSourceThreadPool(numSegments);
            ExecutorService destinationExec = getDestinationThreadPool(maxWriteThreads);
            DynamoDBConsumer consumer = new DynamoDBConsumer(destinationClient,
                    destinationTable, writeThroughput, destinationExec);

            final DynamoDBBootstrapWorker worker = new DynamoDBBootstrapWorker(
                    sourceClient, readThroughput, sourceTable, sourceExec,
                    params.getSection(), params.getTotalSections(), numSegments, consistentScan);

            LOGGER.info("Starting transfer...");
            worker.pipe(consumer);
            LOGGER.info("Finished Copying Table.");
        } catch (ExecutionException e) {
            LOGGER.error("Encountered exception when executing transfer.", e);
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted when executing transfer.", e);
            System.exit(1);
        } catch (SectionOutOfRangeException e) {
            LOGGER.error("Invalid section parameter", e);
        }
    }

    /**
     * returns the provisioned throughput based on the input ratio and the
     * specified DynamoDB table provisioned throughput.
     * table copy completion time ~= # of items in source table * ceiling(average item size / 1KB) / WCU of destination table.
     */
    private static double calculateThroughput(
            TableDescription tableDescription, double throughputRatio,
            boolean read) {
        if (read) {
            if (tableDescription.getBillingModeSummary().getBillingMode().equals(BillingMode.PAY_PER_REQUEST.toString())) {
                double avgRecordSize = Math.ceil(tableDescription.getTableSizeBytes() / (1024.0 * tableDescription.getItemCount()));
                int readCu = 20;
                double timeToCompletionInHours = (tableDescription.getItemCount() * avgRecordSize) / (readCu * 3600);
                while (timeToCompletionInHours > 20) {
                    readCu *= 2;
                    timeToCompletionInHours /= 2;
                }
                return readCu;
            }
            return tableDescription.getProvisionedThroughput()
                    .getReadCapacityUnits() * throughputRatio;
        }
        return tableDescription.getProvisionedThroughput()
                .getWriteCapacityUnits() * throughputRatio;
    }

    /**
     * Returns the thread pool for the destination DynamoDB table.
     */
    private static ExecutorService getDestinationThreadPool(int maxWriteThreads) {
        int corePoolSize = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_CORE_POOL_SIZE;
        if (corePoolSize > maxWriteThreads) {
            corePoolSize = maxWriteThreads - 1;
        }
        final long keepAlive = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_KEEP_ALIVE;
        ExecutorService exec = new ThreadPoolExecutor(corePoolSize,
                maxWriteThreads, keepAlive, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(maxWriteThreads),
                new ThreadPoolExecutor.CallerRunsPolicy());
        return exec;
    }

    /**
     * Returns the thread pool for the source DynamoDB table.
     */
    private static ExecutorService getSourceThreadPool(int numSegments) {
        int corePoolSize = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_CORE_POOL_SIZE;
        if (corePoolSize > numSegments) {
            corePoolSize = numSegments - 1;
        }

        final long keepAlive = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_KEEP_ALIVE;
        ExecutorService exec = new ThreadPoolExecutor(corePoolSize,
                numSegments, keepAlive, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(numSegments),
                new ThreadPoolExecutor.CallerRunsPolicy());
        return exec;
    }

}
