package com.pipeline.streaming;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RawDataJob {

    public static void main(String[] args) throws Exception {


        String kafkaBootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String kafkaTopic = System.getenv("KAFKA_TOPIC_PAGEVIEW");
        String kafkaConsumerGroup = System.getenv("KAFKA_CONSUMER_GROUP_RAW");

        String s3BucketName = System.getenv("S3_BUCKET_NAME");
        String catalogName = System.getenv("ICEBERG_CATALOG_NAME");
        String dbName = System.getenv("ICEBERG_RAW_DB");
        String tableName = System.getenv("ICEBERG_RAW_TABLE");

        if (s3BucketName == null || s3BucketName.isEmpty()) {
            throw new IllegalArgumentException("Missing S3_BUCKET_NAME environment variable! Check .env file.");
        }

        String s3Path = "s3a://" + s3BucketName + "/" + catalogName;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        tableEnv.executeSql(
                "CREATE TABLE kafka_raw_pageviews (" +
                        "  `user id` INT," +
                        "  `postcode` STRING," +
                        "  `webpage` STRING," +
                        "  `timestamp` BIGINT" +
                        ") WITH (" +
                        "  'connector' = 'kafka'," +
                        "  'topic' = '" + kafkaTopic + "'," +
                        "  'properties.bootstrap.servers' = '" + kafkaBootstrapServers + "'," +
                        "  'properties.group.id' = '" + kafkaConsumerGroup + "'," +
                        "  'scan.startup.mode' = 'earliest-offset'," +
                        "  'format' = 'json'" +
                        ")"
        );

        tableEnv.executeSql(
                "CREATE CATALOG " + catalogName +" WITH (" +
                        "  'type' = 'iceberg'," +
                        "  'catalog-type' = 'hadoop'," +
                        "  'warehouse' = '" + s3Path + "'" +
                        ")"
        );

        tableEnv.useCatalog(catalogName);
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS " + dbName);

        String fullTableName = catalogName + "." + dbName + "." + tableName;


        tableEnv.executeSql(
                "CREATE TABLE IF NOT EXISTS " + fullTableName + " (" +
                        "  `user id` INT," +
                        "  `postcode` STRING," +
                        "  `webpage` STRING," +
                        "  `timestamp` BIGINT" +
                        ") WITH (" +
                        "  'format-version' = '2'," +
                        "  'write.format.default' = 'parquet'" +
                        ")"
        );


        tableEnv.executeSql(
                "INSERT INTO " + fullTableName + " " +
                        "SELECT `user id`, postcode, webpage, `timestamp` " +
                        "FROM default_catalog.default_database.kafka_raw_pageviews"
        );
    }
}