package com.pipeline.streaming;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.configuration.Configuration;

public class AggregationJob {

    public static void main(String[] args) throws Exception {

        String kafkaBootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092");
        String kafkaTopic = System.getenv("KAFKA_TOPIC_PAGEVIEW");
        String kafkaConsumerGroup = System.getenv("KAFKA_CONSUMER_GROUP_AGGREGATION");

        String s3BucketName = System.getenv("S3_BUCKET_NAME");
        String catalogName = System.getenv("ICEBERG_CATALOG_NAME");
        String dbName = System.getenv("ICEBERG_AGG_DB");
        String tableName = System.getenv("ICEBERG_AGG_TABLE");

        if (s3BucketName == null || s3BucketName.isEmpty()) {
            throw new IllegalArgumentException("Missing S3_BUCKET_NAME environment variable! Check .env file.");
        }

        String s3Path = "s3a://" + s3BucketName + "/" + catalogName;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(
                "CREATE TABLE kafka_agg_pageviews (" +
                        "  `user id` INT," +
                        "  `postcode` STRING," +
                        "  `webpage` STRING," +
                        "  `timestamp` BIGINT," +
                        "  `event_time` AS TO_TIMESTAMP(FROM_UNIXTIME(`timestamp`))," +
                        "  WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND" +
                        ") WITH (" +
                        "  'connector' = 'kafka'," +
                        "  'topic' = '" + kafkaTopic + "'," +
                        "  'properties.bootstrap.servers' = '" + kafkaBootstrapServers + "'," +
                        "  'properties.group.id' = '" + kafkaConsumerGroup + "'," +
                        "  'scan.startup.mode' = 'earliest-offset'," +
                        "  'format' = 'json'" +
                        ")"
        );


            // 5. Configure the Iceberg Catalog (Force-feeding the AWS keys)
        tableEnv.executeSql(
                "CREATE CATALOG " + catalogName + " WITH (" +
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
                        "  postcode STRING," +
                        "  window_start TIMESTAMP(3)," +
                        "  window_end TIMESTAMP(3)," +
                        "  pageview_count BIGINT" +
                        ") WITH (" +
                        "  'format-version' = '2'," +
                        "  'write.format.default' = 'parquet'" +
                        ")"
        );


        tableEnv.executeSql(
                "INSERT INTO " + fullTableName + " " +
                        "SELECT " +
                        "  postcode, " +
                        "  window_start, " +
                        "  window_end, " +
                        "  COUNT(*) AS pageview_count " +
                        "FROM TABLE(" +
                        "  TUMBLE(TABLE default_catalog.default_database.kafka_agg_pageviews, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)" +
                        ") " +
                        "GROUP BY postcode, window_start, window_end"
        );
    }
}