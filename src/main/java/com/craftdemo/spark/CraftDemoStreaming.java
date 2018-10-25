package com.craftdemo.spark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.io.Serializable;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.sql.*;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import com.databricks.spark.avro.SchemaConverters;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/*
Usage Example:
    spark-submit --master spark://127.0.0.1:7077 --class com.craftdemo.spark.CraftDemo SparkProject-1.0-SNAPSHOT.jar -confPath conf/conf.yaml
 */


public class CraftDemoStreaming {

    private static final Logger LOG = LoggerFactory.getLogger(CraftDemoStreaming.class);

    public static void main(String[] args){

        Options opts = new Options();
        opts.addOption("confPath", true, "Path to the config file.");

        CommandLine cmd = null;

        try {
            CommandLineParser parser = new BasicParser();
            cmd = parser.parse(opts, args);
        } catch (ParseException e){
            e.printStackTrace();
            LOG.error("Parameter parsing exception" + e.getMessage());
        }

        String configPath = cmd.getOptionValue("confPath");

        final Map commonConfig = Utils.findAndReadConfigFile(configPath, true);

        String kafkaTopic = (String)commonConfig.get("kafka.topic");
        String kafkaBroker = (String)commonConfig.get("kafka.broker");

        String dataDir = (String)commonConfig.get("file.path");

        Map<String, Object> kafkaParams = new HashMap<>();

        kafkaParams.put("bootstrap.servers", kafkaBroker);
        kafkaParams.put("group.id", "sales-id-02");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", ByteArrayDeserializer.class);
        kafkaParams.put("enable.auto.commit", false);

        SparkConf conf = new SparkConf()
                .setAppName("Spark Craft Demo")
                .setMaster("local[*]")
                .set("spark.driver.allowMultipleContexts", "true");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(10000));

        TopicPartition partition = new TopicPartition(kafkaTopic, 0);

        // Create a Spark Kafka consumer that extracts sales per state in a DStream
        JavaInputDStream<ConsumerRecord<String,byte[]>> messages = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Assign(Arrays.asList(partition),kafkaParams));

        // Convert each RDD in a Row
        List<Row> rowList = new ArrayList<Row>();

        messages.foreachRDD((VoidFunction<JavaRDD<ConsumerRecord<String, byte[]>>>) rdd -> {
            rdd.foreach(new VoidFunction<ConsumerRecord<String, byte[]>>() {

                @Override
                public void call(ConsumerRecord<String, byte[]> t) throws Exception {
                    GenericRecord genericRecord =
                            AvroSupport.byteArrayToData(AvroSupport.getSchema(), t.value());

                    String state = AvroSupport.getValue(genericRecord, "state", String.class);
                    Float salesPrice = AvroSupport.getValue(genericRecord, "salesprice", Float.class);
                    Long timeStamp = AvroSupport.getValue(genericRecord, "timestamp", Long.class);

                    LOG.info("State: {}", state);
                    LOG.info("Sales Price: {}", salesPrice);
                    LOG.info("Timestamp: {}", timeStamp);

                    Row rowRecord = genericRecordToRow(genericRecord);
                    rowList.add(rowRecord);

                }
            });
        });

        LOG.info("Row[] size: " +  rowList.size());

        // Convert List of Rows to a Dataframe
        SQLContext sqc = new org.apache.spark.sql.SQLContext(sc);
        JavaRDD<Row>  jRDD =
                new JavaSparkContext(sqc.sparkContext()).parallelize(rowList);

        Dataset<Row> salesByStateDF = sqc.createDataFrame(jRDD, Row.class);
        salesByStateDF.printSchema();

        // TODO: Compute Total Sales per State per year
     /*   salesByStateDF.select(
                salesByStateDF.col("state"),
                salesByStateDF.col("salesprice"),
                salesByStateDF.col("timestamp"))
                .groupBy(
                        salesByStateDF.col("state"),
                        functions.year(salesByStateDF.col("timestamp").cast("timestamp").alias("year)"))
                )
                .sum("salesprice")
                .orderBy(salesByStateDF.col("state"))
                .coalesce(1)
                .write().mode("append").format("com.databricks.spark.csv")
                .saveAsTable("StreamingSalesPerYear");
*/
        // TODO: Compute Total Sales per State per month
        // TODO: Compute Total Sales per State per day
        // TODO: Compute Total Sales per State per hour

        ssc.start();

        // Keep the processing live by halting here unless terminated manually
       try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    // Function to convert from GenericRecord to Row
    private static Row genericRecordToRow(GenericRecord avroRecord) {
        if (null == avroRecord) {
            return null;
        }

        Object[] objectArray = new Object[avroRecord.getSchema().getFields().size()];
        StructType structType = (StructType) SchemaConverters.toSqlType(avroRecord.getSchema()).dataType();

        for (Schema.Field field : avroRecord.getSchema().getFields()) {
            if(field.schema().getType().toString().equalsIgnoreCase("STRING")
                    || field.schema().getType().toString().equalsIgnoreCase("ENUM")){
                objectArray[field.pos()] = ""+avroRecord.get(field.pos());
            }else {
                objectArray[field.pos()] = avroRecord.get(field.pos());
            }
        }
        return new GenericRowWithSchema(objectArray, structType);
    }
}
