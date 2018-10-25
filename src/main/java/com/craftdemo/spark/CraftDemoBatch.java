package com.craftdemo.spark;

import org.apache.commons.cli.*;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.functions;
import java.util.*;

public class CraftDemoBatch {

    private static final Logger LOG = LoggerFactory.getLogger(CraftDemoBatch.class);

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

        String customersAvroFilePath = (String)commonConfig.get("customers.avro.path.file");
        String salesAvroFilePath = (String)commonConfig.get("sales.avro.path.file");
        String outputPath = (String) commonConfig.get("output.path");


        //--Batch

        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        spark.conf().set("spark.driver.host", "127.0.0.1");

        // Creates a DataFrame from the customer avro file
        Dataset<Row> dfCust = spark.read()
                .format("com.databricks.spark.avro")
                .load(customersAvroFilePath);

        dfCust.show();

        LOG.info("Customer records count:" + dfCust.count());

        // Creates a DataFrame from the sales avro file
        Dataset<Row> dfSales = spark.read()
                .format("com.databricks.spark.avro")
                .load(salesAvroFilePath);

        dfSales.show();

        LOG.info("Sale records count:" + dfSales.count());

        // Creates a DataFrame from joining the Customer and Sales DFs
        Dataset<Row> dfCustSales =
                dfCust.join(dfSales, dfCust.col("customerid")
                        .equalTo(dfSales.col("customerid")))
                        .drop(dfSales.col("customerid"));

        dfCustSales.show();

        // Total sales aggregations
        dfCustSales.select(
                dfCustSales.col("state"),
                dfCustSales.col("salesprice"),
                dfCustSales.col("timestamp"))
                .groupBy(
                        dfCustSales.col("state"),
                        functions.year(dfCustSales.col("timestamp").cast("timestamp").alias("year)"))
                )
                .sum("salesprice")
                .orderBy(dfCustSales.col("state"))
                .coalesce(1)
                .write().mode("append").format("com.databricks.spark.csv").save(outputPath);

                //.show();

        dfCustSales.select(
                dfCustSales.col("state"),
                dfCustSales.col("salesprice"),
                dfCustSales.col("timestamp"))
                .groupBy(
                        dfCustSales.col("state"),
                        functions.year(dfCustSales.col("timestamp").cast("timestamp")).alias("year"),
                        functions.month(dfCustSales.col("timestamp").cast("timestamp")).alias("month")
                )
                .sum("salesprice")
                .orderBy(dfCustSales.col("state"))
                .coalesce(1)
                .write().mode("append").format("com.databricks.spark.csv").save(outputPath);

                //.show();

        dfCustSales.select(
                dfCustSales.col("state"),
                dfCustSales.col("salesprice"),
                dfCustSales.col("timestamp"))
                .groupBy(
                        dfCustSales.col("state"),
                        functions.year(dfCustSales.col("timestamp").cast("timestamp")).alias("year"),
                        functions.month(dfCustSales.col("timestamp").cast("timestamp")).alias("month"),
                        functions.dayofmonth(dfCustSales.col("timestamp").cast("timestamp")).alias("day")
                )
                .sum("salesprice").alias("price")
                .orderBy(dfCustSales.col("state"))
                .coalesce(1)
                .write().mode("append").format("com.databricks.spark.csv").save(outputPath);

               // .show();

        dfCustSales.select(
                dfCustSales.col("state"),
                dfCustSales.col("salesprice"),
                dfCustSales.col("timestamp"))
                .groupBy(
                        dfCustSales.col("state"),
                        functions.year(dfCustSales.col("timestamp").cast("timestamp")).alias("year"),
                        functions.month(dfCustSales.col("timestamp").cast("timestamp")).alias("month"),
                        functions.dayofmonth(dfCustSales.col("timestamp").cast("timestamp")).alias("day"),
                        functions.hour(dfCustSales.col("timestamp").cast("timestamp").alias("hours"))
                )
                .sum("salesprice")
                .orderBy(dfCustSales.col("state"))
                .coalesce(1)
                .write().mode("append").format("com.databricks.spark.csv").save(outputPath);


        Dataset<Row> test = spark.table("SalesPerHour");
        test.show();

    }
}
