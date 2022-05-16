package com.reins;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.from_json;

public class Mongo2HDFS {
    private static String warehouseDir = "hdfs://10.0.0.203:9000/mongo/warehouse/";
    private static String tableName = "mongo_1";
    private static String ckptPath = String.format("hdfs://10.0.0.203:9000/mongo/ckpt/%s", tableName);

    public static void main(String[] args){
        SparkSession spark = SparkSession.builder()
                .config("spark.master", "local")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.warehouse.dir", warehouseDir)
                .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                        "jdbc:mysql://rm-uf67ktcrjo69g32viko.mysql.rds.aliyuncs.com:3306/delta_metastore")
                .config("spark.hadoop.javax.jdo.option.ConnectionDriverName",
                        "com.mysql.cj.jdbc.Driver")
                .config("spark.hadoop.javax.jdo.option.ConnectionUserName", "zzt")
                .config("spark.hadoop.javax.jdo.option.ConnectionPassword", "Zzt19980924x")
                .config("spark.hadoop.datanucleus.autoCreateSchema", true)
                .config("spark.hadoop.datanucleus.autoCreateTables", true)
                .config("spark.hadoop.datanucleus.fixedDatastore", false)
                .config("spark.hadoop.datanucleus.readOnlyDatastore", false)
                .config("spark.hadoop.datanucleus.autoStartMechanism", "SchemaTable")
                .config("spark.hadoop.datanucleus.autoStartMechanism", "SchemaTable")
                .config("spark.hadoop.hive.metastore.schema.verification", false)
                .config("spark.hadoop.hive.metastore.schema.verification.record.version",
                        false)
                .config("spark.mongodb.input.uri", "mongodb://10.0.0.203:27000/car.car")
                .enableHiveSupport()
                .getOrCreate();

        StructType schema = new StructType()
                //.add("_id", DataTypes.StringType)
                .add("taxiId", DataTypes.IntegerType)
                .add("timestamp", DataTypes.StringType)
                .add("lat", DataTypes.DoubleType)
                .add("lon", DataTypes.DoubleType)
                .add("speed", DataTypes.DoubleType)
                .add("dir", DataTypes.IntegerType)
                .add("status", DataTypes.IntegerType);

        Dataset<Row> df = spark.read().schema(schema).format("mongo").load();
        df.printSchema();
        df.show();

        df.write().format("delta").mode("append").saveAsTable(tableName);

        spark.close();
    }
}
