package com.sreedhar.spark.udemy;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;

import static java.lang.Math.min;

/**
 * Created by Sreedhar on 7/11/16.
 */
public class MinTempFilter {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("MinTempFilter").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile("/Users/Sreedhar/2016/SparkScala/SparkScala/1800.csv");
        JavaRDD<TempRecord> records = lines.map(s -> parseLine(s));

        JavaRDD<TempRecord> tminRecords = records.filter(tempRecord -> tempRecord.getEntryType().equalsIgnoreCase("TMIN"));
        JavaPairRDD<String, Float> stationTempPairs = tminRecords.mapToPair(tempRecord -> new Tuple2<>(tempRecord.getStationId(), tempRecord.getTemp()));
        stationTempPairs.collect().stream().forEach(System.out::println);
        JavaPairRDD<String, Float> reducedMinTemps = stationTempPairs.reduceByKey((a, b) -> min(a, b));
        reducedMinTemps.collect().stream().forEach(System.out::println);

//        tminRecords.collect().stream().forEach(System.out::println);

    }

    private static TempRecord parseLine(String line) {
        String [] fields = line.split(",");
        float temp = Float.valueOf(fields[3]) * 0.1f * (9.0f/5.0f) + 32.0f;
        return new TempRecord(fields[0], fields[2], temp);
    }

    private static final class TempRecord implements Serializable{
        private String stationId;
        private String entryType;
        private float temp;

        public TempRecord(String stationId, String entryType, float temp) {
            this.stationId = stationId;
            this.entryType = entryType;
            this.temp = temp;
        }

        public String getStationId() {
            return stationId;
        }

        public String getEntryType() {
            return entryType;
        }

        public float getTemp() {
            return temp;
        }

        @Override
        public String toString() {
            return "TempRecord{" +
                    "stationId='" + stationId + '\'' +
                    ", entryType='" + entryType + '\'' +
                    ", temp=" + temp +
                    '}';
        }
    }
}
