package com.sreedhar.spark.udemy;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import java.util.HashMap;
import java.util.Map;

import static java.util.Map.Entry.comparingByKey;


public class FirstSparkApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Dummy").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("/Users/Sreedhar/2016/SparkScala/ml-100k/u.data");

        JavaRDD<String> ratings = lines.map(x -> x.toString().split("\t")[2]);

        Map<String, Long> ratingsMap = ratings.countByValue();
        Map<String, Long> sortedRatingas = new HashMap<>();
        ratingsMap.entrySet()
                .stream().sorted(Map.Entry.comparingByKey())
                .forEachOrdered(x-> sortedRatingas.put(x.getKey(), x.getValue()));

        System.out.println(sortedRatingas);

    }
}
