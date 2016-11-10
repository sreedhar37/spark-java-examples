package com.sreedhar.spark.udemy;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by Sreedhar on 10/11/16.
 */
public class WordCountBetter {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("WordCountBetter").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile("/Users/Sreedhar/2016/SparkScala/SparkScala/book.txt");
        JavaRDD<String> words = lines.flatMap(x -> Arrays.asList(x.split("\\W+")).iterator());
        Map<String, Long> wordCount = words.countByValue();
        wordCount.forEach((k, v) -> System.out.println("(" + k + "," + v + ")"));
    }
}
