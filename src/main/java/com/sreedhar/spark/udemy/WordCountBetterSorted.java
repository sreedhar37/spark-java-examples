package com.sreedhar.spark.udemy;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by Sreedhar on 10/11/16.
 */
public class WordCountBetterSorted {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("WordCountBetterSorted").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile("/Users/Sreedhar/2016/SparkScala/SparkScala/book.txt");
        JavaRDD<String> words = lines.flatMap(x -> Arrays.asList(x.split("\\W+")).iterator());
        JavaPairRDD<String, Integer> javaPairRDD = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> wordCount = javaPairRDD.reduceByKey((Function2<Integer, Integer, Integer>) (integer, integer2) -> integer + integer2);
        JavaPairRDD<Integer, String> wordCountsSwapped = wordCount.mapToPair((PairFunction<Tuple2<String, Integer>, Integer, String>) stringIntegerTuple2 -> new Tuple2(stringIntegerTuple2._2(), stringIntegerTuple2._1()));
        wordCountsSwapped.sortByKey().collect().stream().forEach(System.out::println);
    }
}
