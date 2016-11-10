package com.sreedhar.spark.udemy;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by Sreedhar on 10/11/16.
 */
public class PopularMovies {


    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("WordCountBetter").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile("/Users/Sreedhar/2016/SparkScala/ml-100k/u.data");
        JavaPairRDD<Integer, Integer> movies = lines.mapToPair((PairFunction<String, Integer, Integer>) s -> new Tuple2<>(Integer.valueOf(s.split("\t")[1]), 1));
        JavaPairRDD<Integer, Integer> movieCounts = movies.reduceByKey((integer, integer2) -> integer + integer2);
        JavaPairRDD<Integer, Integer> flipped = movieCounts.mapToPair(stringIntegerTuple2 -> new Tuple2<>(stringIntegerTuple2._2(), stringIntegerTuple2._1()));
        flipped.sortByKey().collect().stream().forEach(System.out::println);
    }
}
