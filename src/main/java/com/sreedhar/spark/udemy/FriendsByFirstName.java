package com.sreedhar.spark.udemy;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by Sreedhar on 6/11/16.
 */
public class FriendsByFirstName {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Dummy").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("/Users/Sreedhar/2016/SparkScala/SparkScala/fakefriends.csv");
        JavaPairRDD<String, Tuple2<Integer, Integer>> pairs = lines.map(s -> parseLine(s)).mapToPair(s-> new Tuple2(s._1(),new Tuple2<Integer, Integer>(s._2(),1)));
        pairs.collect().stream().forEach(System.out::println);
        JavaPairRDD<String, Tuple2<Integer, Integer>> firstReduceBy = pairs.reduceByKey((a, b) -> (new Tuple2(a._1() + b._1(), a._2() + b._2())));
        firstReduceBy.sortByKey().collect().stream().forEach(System.out::println);
        JavaPairRDD<String, Integer> finalResult = firstReduceBy.mapValues(x -> (x._1() / x._2()));
        finalResult.collect().stream().forEach(System.out::println);
    }

    private static Tuple2<String, Integer> parseLine(String line) {
        String[] fields = line.split(",");
        String firstName = fields[1];
        int numFriends = Integer.valueOf(fields[3]);
        return new Tuple2(firstName, numFriends);
    }
}
