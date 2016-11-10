package com.sreedhar.spark.udemy;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Sreedhar on 10/11/16.
 */
public class PopularMoviesNicer {

    private static Map<Integer, String> loadMovieNames() {
        Map<Integer, String> ratings = new HashMap<>();

        Charset UTF_8 = Charset.forName("UTF-8");
        CharsetDecoder decoder = UTF_8.newDecoder();
        UTF_8.newDecoder().onUnmappableCharacter(CodingErrorAction.REPLACE);
        decoder.onMalformedInput(CodingErrorAction.REPLACE);
        decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);

        try(BufferedReader reader = Files.newBufferedReader(Paths.get("/Users/Sreedhar/2016/SparkScala/ml-100k/movies.csv"),UTF_8)) {
            reader.lines().skip(1).forEach(line -> {
                String[] fields = line.split(",");
                ratings.put(Integer.valueOf(fields[0]), fields[1]);
            });

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(ratings.size());
        return ratings;
    }

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("WordCountBetter").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        Broadcast<Map<Integer, String>> nameDict = sc.broadcast(loadMovieNames());

        JavaRDD<String> lines = sc.textFile("/Users/Sreedhar/2016/SparkScala/ml-20m/ratings_copy.csv");
        JavaPairRDD<Integer, Integer> movies = lines.mapToPair((PairFunction<String, Integer, Integer>) s -> new Tuple2<>(Integer.valueOf(s.split(",")[1]), 1));
        JavaPairRDD<Integer, Integer> movieCounts = movies.reduceByKey((integer, integer2) -> integer + integer2);
        JavaPairRDD<Integer, Integer> flipped = movieCounts.mapToPair(stringIntegerTuple2 -> new Tuple2<>(stringIntegerTuple2._2(), stringIntegerTuple2._1()));

        JavaPairRDD<Integer, Integer> sortedMovies = flipped.sortByKey();
        JavaPairRDD<String, String> results = sortedMovies.mapToPair((PairFunction<Tuple2<Integer, Integer>, String, String>) integerIntegerTuple2 -> new Tuple2<>(nameDict.getValue().get(integerIntegerTuple2._2()), integerIntegerTuple2._1().toString()));
        results.collect().forEach(System.out::println);
    }
}
