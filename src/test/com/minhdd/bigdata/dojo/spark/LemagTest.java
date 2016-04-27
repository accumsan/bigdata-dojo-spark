package com.minhdd.bigdata.dojo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

/**
 * Created by mdao on 27/04/2016.
 */
public class LemagTest {
    @Test
    public void test() {
        SparkConf conf = new SparkConf();
        conf.setAppName("Spark Hadoop");
        conf.setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<String> lines = sparkContext.textFile("data/validations-sur-le-reseau-ferre-nombre-de-validations-par-jour-1er-semestre-2015.csv");
        
        System.out.println(lines.count());
    }
}
