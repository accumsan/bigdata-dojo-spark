package com.minhdd.bigdata.dojo.spark;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by minhdao on 28/04/16.
 */ // Comparator sur les validations par gare
class MyTupleComparator implements Comparator<Tuple2<String, Long>>, Serializable {
    final static MyTupleComparator INSTANCE = new MyTupleComparator();
    public int compare(Tuple2<String, Long> t1, Tuple2<String, Long> t2) {
        return t1._2.compareTo(t2._2);    // sort descending
    }
}
