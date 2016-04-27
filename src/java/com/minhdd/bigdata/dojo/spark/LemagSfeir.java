package com.minhdd.bigdata.dojo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.time.Month;
import java.util.HashMap;
import java.util.Map;

public class LemagSfeir {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Spark Hadoop");
        conf.setMaster("local");
        try (JavaSparkContext sparkContext = new JavaSparkContext(conf)) {
            JavaRDD<String> lines = sparkContext.textFile("data/validations-sur-le-reseau-ferre-nombre-de-validations-par-jour-1er-semestre-2015.csv");
            System.out.println(lines.count());
            JavaRDD<Validation> validations = lines
                    .filter(line -> !line.equals("JOUR;CODE_STIF_TRNS;CODE_STIF_RES;CODE_STIF_ARRET;LIBELLE_ARRET;ID_REFA_LDA;CATEGORIE_TITRE;NB_VALD"))
                    .map(Validation::new);

            // On met le RDD en cache mémoire pour une prochaine utilisation
            validations.cache();
            long totalValidations = validations.map(Validation::getValidations).reduce((v1, v2) -> v1 + v2);

            validations.groupBy(v -> v.getJour().getMonth())
                    // On calcule le total des validations par mois
                    .map(tuple -> {
                        Map<Month, Long> result = new HashMap<>();
                        result.put(tuple._1(), 0L);
                        tuple._2().forEach(validation -> {
                            result.put(tuple._1(), result.get(tuple._1()) + validation.getValidations());
                        });
                        return result;
                    })
                    // On récupère les résultats dans une map "Mois/Total validations du mois"
                    .reduce((v1, v2) -> {
                        Map<Month, Long> result = new HashMap<Month, Long>();
                        result.putAll(v1);
                        result.putAll(v2);
                        return result;
                    })
                    // Pour chaque ligne on divise par le total des validations du semestre et on affiche le résultat
                    .forEach((month, aLong) -> System.out.println(month + " : " + ((double) aLong / totalValidations) * 100));

            // On groupe par gare
            validations.groupBy(v -> v.getLibelleArret())
                    // Pour chaque gare on calcule le total des validations
                    .mapToPair(tuple -> {
                        Map<String, Long> result = new HashMap<>();
                        result.put(tuple._1(), 0L);
                        tuple._2().forEach(validation -> {
                            result.put(tuple._1(), result.get(tuple._1()) + validation.getValidations());
                        });
                        return new Tuple2<String, Long>(tuple._1(), result.get(tuple._1()));
                    })
                    // On trie par ordre ascendant sur les validations et on garde le top 5
                    .top(5, MyTupleComparator.INSTANCE)
                    .stream().forEach(stringLongTuple2 -> System.out.println(stringLongTuple2._1() + " : " + stringLongTuple2._2() + " =====> " + ((double) stringLongTuple2._2() / totalValidations) * 100 + " %"));
        }
    }
}
