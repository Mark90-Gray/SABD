package KMeansNaive;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
public class LloydKMeans {

    public static List<Tuple2<Integer, Tuple2<Double, String>>> Naive(JavaRDD<Iterable<Tuple2<Double,String>>> input, Integer mesex, int numeber_top, int ITERAZIONI) {

        //RDD con chiave il mese e valore la lista dei trend top
        JavaPairRDD<Integer, ArrayList<Double>> init = input.mapToPair(new PairFunction<Iterable<Tuple2<Double, String>>, Integer, ArrayList<Double>>() {
            @Override
            public Tuple2<Integer, ArrayList<Double>> call(Iterable<Tuple2<Double, String>> tuple2s) throws Exception {
                ArrayList<Double> p = new ArrayList<>();
                for (Tuple2<Double, String> temp : tuple2s) {
                    p.add(temp._1());
                }
                return new Tuple2<>(mesex, p);
            }
        });

        // Array List di trend
        ArrayList<Double> d = new ArrayList<>();

        for (Tuple2<Integer, ArrayList<Double>> t : init.collect()) {
            for (int k = 0; k < t._2().size(); k++) {
                d.add(t._2.get(k));
            }
        }

        //Generatore random di centroidi sulla lista dei trend arrivati per mese
        Random r = new Random();
        int upper = numeber_top;
        ArrayList<Double> centroide = new ArrayList<>();
        centroide.add(d.get(r.nextInt(upper)));
        centroide.add(d.get(r.nextInt(upper)));
        centroide.add(d.get(r.nextInt(upper)));
        centroide.add(d.get(r.nextInt(upper)));

        //Inizializzazione RDD per ciclo di iterazioni
        JavaPairRDD<Integer, Tuple2<Double, String>> cluster_1 = null;

        //Ciclo di iterazioni
        for (int iter = 0; iter < ITERAZIONI; iter++) {

            //MAP dei trend per i centroidi restituisce il cluster e il trend con lo stato associato
            cluster_1 = input.flatMapToPair(new PairFlatMapFunction<Iterable<Tuple2<Double, String>>, Integer, Tuple2<Double, String>>() {
                @Override
                public Iterator<Tuple2<Integer, Tuple2<Double, String>>> call(Iterable<Tuple2<Double, String>> tuple2s) throws Exception {
                    Double min;
                    Double z;
                    Integer cluster;
                    ArrayList<Tuple2<Integer, Tuple2<Double, String>>> res = new ArrayList<>();
                    for (Tuple2<Double, String> t : tuple2s) {
                        min = Math.abs(centroide.get(0) - t._1());
                        cluster = 0;
                        for (int i = 1; i < centroide.size(); i++) {
                            z = Math.abs(centroide.get(i) - t._1());
                            if (min > z) {
                                min = z;
                                cluster = i;
                            }
                        }
                        res.add(new Tuple2<>(cluster, t));

                    }
                    return res.iterator();
                }
            });

            //Map in RDD con chiave il cluster  e valore il trend
            JavaPairRDD<Integer, Double> cluster_2 = cluster_1.mapToPair(x -> new Tuple2<>(x._1(), x._2()._1()));

            //Reduce somma dei trend
            JavaPairRDD<Integer, Double> sum_trend = cluster_2.reduceByKey((a, b) -> a + b);

            //Raggruppamento per cluster
            JavaPairRDD<Integer, Iterable<Double>> pro= sum_trend.groupByKey();

            //RDD con chiave cluster e valore  generico, necessario per il count di elementi che fanno parte del cluster
            JavaPairRDD<Integer, Double> cluster_count = cluster_1.mapToPair(x -> new Tuple2<>(x._1(), 1.0));
            JavaPairRDD<Integer,Double> count=cluster_count.reduceByKey((a,b)->a+b);

            JavaPairRDD<Integer, Tuple2<Double, Double>> join_fox0 = sum_trend.join(count);

            //RDD con chiave il cluster e valore il nuovo centroide
            JavaPairRDD<Integer, Double> centroide_nuovo_final = join_fox0.mapToPair(x -> new Tuple2<>(x._1(), ((x._2()._1())/x._2()._2())));


            //Aggiorna Array List di centroidi con i nuovi
            for (Tuple2<Integer, Double> p : centroide_nuovo_final.distinct().collect()) {
                for (int i = 0; i < centroide.size(); i++)
                    if (p._1() == i) {
                        centroide.set(i, p._2());
                    }
            }
        }

        return cluster_1.collect();
    }

}


