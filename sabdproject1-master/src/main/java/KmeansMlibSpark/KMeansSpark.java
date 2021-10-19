package KmeansMlibSpark;

import org.apache.spark.mllib.clustering.KMeansModel;

public class KMeansSpark {
    private static Integer num_cluster=4;
    private static Integer num_iterazioni=20;
    private KMeansModel kMeansModel;

    public KMeansSpark(KMeansModel kMeansModel){
        this.kMeansModel=kMeansModel;
    }

    public KMeansModel getkMeansModel() {
        return kMeansModel;
    }
}


