/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterkmeans;

/**
 *
 * @author param
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;


public class KMeansUtil {
   

    public static final double THRESHOLD = 0.005;
    public static final String KEY_COUNTER = "CENTROID_";
    public static final String KEY_GROUP = "KMEANS";

    public static final String CONF_NUM_CENTROIDS = "iteration.centroids.count";
    public static final String CONF_CENTROID_KEY = "iteration.centroid.";

    public static Map<Integer, Double> getInitialCentroids(int numClusters) {
        Map<Integer, Double> initCentroids = new HashMap<Integer, Double>();
        int i = 1;
        long v = 0;
        while(i <= numClusters) {
            initCentroids.put(i, (double)v);
            v += 1000*(++i);
        }
        return initCentroids;
    }

    public static Map<Integer, Double> readCentroids(Job job) throws IOException {
      
        Map<Integer, Double> iterCentroids = new HashMap<Integer, Double>();
        int num = job.getConfiguration().getInt(CONF_NUM_CENTROIDS, -1);
    
        int i = 1;
        while(i <= num) {
            long value = job.getCounters().findCounter(KEY_GROUP, KEY_COUNTER + i).getValue();
            double dblValue = value/1000;
            
            iterCentroids.put(i, dblValue);
            i++;
        }

        return iterCentroids;
    }

    public static Reducer.Context writeCentroid(Reducer.Context context, int key, Double value) {
        long undecimalVal = undecimal(value);
        System.out.println("KMeansUtil: Modifying counter in Reducer for centroid " + key + " from " +
                context.getCounter(KEY_GROUP, KEY_COUNTER + key).getValue() +
                " to " + value + " as " + undecimalVal);
        context.getCounter(KEY_GROUP, KEY_COUNTER + key).setValue(undecimalVal);
        return context;
    }

    private static long undecimal(double decimalVal) {
        long undecimalVal = -1;
        try {
            DecimalFormat df = new DecimalFormat("#.###");
            df.setMaximumFractionDigits(3);
            df.setRoundingMode(RoundingMode.UP);
            String formattedVal = df.format(decimalVal);
            double formattedDecimalVal = Double.valueOf(formattedVal);
            undecimalVal = (long)(formattedDecimalVal * 1000);
        } catch (Exception e) {
            undecimalVal = -1;
        }
        return undecimalVal;
    }

    public static void writeCentroids(Configuration conf, Map<Integer, Double> tempCentroids) {
        int num = tempCentroids.size();
        conf.setInt(CONF_NUM_CENTROIDS, num);

        int i = 1;
        for(Integer entry : tempCentroids.keySet()) {
            double decimalVal = tempCentroids.get(entry);
            conf.setDouble(CONF_CENTROID_KEY + i, decimalVal);
           
            i++;
        }
    }

    public static Map<Integer, Double> readCentroids(Configuration conf) {
        Map<Integer, Double> iterCentroids = new HashMap<Integer, Double>();
        int num = conf.getInt(CONF_NUM_CENTROIDS, -1);
        System.out.println("KMeansUtil: Reading " + num + " centroids from Configuration");
        boolean isNeg = false;
        if(num > 0) {
            int i = 1;
            while(i <= num) {
                double centroid = conf.getDouble(CONF_CENTROID_KEY + i, -1.0);
                if(centroid >= 0) {
                    System.out.println("KMeansUtil: Found Centroid " + i + " : " + centroid + " in Configuration");
                    iterCentroids.put(i, centroid);
                    i++;
                    isNeg = false;
                } else {
                    if(!isNeg) {
                        System.out.println("KMeansUtil: Centroid not found for " + CONF_CENTROID_KEY + i);
                        isNeg = true;
                    } else {
                        System.out.print("."+i);
                    }
                    i++;
                }
            }
        }
        return iterCentroids;
    }

    public static void showOutput(Map<Integer, Double> tempCentroids) {
        for(Map.Entry<Integer, Double> entry : tempCentroids.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }
}

