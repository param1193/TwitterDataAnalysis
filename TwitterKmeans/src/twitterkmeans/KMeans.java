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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.Map;


public class KMeans {

    public static final String INPUT_PATH = "C:/Users/param//Desktop/hadoop/FinalProject/data/Kmeans/users500.txt";
    public static final String OUTPUT_PATH = "C:/Users/param//Desktop/hadoop/FinalProject/data/Kmeans/output";
    public static final int NUM_CLUSTERS = 10;




    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();



        String inputPath = "C:/Users/param//Desktop/hadoop/FinalProject/data/Kmeans/users500.txt";
        String outputPath = "C:/Users/param//Desktop/hadoop/FinalProject/data/Kmeans/output";


        int K = 10;
        boolean isConverged = false;
        int maxIterations = 50;
        boolean iterationFinished = true;
        int numIteration = 0;
        Map<Integer, Double> tempCentroids = KMeansUtil.getInitialCentroids(NUM_CLUSTERS);
      
        KMeansUtil.showOutput(tempCentroids);

        while(!isConverged && iterationFinished && (numIteration < maxIterations)) {
            numIteration++;
    
            conf.setInt("iteration", numIteration);
            KMeansUtil.writeCentroids(conf, tempCentroids);
            Job job = Job.getInstance(conf);
            job.setJarByClass(KMeans.class);

            job.setMapperClass(KMeansMapper.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(LongWritable.class);

            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
            String iterOutputPath = OUTPUT_PATH + numIteration;
            deleteFolder(conf, iterOutputPath);
            FileOutputFormat.setOutputPath(job, new Path(iterOutputPath));

            iterationFinished = job.waitForCompletion(true);
            long convergeCount = job.getCounters().findCounter(KMeansReducer.Convergence.CONVERGENT).getValue();
            if(convergeCount == NUM_CLUSTERS) {
                isConverged = true;
               
            }

            tempCentroids = KMeansUtil.readCentroids(job);
            KMeansUtil.showOutput(tempCentroids);
        }

        KMeansUtil.showOutput(tempCentroids);

    }

   
    private static void deleteFolder(Configuration conf, String folderPath ) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(folderPath);
        if(fs.exists(path)) {
            fs.delete(path,true);
        }
    }
}
