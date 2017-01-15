/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterwordcooccurence;

/**
 *
 * @author param
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class WordCount {

	

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		
		String inputPathWordCount = "C:/Users/param//Desktop/hadoop/FinalProject/data/tweets.txt";
		String outputPathWordCount = "C:/Users/param//Desktop/hadoop/FinalProject/data/output";
		String outputPathPair = "C:/Users/param//Desktop/hadoop/FinalProject/data/output/pair";
		String outputPathStripes = "C:/Users/param//Desktop/hadoop/FinalProject/data/output/stripes";

		
		deleteFolder(conf, outputPathWordCount);
		deleteFolder(conf, outputPathPair);
		deleteFolder(conf, outputPathStripes);
		deleteFolder(conf, outputPathWordCount);

		Job job = Job.getInstance(conf);
		Job job2 = null;
		job.setJarByClass(WordCount.class);
		job.setMapperClass(PairMapper.class);
	
		job.setReducerClass(PairReducer.class);
		job.setNumReduceTasks(2);
		job.setPartitionerClass(PairPartitioner.class);
		job.setSortComparatorClass(PairComparator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPathWordCount));
		FileOutputFormat.setOutputPath(job, new Path(outputPathPair));
		

		if (job.waitForCompletion(true)) {
			job2 = Job.getInstance(conf);
			job2.setJarByClass(WordCount.class);
			job2.setMapperClass(StripeMapper.class);
			job2.setCombinerClass(StripeReducer.class);
			job2.setReducerClass(StripeReducer.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job2, new Path(inputPathWordCount));
			FileOutputFormat.setOutputPath(job2, new Path(outputPathStripes));
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
	}

	
	private static void deleteFolder(Configuration conf, String folderPath)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}
}