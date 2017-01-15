/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package twittershortestpathsocialgraph;

/**
 *
 * @author param
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ShortestPath {



	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		
		
		String inputPathSocialGraph = "C:/Users/param//Desktop/hadoop/FinalProject/data/SocialGraph/input-graph-large";
		String outputPathSocialGraph = "C:/Users/param//Desktop/hadoop/FinalProject/data/SocialGraph/output";
		
		deleteFolder(conf, outputPathSocialGraph);

		
		boolean isCompleted = false;
		int count = 0;
		while (isCompleted == false) {
			String input = null;
			String output = null;
			if (count == 0) {
				input = inputPathSocialGraph;
			} else {
				input = outputPathSocialGraph + "/" + count;
			}
			count++;
			output = outputPathSocialGraph + "/" + count;
			deleteFolder(conf, output);
			Job job = Job.getInstance(conf);
			job.setJarByClass(ShortestPath.class);
			job.setMapperClass(NodeMapper.class);
			job.setReducerClass(NodeReducer.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			boolean isJobCompleted = job.waitForCompletion(true);
			if (isJobCompleted) {
				isCompleted = (job.getCounters().getGroup("group").findCounter("isModified").getValue() > 0 ? false : true);
			}
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