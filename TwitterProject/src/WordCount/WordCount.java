/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package WordCount;

/**
 *
 * @author param
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class WordCount {

	

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		
		String inputPathWordCount = "C:/Users/param//Desktop/hadoop/FinalProject/data/tweets.txt";
		String outputPathWordCount = "C:/Users/param//Desktop/hadoop/FinalProject/data/output/original";
		String inputPathWordCountSort = "C:/Users/param//Desktop/hadoop/FinalProject/data/final_sort";
		String outputPathWordCountSort = "C:/Users/param//Desktop/hadoop/FinalProject/data/output/final_sort";
		String inputPathUsersTweet = "C:/Users/param//Desktop/hadoop/FinalProject/data/final_tweet";
		String outputPathUsersTweet = "C:/Users/param//Desktop/hadoop/FinalProject/data/output/final_tweet";
		String outputPathUsersTweetSort = "/output/final_tweet/sort";
		String inputPathHashTags = "C:/Users/param//Desktop/hadoop/FinalProject/data/final_hash";
		String outputPathHashTags = "C:/Users/param//Desktop/hadoop/FinalProject/data/output/final_hash";
		String outputPathHashTagSort = "C:/Users/param//Desktop/hadoop/FinalProject/data/output/final_hash/sort";
		String output = "C:/Users/param//Desktop/hadoop/FinalProject/data/output";
		
		deleteFolder(conf, outputPathWordCount);
		deleteFolder(conf, outputPathWordCountSort);
		deleteFolder(conf, outputPathUsersTweet);
		deleteFolder(conf, outputPathHashTags);
		deleteFolder(conf, outputPathUsersTweetSort);
		deleteFolder(conf, outputPathHashTagSort);
		deleteFolder(conf, inputPathWordCountSort);
		deleteFolder(conf, inputPathUsersTweet);
		deleteFolder(conf, inputPathHashTags);
		deleteFolder(conf, output);

	

		Job job = Job.getInstance(conf);
		Job job2 = null, job3 = null, job4 = null;
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPathWordCount));
		FileOutputFormat.setOutputPath(job, new Path(outputPathWordCount));

		if (job.waitForCompletion(true)) {
			job2 = Job.getInstance(conf);
			job2.setJarByClass(WordCount.class);
			job2.setMapperClass(SortingMapper.class);
			job2.setCombinerClass(SortReducer.class);
			job2.setReducerClass(SortReducer.class);
			job2.setOutputKeyClass(IntWritable.class);
			job2.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job2, new Path(outputPathWordCount));
			FileOutputFormat.setOutputPath(job2, new Path(outputPathWordCountSort));
		}

		if (job2.waitForCompletion(true)) {
			job3 = Job.getInstance(conf);
			job3.setJarByClass(WordCount.class);
			job3.setMapperClass(HashTagMapper.class);
			job3.setCombinerClass(HashTagReducer.class);
			job3.setReducerClass(HashTagReducer.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job3, new Path(inputPathWordCount));
			FileOutputFormat.setOutputPath(job3, new Path(outputPathHashTags));
			if(job3.waitForCompletion(true)){
				job3 = Job.getInstance(conf);
				job3.setJarByClass(WordCount.class);
				job3.setMapperClass(SortingMapper.class);
				job3.setCombinerClass(SortReducer.class);
				job3.setReducerClass(SortReducer.class);
				job3.setOutputKeyClass(IntWritable.class);
				job3.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(job3, new Path(outputPathHashTags));
				FileOutputFormat.setOutputPath(job3, new Path(outputPathHashTagSort));
			}
		}
		
		if (job3.waitForCompletion(true)) {
			job4 = Job.getInstance(conf);
			job4.setJarByClass(WordCount.class);
			job4.setMapperClass(TweetMapper.class);
			job4.setCombinerClass(TweetReducer.class);
			job4.setReducerClass(TweetReducer.class);
			job4.setOutputKeyClass(Text.class);
			job4.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job4, new Path(inputPathWordCount));
			FileOutputFormat.setOutputPath(job4, new Path(outputPathUsersTweet));
			if(job4.waitForCompletion(true)){
				job4 = Job.getInstance(conf);
				job4.setJarByClass(WordCount.class);
				job4.setMapperClass(SortingMapper.class);
				job4.setCombinerClass(SortReducer.class);
				job4.setReducerClass(SortReducer.class);
				job4.setOutputKeyClass(IntWritable.class);
				job4.setOutputValueClass(Text.class);
				FileInputFormat.addInputPath(job4, new Path(outputPathUsersTweet));
				FileOutputFormat.setOutputPath(job4, new Path(outputPathUsersTweetSort));
			}
			System.exit(job4.waitForCompletion(true) ? 0 : 1);
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