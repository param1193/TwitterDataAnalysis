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
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HashTagMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	static Pattern p = Pattern.compile("#([^\\s]+)");

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String temp_url = value.toString();
		String temp = temp_url.replaceAll("http[s]?://t.co/[A-Za-z0-9]+", " ");
		temp = temp.replaceAll("[^A-Za-z0-9@#]", " ");
		temp = temp.replaceAll("#+", "#");
		Matcher match = p.matcher(temp);
		while(match.find()){
			word.clear();
			word.set(match.group());
			context.write(word, one);
		}
	}
}