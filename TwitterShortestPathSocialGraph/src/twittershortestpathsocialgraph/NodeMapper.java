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
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NodeMapper extends Mapper<Object, Text, LongWritable, Text> {

	private Text word = new Text();
	private LongWritable lo = new LongWritable();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		if (!"".equals(value.toString())) {
			String data[] = value.toString().split("[ \t]+");
			long nodeId = Long.parseLong(data[0]);
			long distance = Long.parseLong(data[1]);
			String adjacencyList[] = data[2].split(":");

			word.clear();
			word.set("node," + data[2]);
			lo.set(nodeId);
			context.write(lo, word);

			word.clear();
			word.set("origin_distance," + distance);
			context.write(lo, word);

			distance++;
			word.clear();
			word.set("distance," + distance);
			for (int i = 0; i < adjacencyList.length; i++) {
				lo.set(Long.parseLong(adjacencyList[i]));
				context.write(lo, word);
			}
		}
	}
}