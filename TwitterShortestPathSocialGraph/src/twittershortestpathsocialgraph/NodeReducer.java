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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NodeReducer extends
		Reducer<LongWritable, Text, LongWritable, Text> {
	Text word = new Text();

        @Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		long lowest = -1;
		long originDistance = -1;
		String node = null;
		for (Text t : values) {
			String data[] = t.toString().split(",");
			if (null != data[0]) 
                            switch (data[0]) {
                        case "node":
                            node = data[1];
                            break;
                        case "distance":
                            long distance = Long.parseLong(data[1]);
                            if (lowest == -1) {
                                lowest = distance;
                            } else {
                                if (lowest > distance) {
                                    lowest = distance;
                                }
                            }
                            break;
                        case "origin_distance":
                            originDistance = Long.parseLong(data[1]);
                            break;
                        default:
                            break;
                    }
		}
		if (originDistance > lowest && lowest > -1){
			context.getCounter("group", "isModified").increment(1);
		} else {
			lowest = originDistance;
		}
			

		word.clear();
		word.set(lowest + " " + node);
		context.write(key, word);
	}

}