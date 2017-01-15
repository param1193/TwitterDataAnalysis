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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PairPartitioner extends Partitioner<Text, DoubleWritable> {
	private Text temp = new Text();

	@Override
	public int getPartition(Text text, DoubleWritable value, int noOfPartitions) {
		temp.clear();
		temp.set(text.toString().split(",")[0]);
		return Math.abs(temp.hashCode()) % noOfPartitions;
	}

}
