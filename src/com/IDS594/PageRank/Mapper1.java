package com.IDS594.PageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
/*
 * Sample Input:
	node1, 0.3333333333333333	node2,node3
	node2, 0.3333333333333333	node1
	node3, 0.3333333333333333	
 */
/*Output of Mapper:
	for each outlink PageK
	PageK	PageN,RankN/Nn
	PageN	PageA,PageB,PageC…
 * 
 */
public class Mapper1 extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		String[] content = line.split("	");
		if(content.length > 1){
			String[] outlinks = content[1].split(",");
			String[] nodeConsidered = content[0].split(",");
			int Nn = outlinks.length;
			//double initRank = Double.parseDouble(nodeConsidered[1]);
			double Ninit = Double.parseDouble(nodeConsidered[1])/(double)Nn;
			//for each outlink pagek
			for(int index = 0; index < Nn; index++){
				String rhs = nodeConsidered[0]+"@"+ Double.toString(Ninit);
				String lhs = outlinks[index];
				output.collect(new Text(lhs), new Text(rhs));
			}
			output.collect(new Text(nodeConsidered[0]), new Text(content[1]));
		}
	}
}
