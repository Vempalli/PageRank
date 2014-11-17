package com.IDS594.PageRank;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.google.common.collect.Lists;


/*
 * Input for Reducer:
 * 
 *  PageK	PageN1,RankN1/Nn1
 *  		PageN2,RankN2/Nn2
 *  		.....
 *  		PageAk,PageBk,PageCk…
 *  
 *  Output:
	
	node1, 0.3333333333333333	node2,node3
	node2, 0.3333333333333333	node1
	
 */
public class Reducer1 extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String rhsFinal = "";
		double rankK = 0.0;
		double beta = 0.85;
		List<Text> cache = Lists.newArrayList(values);
		for(int ind = 0; ind < cache.size(); ind++){
			//do : RankK += RankNi/Nni * beta
			String vals = cache.get(ind).toString();
			if(vals.contains("@")){
				String[] rhs = vals.split("@");
				double rankFrmInlink = Double.parseDouble(rhs[1]);
				rankK += (rankFrmInlink*beta); 
			}
			else{
				rhsFinal = vals;
			}
		}
		String lhs = key.toString() + ", "+ Double.toString(rankK);
		output.collect(new Text(lhs), new Text(rhsFinal));
		/*while(values.hasNext()){
			output.collect(new Text(key), new Text(values.next().toString()));
		}*/
		
	}
}