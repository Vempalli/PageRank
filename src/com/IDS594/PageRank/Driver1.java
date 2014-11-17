package com.IDS594.PageRank;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Driver1 extends Configured implements Tool {
	public int run(String[] args) throws Exception {

		// creating a JobConf object and assigning a job name for identification
		// purposes
		JobConf conf = new JobConf(getConf(), Driver1.class);
		conf.setJobName("Job1");
		// Setting configuration object with the Data Type of output Key and
		// Value
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		// Providing the mapper and reducer class names
		conf.setJarByClass(Driver1.class);
		conf.setMapperClass(Mapper1.class);
		conf.setReducerClass(Reducer1.class);
		// the hdfs input and output directory to be fetched from the command
		// line
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		/*
		// job2
		JobConf conf1 = new JobConf(getConf(), Driver1.class);
		conf1.setJobName("Job2");
		// Setting configuration object with the Data Type of output Key and
		// Value
		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(Text.class);
		// Providing the mapper and reducer class names
		conf1.setJarByClass(Driver1.class);
		conf1.setMapperClass(Mapper2.class);
		conf1.setReducerClass(Reducer2.class);
		// the hdfs input and output directory to be fetched from the command
		// line
		FileInputFormat.addInputPath(conf1, new Path(args[1]
				+ "//temp//part-00000"));
		FileOutputFormat.setOutputPath(conf1, new Path(args[1] + "//final"));
		JobClient.runJob(conf1);*/
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Driver1(), args);
		System.exit(res);
	}
}