package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountTopKDriver extends Configured implements Tool {

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WordCountTopKDriver(), args);
		System.exit(res);
	}

	/**
	 * 
	 */
	public int run(String args[]) {
		try {
			if (args.length < 3) {
				System.err.println("Usage: <input> <intermediate> <outputBase> [topK] [ratioScale]");
				return 2;
			}

			String input = args[0];
			String intermediate = args[1];
			String baseOutput = args[2];
			int topK = (args.length > 3) ? Integer.parseInt(args[3]) : 3;
			int ratioScale = (args.length > 4) ? Integer.parseInt(args[4]) : 100;

			Path airportOut = new Path(intermediate + "/airport-count");
			Path airportTopkOut = new Path(baseOutput + "/airport-topk");
			Path airlineOut = new Path(intermediate + "/airline-ratio");
			Path airlineTopkOut = new Path(baseOutput + "/airline-topk");

			// Task 1: count flights per origin airport
			Configuration airportConf = new Configuration();
			airportConf.set("mode", "airport");
			Job airportJob = new Job(airportConf, "AirportFlightCount");
			airportJob.setJarByClass(WordCountTopKDriver.class);
			airportJob.setMapperClass(WordCountMapper.class);
			airportJob.setCombinerClass(WordCountReducer.class);
			airportJob.setReducerClass(WordCountReducer.class);
			airportJob.setOutputKeyClass(Text.class);
			airportJob.setOutputValueClass(IntWritable.class);
			airportJob.setInputFormatClass(TextInputFormat.class);
			airportJob.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(airportJob, new Path(input));
			FileOutputFormat.setOutputPath(airportJob, airportOut);

			if (!airportJob.waitForCompletion(true)) {
				return 1;
			}

			Configuration airportTopConf = new Configuration();
			airportTopConf.setInt("topk", topK);
			Job airportTopJob = new Job(airportTopConf, "AirportTopK");
			airportTopJob.setJarByClass(WordCountTopKDriver.class);
			airportTopJob.setMapperClass(TopKMapper.class);
			airportTopJob.setReducerClass(TopKReducer.class);
			airportTopJob.setOutputKeyClass(Text.class);
			airportTopJob.setOutputValueClass(IntWritable.class);
			airportTopJob.setNumReduceTasks(1);
			airportTopJob.setInputFormatClass(KeyValueTextInputFormat.class);
			airportTopJob.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(airportTopJob, airportOut);
			FileOutputFormat.setOutputPath(airportTopJob, airportTopkOut);

			if (!airportTopJob.waitForCompletion(true)) {
				return 1;
			}

			// Task 2: airline delay ratio (sum delays / flight count)
			Configuration airlineConf = new Configuration();
			airlineConf.set("mode", "airline");
			airlineConf.setInt("ratio.scale", ratioScale);
			Job airlineJob = new Job(airlineConf, "AirlineDelayRatio");
			airlineJob.setJarByClass(WordCountTopKDriver.class);
			airlineJob.setMapperClass(WordCountMapper.class);
			// No combiner to avoid skewing ratio
			airlineJob.setReducerClass(WordCountReducer.class);
			airlineJob.setOutputKeyClass(Text.class);
			airlineJob.setOutputValueClass(IntWritable.class);
			airlineJob.setInputFormatClass(TextInputFormat.class);
			airlineJob.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(airlineJob, new Path(input));
			FileOutputFormat.setOutputPath(airlineJob, airlineOut);

			if (!airlineJob.waitForCompletion(true)) {
				return 1;
			}

			Configuration airlineTopConf = new Configuration();
			airlineTopConf.setInt("topk", topK);
			Job airlineTopJob = new Job(airlineTopConf, "AirlineTopK");
			airlineTopJob.setJarByClass(WordCountTopKDriver.class);
			airlineTopJob.setMapperClass(TopKMapper.class);
			airlineTopJob.setReducerClass(TopKReducer.class);
			airlineTopJob.setOutputKeyClass(Text.class);
			airlineTopJob.setOutputValueClass(IntWritable.class);
			airlineTopJob.setNumReduceTasks(1);
			airlineTopJob.setInputFormatClass(KeyValueTextInputFormat.class);
			airlineTopJob.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(airlineTopJob, airlineOut);
			FileOutputFormat.setOutputPath(airlineTopJob, airlineTopkOut);

			return (airlineTopJob.waitForCompletion(true) ? 0 : 1);

		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during driver job.");
			e.printStackTrace();
			return 2;
		}
	}
}
