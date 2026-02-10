package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


import org.apache.log4j.Logger;


public class TopKMapper extends Mapper<Text, Text, Text, IntWritable> {

	private Logger logger = Logger.getLogger(TopKMapper.class);


	private PriorityQueue<WordAndCount> pq;
	private int topK;

	public void setup(Context context) {
		pq = new PriorityQueue<>();
		topK = Math.max(1, context.getConfiguration().getInt("topk", 10));

	}

	/**
	 * Reads in results from the first job and filters the topk results
	 *
	 * @param key
	 * @param value a float value stored as a string
	 */
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {
		String raw = value.toString().trim();
		if (raw.isEmpty()) {
			return;
		}

		int count;
		try {
			// Allow both integer counts and scaled ratios; fall back to parsing as double if needed
			count = Integer.parseInt(raw);
		} catch (NumberFormatException ex) {
			try {
				double parsed = Double.parseDouble(raw);
				count = (int) Math.round(parsed);
			} catch (NumberFormatException ignored) {
				// Skip malformed rows instead of failing the job
				logger.warn("TopKMapper skipping non-numeric value: " + raw);
				return;
			}
		}

		pq.add(new WordAndCount(new Text(key), new IntWritable(count)) );

		if (pq.size() > topK) {
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {


		while (pq.size() > 0) {
			WordAndCount wordAndCount = pq.poll();
			context.write(wordAndCount.getWord(), wordAndCount.getCount());
			logger.info("TopKMapper PQ Status: " + pq.toString());
		}
	}

}