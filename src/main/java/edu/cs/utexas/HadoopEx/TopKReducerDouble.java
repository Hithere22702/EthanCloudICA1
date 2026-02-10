package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Top-K reducer that outputs double values (e.g., ratios) with a configurable scale.
 */
public class TopKReducerDouble extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    private PriorityQueue<WordAndCount> pq;
    private int topK;
    private int scale;

    private final Logger logger = Logger.getLogger(TopKReducerDouble.class);

    @Override
    public void setup(Context context) {
        pq = new PriorityQueue<>(10);
        topK = Math.max(1, context.getConfiguration().getInt("topk", 10));
        scale = Math.max(1, context.getConfiguration().getInt("ratio.scale", 100));
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for (IntWritable value : values) {
            pq.add(new WordAndCount(new Text(key), new IntWritable(value.get())));
            while (pq.size() > topK) {
                pq.poll();
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        List<WordAndCount> values = new ArrayList<>(pq.size());
        while (!pq.isEmpty()) {
            values.add(pq.poll());
        }

        // Highest first
        Collections.reverse(values);

        for (WordAndCount value : values) {
            double ratio = value.getCount().get() / (double) scale;
            context.write(value.getWord(), new DoubleWritable(ratio));
            logger.info("TopKReducerDouble output: " + value.getWord() + "  Ratio:" + ratio);
        }
    }
}
