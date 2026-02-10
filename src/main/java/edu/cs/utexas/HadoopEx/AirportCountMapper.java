package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for counting flights per origin airport.
 */
public class AirportCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int ORIGIN_INDEX = 7;
    private static final IntWritable ONE = new IntWritable(1);
    private final Text airport = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Skip header
        if (line.startsWith("YEAR")) {
            return;
        }

        String[] fields = line.split(",", -1);
        if (fields.length <= ORIGIN_INDEX) {
            return;
        }

        String origin = fields[ORIGIN_INDEX].trim();
        if (origin.isEmpty()) {
            return;
        }

        airport.set(origin);
        context.write(airport, ONE);
    }
}
