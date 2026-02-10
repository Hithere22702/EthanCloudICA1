package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private String mode;
    private int ratioScale;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mode = context.getConfiguration().get("mode", "airport");
        ratioScale = Math.max(1, context.getConfiguration().getInt("ratio.scale", 100));
    }

    public void reduce(Text text, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        if ("airport".equalsIgnoreCase(mode)) {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(text, new IntWritable(sum));
            return;
        }

        double totalDelay = 0d;
        int flightCount = 0;
        for (IntWritable value : values) {
            totalDelay += value.get();
            flightCount += 1;
        }

        if (flightCount == 0) {
            return;
        }

        double ratio = totalDelay / flightCount;
        int scaled = (int) Math.round(ratio * ratioScale);
        context.write(text, new IntWritable(scaled));
    }
}