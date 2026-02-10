package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final int ORIGIN_INDEX = 7;
	private static final int AIRLINE_INDEX = 4;
	private static final int DEPARTURE_DELAY_INDEX = 11;
	private static final int CANCELLED_INDEX = 24;

	private final IntWritable outValue = new IntWritable(1);
	private final Text outKey = new Text();
	private String mode;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mode = context.getConfiguration().get("mode", "airport");
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();

		// Skip header
		if (line.startsWith("YEAR")) {
			return;
		}

		String[] fields = line.split(",", -1);

		if ("airport".equalsIgnoreCase(mode)) {
			if (fields.length <= ORIGIN_INDEX) {
				return;
			}
			String origin = fields[ORIGIN_INDEX].trim();
			if (origin.isEmpty()) {
				return;
			}
			outKey.set(origin);
			outValue.set(1);
			context.write(outKey, outValue);
			return;
		}

		// airline mode
		if (fields.length <= DEPARTURE_DELAY_INDEX || fields.length <= CANCELLED_INDEX) {
			return;
		}

		String cancelled = fields[CANCELLED_INDEX].trim();
		if ("1".equals(cancelled)) {
			return;
		}

		String airline = fields[AIRLINE_INDEX].trim();
		if (airline.isEmpty()) {
			return;
		}

		String delayStr = fields[DEPARTURE_DELAY_INDEX].trim();
		if (delayStr.isEmpty()) {
			return;
		}

		int delay;
		try {
			delay = (int) Math.round(Double.parseDouble(delayStr));
		} catch (NumberFormatException e) {
			return;
		}

		outKey.set(airline);
		outValue.set(delay);
		context.write(outKey, outValue);
	}
}