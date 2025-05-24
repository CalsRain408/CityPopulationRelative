package com.cities.job4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LatitudeBandReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;

        // Sum up the counts for this latitude band
        for (IntWritable val : values) {
            sum += val.get();
        }

        result.set(sum);

        // Emit the latitude band and count
        context.write(key, result);

        // Track statistics
        context.getCounter("ReducerCounters", "LatitudeBandsProcessed").increment(1);

        // Track high-density bands
        if (sum > 100) {
            context.getCounter("ReducerCounters", "HighDensityBands").increment(1);
        }
    }
}
