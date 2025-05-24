package com.cities.job3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CitiesCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;

        // Sum up the counts for this country
        for (IntWritable val : values) {
            sum += val.get();
        }

        result.set(sum);

        // Emit the country and count of large cities
        context.write(key, result);

        // Track statistics
        context.getCounter("ReducerCounters", "CountriesProcessed").increment(1);

        // Categorize countries by number of large cities
        if (sum == 0) {
            context.getCounter("ReducerCounters", "CountriesWithNoLargeCities").increment(1);
        } else if (sum >= 10) {
            context.getCounter("ReducerCounters", "CountriesWithManyLargeCities").increment(1);
        }
    }
}
