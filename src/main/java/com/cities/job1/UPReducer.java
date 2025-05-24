package com.cities.job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UPReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable totalPopulation = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        int cityCount = 0;

        // Sum all city populations for this country
        for (IntWritable population : values) {
            sum += population.get();
            cityCount++;
        }

        // Set the total urban population
        totalPopulation.set(sum);

        // Emit the country and its total urban population
        context.write(key, totalPopulation);

        // Track statistics using counters
        context.getCounter("ReducerCounters", "CountriesProcessed").increment(1);
        context.getCounter("ReducerCounters", "TotalCities").increment(cityCount);

        // Log countries with high urban population (optional)
        if (sum > 100000000) { // Countries with > 100 million urban population
            context.getCounter("ReducerCounters", "HighUrbanPopCountries").increment(1);
        }
    }
}
