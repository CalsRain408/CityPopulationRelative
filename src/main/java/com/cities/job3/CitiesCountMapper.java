package com.cities.job3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CitiesCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int POPULATION_THRESHOLD = 100000;
    private Text country = new Text();
    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = line.split("\t");

        if (fields.length >= 7) {
            try {
                String cityURI = fields[0];
                String cityName = fields[1];
                String countryName = fields[2];
                String countryURI = fields[3];
                String cityPopulationStr = fields[4];
                String latitude = fields[5];
                String longitude = fields[6];

                if (cityPopulationStr != null && !cityPopulationStr.trim().isEmpty()) {
                    int cityPopulation = Integer.parseInt(cityPopulationStr.trim());

                    // Only emit if population is greater than threshold
                    if (cityPopulation > POPULATION_THRESHOLD) {
                        country.set(countryName);
                        context.write(country, one);

                        context.getCounter("MapperCounters", "LargeCities").increment(1);
                    } else {
                        context.getCounter("MapperCounters", "SmallCities").increment(1);
                    }

                    context.getCounter("MapperCounters", "ValidRecords").increment(1);
                } else {
                    context.getCounter("MapperCounters", "MissingPopulation").increment(1);
                }

            } catch (NumberFormatException e) {
                context.getCounter("MapperCounters", "InvalidPopulation").increment(1);
            }
        } else {
            context.getCounter("MapperCounters", "MalformedLines").increment(1);
        }
    }
}
