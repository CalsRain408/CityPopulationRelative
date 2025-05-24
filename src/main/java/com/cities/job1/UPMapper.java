package com.cities.job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UPMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text country = new Text();
    private IntWritable population = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Convert the input line to string
        String line = value.toString();

        // Split by tab character
        String[] fields = line.split("\t");

        // Check if we have the expected number of fields (at least 7)
        if (fields.length >= 7) {
            try {
                // Extract fields according to the schema
                String cityURI = fields[0];
                String cityName = fields[1];
                String countryName = fields[2];
                String countryURI = fields[3];
                String cityPopulationStr = fields[4];
                String latitude = fields[5];
                String longitude = fields[6];

                // Check if population field is not empty
                if (cityPopulationStr != null && !cityPopulationStr.trim().isEmpty()) {
                    // Parse population as long to handle large cities
                    int cityPopulation = Integer.parseInt(cityPopulationStr.trim());

                    // Set country name as key
                    country.set(countryName);

                    // Set population as value
                    population.set(cityPopulation);

                    // Emit the key-value pair
                    context.write(country, population);

                    // Increment counter for successful records
                    context.getCounter("MapperCounters", "ValidRecords").increment(1);
                } else {
                    // Log missing population data
                    context.getCounter("MapperCounters", "MissingPopulation").increment(1);
                    System.err.println("Missing population for city: " + cityName);
                }

            } catch (NumberFormatException e) {
                // Handle invalid population format
                context.getCounter("MapperCounters", "InvalidPopulation").increment(1);
                System.err.println("Invalid population format in line: " + line);
            }
        } else {
            // Handle malformed lines
            context.getCounter("MapperCounters", "MalformedLines").increment(1);
            System.err.println("Malformed line with " + fields.length + " fields: " + line);
        }
    }
}

