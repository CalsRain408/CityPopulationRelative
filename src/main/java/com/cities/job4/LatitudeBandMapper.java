package com.cities.job4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LatitudeBandMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text latitudeBand = new Text();
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
                String latitudeStr = fields[5];
                String longitude = fields[6];

                if (latitudeStr != null && !latitudeStr.trim().isEmpty()) {
                    double latitude = Double.parseDouble(latitudeStr.trim());

                    // Calculate the latitude band
                    String band = calculateLatitudeBand(latitude);

                    latitudeBand.set(band);
                    context.write(latitudeBand, one);

                    context.getCounter("MapperCounters", "ValidRecords").increment(1);

                    // Track hemisphere distribution
                    if (latitude >= 0) {
                        context.getCounter("MapperCounters", "NorthernHemisphereCities").increment(1);
                    } else {
                        context.getCounter("MapperCounters", "SouthernHemisphereCities").increment(1);
                    }
                } else {
                    context.getCounter("MapperCounters", "MissingLatitude").increment(1);
                }

            } catch (NumberFormatException e) {
                context.getCounter("MapperCounters", "InvalidLatitude").increment(1);
            }
        } else {
            context.getCounter("MapperCounters", "MalformedLines").increment(1);
        }
    }

    // Calculate the 10-degree latitude band for a given latitude
    private String calculateLatitudeBand(double latitude) {
        // Ensure latitude is within valid range [-90, 90]
        if (latitude < -90 || latitude > 90) {
            return "Invalid";
        }

        // Calculate the lower bound of the band
        int lowerBound = ((int) Math.floor(latitude / 10)) * 10;
        int upperBound = lowerBound + 10;

        // Handle edge cases
        if (lowerBound == -90) {
            return "-90° to -80°";
        } else if (lowerBound == 80) {
            return "80° to 90°";
        } else {
            // Format the band string
            String lowerStr = (lowerBound >= 0) ? lowerBound + "°" : lowerBound + "°";
            String upperStr = (upperBound >= 0) ? upperBound + "°" : upperBound + "°";
            return lowerStr + " to " + upperStr;
        }
    }
}
