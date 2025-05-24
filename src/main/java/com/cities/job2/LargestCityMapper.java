package com.cities.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.cities.job2.CityInfo;
import java.io.IOException;

public class LargestCityMapper extends Mapper<LongWritable, Text, Text, CityInfo> {
    private Text country = new Text();
    private CityInfo cityInfo = new CityInfo();

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

                    // Set country name as key
                    country.set(countryName);

                    // Set city info as value
                    cityInfo.setCityName(cityName);
                    cityInfo.setPopulation(cityPopulation);

                    // Emit the key-value pair
                    context.write(country, cityInfo);

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
