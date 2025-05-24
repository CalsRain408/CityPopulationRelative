package com.cities.job2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.cities.job2.CityInfo;
import java.io.IOException;

public class LargestCityReducer extends Reducer<Text, CityInfo, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<CityInfo> values, Context context)
            throws IOException, InterruptedException {

        String largestCityName = "";
        int maxPopulation = 0;
        int cityCount = 0;

        // Find the city with maximum population for this country
        for (CityInfo cityInfo : values) {
            cityCount++;
            if (cityInfo.getPopulation() > maxPopulation) {
                maxPopulation = cityInfo.getPopulation();
                largestCityName = cityInfo.getCityName();
            }
        }

        // Format output
        String output = String.format("%s\t%d", largestCityName, maxPopulation);

        // Emit the country and its largest city
        context.write(key, new Text(output));

        // Track statistics
        context.getCounter("ReducerCounters", "CountriesProcessed").increment(1);
        context.getCounter("ReducerCounters", "TotalCitiesAnalyzed").increment(cityCount);
    }
}
