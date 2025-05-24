package com.cities.job2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CityInfo implements Writable {
    private Text cityName;
    private IntWritable population;

    public CityInfo() {
        this.cityName = new Text();
        this.population = new IntWritable();
    }

    public CityInfo(String cityName, int population) {
        this.cityName = new Text(cityName);
        this.population = new IntWritable(population);
    }

    public String getCityName() {
        return cityName.toString();
    }

    public int getPopulation() {
        return population.get();
    }

    public void setCityName(String name) {
        this.cityName.set(name);
    }

    public void setPopulation(int pop) {
        this.population.set(pop);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        cityName.write(out);
        population.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        cityName.readFields(in);
        population.readFields(in);
    }

    @Override
    public String toString() {
        return cityName.toString() + "\t" + population.get();
    }
}
