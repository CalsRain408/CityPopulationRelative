package com.cities.job1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MainDriver
{
    /**
     * @param args
     *
     * Requires two parameters passed on the command line: the name of the
     * input file and the name of the output file.
     */
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: UrbanPopulationDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // Set the starting class to run with this job.
        Job job = Job.getInstance(conf);
        job.setJarByClass(MainDriver.class);
        job.setJobName("UrbanPopulation");

        // Set the key/value types for the overall job output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set the mapper and reducer classes to use
        job.setMapperClass(UPMapper.class);
        job.setReducerClass(UPReducer.class);

        // Set the input and output formats (tab-separated,
        // newline-delimited text files)
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set the locations in the DFS of the input and output files
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Run the job and wait for it to finish
        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}
