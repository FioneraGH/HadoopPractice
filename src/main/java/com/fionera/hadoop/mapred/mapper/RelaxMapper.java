package com.fionera.hadoop.mapred.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RelaxMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {
        String line = value.toString();

        try {
            String[] lineSplit = line.split(" ");
            String _id = lineSplit[0];
            String _price = lineSplit[1];
            String _date = lineSplit[2];

            context.write(new Text(_id), new Text(_date + "_" + _price));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
