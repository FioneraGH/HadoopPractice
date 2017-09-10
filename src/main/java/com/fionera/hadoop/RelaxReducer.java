package com.fionera.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RelaxReducer
        extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,
                          Context context) throws IOException, InterruptedException {
        StringBuilder list = new StringBuilder();
        for (Text value : values) {
            list.append(value).append(" ");
        }
        context.write(key, new Text(list.toString()));
    }
}
