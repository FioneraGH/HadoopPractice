package com.fionera.hadoop.mapred;

import com.fionera.hadoop.mapred.mapper.RelaxMapper;
import com.fionera.hadoop.mapred.reducer.RelaxReducer;
import com.fionera.hadoop.util.HDFSUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RelaxDriver
        extends Configured
        implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();


        Job job = Job.getInstance(configuration, "Relax");
        job.setJarByClass(RelaxDriver.class);

        if (HDFSUtil.checkFile(strings[1] + "/test")) {
            HDFSUtil.readFile(strings[1]);
        }
        FileInputFormat.addInputPath(job, new Path(strings[1]));
        FileOutputFormat.setOutputPath(job, new Path(strings[2]));

        job.setMapperClass(RelaxMapper.class);
        job.setReducerClass(RelaxReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

        return job.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if (otherArgs == null || otherArgs.length != 3) {
            System.out.println("There must be three params. <env> <in> <out>");
            System.exit(2);
        }

        switch (otherArgs[0]) {
            case "local":
                configuration.addResource("hadoop-local.xml");
                break;
            case "single":
                configuration.addResource("hadoop-single.xml");
                break;
            default:
                System.out.println("<env> must be \"local\" or \"single\".");
                System.exit(3);
        }
        int res = ToolRunner.run(configuration, new RelaxDriver(), args);
        System.exit(res);
    }
}
