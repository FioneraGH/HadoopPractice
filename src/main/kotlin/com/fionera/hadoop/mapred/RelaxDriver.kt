package com.fionera.hadoop.mapred

import com.fionera.hadoop.mapred.mapper.RelaxMapper
import com.fionera.hadoop.mapred.reducer.RelaxReducer
import com.fionera.hadoop.util.HDFSUtil

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner

class RelaxDriver : Configured(), Tool {

    @Throws(Exception::class)
    override fun run(strings: Array<String>): Int {
        val configuration = conf

        val job = Job.getInstance(configuration, "Relax")
        job.setJarByClass(RelaxDriver::class.java)

        if (HDFSUtil.checkFile(strings[1] + "/test")) {
            HDFSUtil.readFile(strings[1])
        }
        FileInputFormat.addInputPath(job, Path(strings[1]))
        FileOutputFormat.setOutputPath(job, Path(strings[2]))

        job.mapperClass = RelaxMapper::class.java
        job.reducerClass = RelaxReducer::class.java
        job.outputFormatClass = TextOutputFormat::class.java
        job.outputKeyClass = Text::class.java
        job.outputValueClass = Text::class.java

        job.waitForCompletion(true)

        return if (job.isSuccessful) 0 else 1
    }

    companion object {

        @Throws(Exception::class)
        @JvmStatic
        fun main(args: Array<String>) {
            val configuration = Configuration()
            val otherArgs = GenericOptionsParser(configuration, args).remainingArgs
            if (otherArgs == null || otherArgs.size != 3) {
                println("There must be three params. <env> <in> <out>")
                System.exit(2)
            }

            when (otherArgs!![0]) {
                "local" -> configuration.addResource("hadoop-local.xml")
                "single" -> configuration.addResource("hadoop-single.xml")
                else -> {
                    println("<env> must be \"local\" or \"single\".")
                    System.exit(3)
                }
            }
            val res = ToolRunner.run(configuration, RelaxDriver(), args)
            System.exit(res)
        }
    }
}
