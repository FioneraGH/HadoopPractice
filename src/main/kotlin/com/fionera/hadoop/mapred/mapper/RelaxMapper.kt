package com.fionera.hadoop.mapred.mapper

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

import java.io.IOException

class RelaxMapper : Mapper<LongWritable, Text, Text, Text>() {

    @Throws(IOException::class, InterruptedException::class)
    override fun map(key: LongWritable, value: Text,
                     context: Context) {
        val line = value.toString()

        try {
            val lineSplit = line.split(" ".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            val _id = lineSplit[0]
            val _price = lineSplit[1]
            val _date = lineSplit[2]

            context.write(Text(_id), Text(_date + "_" + _price))
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }
}
