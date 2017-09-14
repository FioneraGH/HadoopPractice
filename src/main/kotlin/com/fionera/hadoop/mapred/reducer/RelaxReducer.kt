package com.fionera.hadoop.mapred.reducer

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

import java.io.IOException

class RelaxReducer : Reducer<Text, Text, Text, Text>() {

    @Throws(IOException::class, InterruptedException::class)
    override fun reduce(key: Text, values: Iterable<Text>,
                        context: Context) {
        val list = StringBuilder()
        for (value in values) {
            list.append(value).append(" ")
        }
        context.write(key, Text(list.toString()))
    }
}
