package com.hadoop.jojo.reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * description:
 *    Reducer 区域：WordCount 程序 Reduce 类
 *    Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>:Map 的输出类型，就是Reduce 的输入类型
 * @author sunjiamin
 * @date 2018-05-09 09:31
 */
public class WordCountReduce  extends Reducer<Text,IntWritable,Text,IntWritable>{
    /**
     *  输出结果：总次数
     */
    private IntWritable result = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 累加器，累加每个单词出现的总次数
        int sum = 0;
        // 遍历values
        for (IntWritable val : values) {
            // 累加
            sum += val.get();
        }
        // 设置输出 value
        result.set(sum);
        // 上下文输出 reduce 结果
        context.write(key, result);
    }
}
