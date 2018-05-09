package com.hadoop.jojo;

import com.hadoop.jojo.map.WordCountMap;
import com.hadoop.jojo.reduce.WordCountReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Hello world!
 *
 * @author sunjiamin
 * @date 2018-05-09 09:31
 */
public class WordCount
{
    public static void main( String[] args ) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }

        // 创建一个 Job
        Job job = Job.getInstance(conf,"word count jojo");
        // 1. 设置 Job 运行的类
        job.setJarByClass(WordCount.class);
        // 2. 设置Mapper类和Reducer类
        job.setMapperClass(WordCountMap.class);
        job.setCombinerClass(WordCountReduce.class);
        job.setReducerClass(WordCountReduce.class);

        // 3. 设置输出结果 key 和 value 的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 4. 获取输入参数，设置输入文件目录和输出文件目录 支持多个文件输入
        for(int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

        // 5. 提交 job，等待运行结果，并在客户端显示运行信息，最后结束程序
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
