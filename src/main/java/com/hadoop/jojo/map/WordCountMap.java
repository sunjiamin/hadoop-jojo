package com.hadoop.jojo.map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;
import java.util.StringTokenizer;

/**
 * description:
 *
 *
 * Mapper区: WordCount程序 Map 类
 * Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>:
 *        |       |           |             |
 *  输入key类型  输入value类型      输出key类型 输出value类型
 *
 * @author sunjiamin
 * @date 2018-05-09 09:31
 */
public class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     *   输出结果 KEYOUT
     */
    private Text word = new Text();
    /**
     * 因为若每个单词出现后，就置为 1，并将其作为一个<key,value>对，因此可以声明为常量，值为 1  // VALUEOUT
     */
    private final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取每行数据的值
        String lineValue = value.toString();
        // 分词：将每行的单词进行分割,按照"  \t\n\r\f"(空格、制表符、换行符、回车符、换页)进行分割
        StringTokenizer tokenizer = new StringTokenizer(lineValue);
        while (tokenizer.hasMoreTokens()) {
            // 获取每个值
            String wordValue = tokenizer.nextToken();
            word.set(wordValue);
            context.write(word,one);
        }
    }
}
