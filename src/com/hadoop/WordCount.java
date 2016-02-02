package com.hadoop;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//数一堆文件中单词各有多少个
public class WordCount {
	
//	Text相当于jdk中的String ；IntWritable相当于jdk的int类型
	/*
	BooleanWritable：标准布尔型数值
    ByteWritable：单字节数值
    DoubleWritable：双字节数
    FloatWritable：浮点数
    IntWritable：整型数
    LongWritable：长整型数
    Text：使用UTF8格式存储的文本
    */
	public static class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		//声明一个IntWritable变量，作计数用，每出现一个key，给其一个value=1的值  
		private final IntWritable one = new IntWritable(1);
		//用来暂存map输出中的key值，Text类型的
		private Text word = new Text();

		//map打散的过程
		public void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {
			
			String line = value.toString();
			//Hadoop读入的value是以行为单位的，其key为该行所对应的行号，因为我们要计算每个单词的数目，
			//默认以空格作为间隔，故用StringTokenizer辅助做字符串的拆分，也可以用string.split("")来代替
			StringTokenizer token = new StringTokenizer(line);
			//遍历一下每行字符串中的单词
			while (token.hasMoreTokens()) {
				//出现一个单词就给它设成一个key并将其值设为1
				word.set(token.nextToken());
				//输出设为key/value形式
				context.write(word, one);
			}
		}
	}

	public static class WordCountReduce extends	Reducer<Text, IntWritable, Text, IntWritable> {
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable val : values) {
				//由于map的打散，这里会得到如，{key,values}={"hello",{1,1,....}},这样的集合
				//这里需要逐一将它们的value取出来予以相加，取得总的出现次数，即为总和
				sum += val.get();
			}
			// key/value形式    单词/总数
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		//取得系统的参数
		Configuration conf = new Configuration();
//		初始化job
		Job job = new Job(conf);
//		执行WordCount.class这个字节码文件
		job.setJarByClass(WordCount.class);
		job.setJobName("wordcount");
		
		//在reduce的输出时，key的输出类型为Text
		job.setOutputKeyClass(Text.class);
		//在reduce的输出时,value的输出类型为IntWritable
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(WordCountMap.class);
		job.setReducerClass(WordCountReduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//初始化输入文件路径（要计算的）
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//初始化文件计算之后的结果的输出路径 
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//提交job到hadoop上执行，执行完退出
		System.exit(job.waitForCompletion(true) ? 0 : 1); 
//		job.waitForCompletion(true);
	}
}
