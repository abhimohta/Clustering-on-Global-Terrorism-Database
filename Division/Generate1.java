package xyz;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;
import java.net.*;


public class Generate1 extends Configured implements Tool{

public static int count=141966;
public static int k=1;
public static int l=1;
public static int r=1;
public static int index1=0;
public static int index2=1;
public static Vector<Point> centers = new Vector<Point>();

	public static class ImcdpMap extends Mapper<LongWritable, Text, Text, Text> {
		
		String record;
		
		protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
			record = value.toString();
			String [] record1 = record.split(",");
			String output = record1[index1]+","+record1[index2];
			//String[] fields = record.split(",");
			
			int j = count/k;
			int s_id;
			
			if(l<=j){
				s_id = r;
				l++;
			}else{
				l=1;
				r++;
				s_id=r;
			}
			
			
			context.write(new Text(String.valueOf(s_id)), new Text(output));
		} // end of map method
	} // end of mapper class
	

	public static class ImcdpReduce extends Reducer<Text, Text, Text, Text>  {
		
		protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
			//Integer s_id = key.get();
			Integer sum = 0;
			Integer cnt = 0;
			//Double [] avg = new Double[2];
			double a=0.0;
			double b=0.0;
			while(values.iterator().hasNext()) {
				String line = values.iterator().next().toString();
				String[] point = line.split(",");
				/*for(int i=0;i<2;i++){
					int x = Integer.parseInt(point[i]);
					avg[i]+=x;
				}*/
				a+=Integer.parseInt(point[0]);
				b+=Integer.parseInt(point[1]);	
				cnt = cnt + 1;
			}
			
			/*for(int i=0;i<2;i++)
				avg[i]=avg[i]/cnt;*/
			a=a/cnt;
			b=b/cnt;
			Point p = new Point();
			p.arr[0]=(int)a;
			p.arr[1]=(int)b;
			/*while(centers.contains(p)){
				p.arr[0]++;
				p.arr[1]++;
			}*/
			Point p_c = new Point();
			for(int i=0;i<centers.size();i++){
				p_c = centers.get(i);
				if(p_c.arr[0]==p.arr[0] && p_c.arr[1]==p.arr[1]){
					p.arr[0]++;
					p.arr[1]++;
				}
			}
			centers.add(p);
			//centers.add(point);
			//String output = String.valueOf(avg[0])+","+String.valueOf(avg[1]);
			context.write(new Text(String.valueOf(p.arr[0]).trim()), new Text(","+String.valueOf(p.arr[1]).trim()));
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		args = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = args[0];
		String output = args[1];
		k=Integer.parseInt(args[2]);
		index1 = Integer.parseInt(args[3]);
		index2 = Integer.parseInt(args[4]);
		Job job = new Job(conf, "Avg");
		job.setJarByClass(Generate1.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(ImcdpMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(ImcdpReduce.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(input));
		Path outPath = new Path(output);
		FileOutputFormat.setOutputPath(job, outPath);
		outPath.getFileSystem(conf).delete(outPath, true);
		
		job.waitForCompletion(true);
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Generate1(), args);
        System.exit(exitCode);
    }
}
