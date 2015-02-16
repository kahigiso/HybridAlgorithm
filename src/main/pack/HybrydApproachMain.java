package main.pack;

import java.io.IOException;

import helper.pack.WordPairHybrid;
import helper.pack.WordPairMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HybrydApproachMain {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//		if(args.length!=2){
//			System.out.println("usage: [input] [output]");
//			System.exit(-1);
//			}
			System.out.println("========Hybrid Algorithm Approach=========");
			Job job = Job.getInstance(new Configuration());
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			
			job.setMapperClass(MapHybridApproach.class);
			job.setReducerClass(ReducerHybridAppraoch.class);
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setMapOutputKeyClass(WordPairHybrid.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(WordPairMap.class);
		
			Path outDir = new Path("files/output");
			FileSystem.get(job.getConfiguration()).delete(outDir,true);
	        FileInputFormat.setInputPaths(job, new Path("files/input"));
	        FileOutputFormat.setOutputPath(job, outDir);
	        
//	    	FileInputFormat.setInputPaths(job, new Path(args[0]));
//	      	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	        job.setJarByClass(HybrydApproachMain.class);
	        job.setJobName("Hybrid Algorithm Approach");
	        
	        System.exit(job.waitForCompletion(true)?0:1);
		

		}

}
