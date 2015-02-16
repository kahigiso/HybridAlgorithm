package main.pack;


import helper.pack.WordPairHybrid;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapHybridApproach extends Mapper<LongWritable, Text, WordPairHybrid, IntWritable> {
	
	private Map<WordPairHybrid,IntWritable> builder = new HashMap<WordPairHybrid,IntWritable>();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		List<String> itemIds = Arrays.asList(value.toString().trim().split(" "));	
		for(int k=0; k<itemIds.size();k++){
			String item = itemIds.get(k);
			List<String> neighbors = getNeighbors(k, itemIds) ;
			for(String neighbor : neighbors){
				WordPairHybrid hyb = new WordPairHybrid(item,neighbor);
				if(builder.containsKey(hyb)){
					builder.put(hyb,new IntWritable(((IntWritable)builder.get(hyb)).get()+1));
				}else builder.put(hyb,new IntWritable(1));
				context.write(hyb, builder.get(hyb));
			}
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
	   for (WordPairHybrid key : builder.keySet()) {
		   System.out.println("Mapper- key: "+key+" wordPairMap: "+builder.get(key));
		   context.write(key, builder.get(key));
	   }
	}
	
	private List<String> getNeighbors(Integer pos, List<String> items){
		List<String>  neighbors = new ArrayList<String>();
		if(pos == items.size()-1) return neighbors;
		for(int i =pos+1; i< items.size(); i++){
			if(items.get(i).equals(items.get(pos))) break;
			else neighbors.add(items.get(i));
		}
		return neighbors;
	}
}
