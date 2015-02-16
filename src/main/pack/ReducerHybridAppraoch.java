package main.pack;

import helper.pack.WordPairHybrid;
import helper.pack.WordPairMap;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerHybridAppraoch extends Reducer<WordPairHybrid, IntWritable, Text, WordPairMap> {

	private Integer marginal = 0;
	private String currentTerm = null;
	private Map<String, Double> associtar = new HashMap<String, Double>();
	
	public void reduce(WordPairHybrid key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		if(currentTerm == null) currentTerm = key.getKey().toString();
		else if(currentTerm != null && !currentTerm.equals(key.getKey().toString())){
			
			WordPairMap wordPairMap = new WordPairMap();
			for(String set: associtar.keySet()){
				Double dwri =((double) associtar.get(set)/marginal)*10;
				Text textKey = new Text(set);
				wordPairMap.put(textKey,new IntWritable(dwri.intValue()));
			}	
			System.out.println("Reducer - currentTerm: "+currentTerm+" wordPairMap: "+wordPairMap);
			context.write(new Text(currentTerm), wordPairMap);
			marginal = 0;
			associtar.clear();
			currentTerm = key.getKey().toString();
		}
		
		for (IntWritable val : values) {
			if(associtar.containsKey(
					key.getValue().toString())){ associtar.put(key.getValue().toString(),associtar.get(key.getValue().toString())+1);
					marginal += val.get();
			}else {
				associtar.put(key.getValue().toString(), (double)val.get());
				marginal += val.get();
			}
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		WordPairMap wordPairMap = new WordPairMap();	
		for(String set: associtar.keySet()){
			Double dwri = ((double) associtar.get(set)/marginal)*10;
			Text textKey = new Text(set);
			wordPairMap.put(textKey,new IntWritable(dwri.intValue()));
		}
		System.out.println("Reducer - currentTerm: "+currentTerm+" wordPairMap: "+wordPairMap);
		context.write(new Text(currentTerm), wordPairMap);
	}
}
