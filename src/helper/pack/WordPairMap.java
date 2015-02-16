package helper.pack;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class WordPairMap extends MapWritable{
	
	public WordPairMap(){
		super();
	}
	
	public static WordPairMap create(DataInput in) throws IOException {
		  	WordPairMap m = new WordPairMap();
			m.readFields(in);

			return m;
	}
	
	public void increment(Writable key){
		 increment(key,1);
	}
	
	public void increment(Writable key, int n){
		this.put(key,new IntWritable(((IntWritable) this.get(key)).get()+n));
	}
	public String toString(){
		String  key = "";
		for(Map.Entry<Writable, Writable> entry: this.entrySet())
			key +="("+entry.getKey().toString()+","+entry.getValue().toString()+")";
		return  "["+key+"]";
	}
	
	public void add(WordPairMap mapPair){
		for(Map.Entry<Writable, Writable> entry: mapPair.entrySet()){
			Writable  key = entry.getKey();
			if(this.containsKey(key))
				this.put(key,new IntWritable(((IntWritable) this.get(key)).get()+((IntWritable) entry.getValue()).get()));
			else this.put(key, new IntWritable(((IntWritable) entry.getValue()).get()));			
		}
	}
	
	
}
