package pkg;


import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class WHOReducer extends Reducer<Text, Text, Text, Text> {
	
	private Text csvOutput;
	private Text data;
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		csvOutput = new Text();
		data = new Text();
		
		String countryYear = key.toString();
		StringBuilder outputBuilder = new StringBuilder();
		
		outputBuilder.append(countryYear);
		
		for (Text value : values) {
			//outputBuilder.append(value.toString().concat(","));
			data.set(value);
			context.write(key, data);
		}
		
		/*
		outputBuilder.setLength(outputBuilder.length() - 1);
		
		csvOutput.set(outputBuilder.toString());
		
		context.write(NullWritable.get(), csvOutput);*/
	}
}
