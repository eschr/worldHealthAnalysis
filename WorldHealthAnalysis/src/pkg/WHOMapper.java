package pkg;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WHOMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text countryYear;
	private Text whoDataField;
	
	/* Attribute order in WHO data
	 * 
	 * "Country Name","Country Code","Country WHO Region","Country Income Group","Indicator Name","Indicator Code","Indicator Unit",
	 * "Units of Expenditures","Comments","Methods of estimation","Sources","Data type","Footnote","Year","Value","Comments ",
	 * "Methods of estimation","Sources","Data type","Last updated"
	 *
	 */
	public void map(LongWritable key, Text value, Context context) throws
	IOException, InterruptedException{
		
		countryYear = new Text();
		whoDataField = new Text();
		
		String[] fields = value.toString().split(",");
		
		if (fields.length < 15) return;
		
		String countryName = fields[0].trim();
		String indicatorName = fields[4].trim();
		String indicatorUnits = fields[6].trim();
		String year = fields[13].trim();
		String val = fields[14].trim();
		
		String outKey = countryName + "," + year;
		String outVal = indicatorName + ":" + indicatorUnits + ":" + val;
		
		countryYear.set(outKey);
		whoDataField.set(outVal);
		
		context.write(countryYear, whoDataField);
		
	}
}
