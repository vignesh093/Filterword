import java.util.List;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Filterwordmapper extends Mapper<LongWritable,Text,Text,NullWritable>{
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
	{
		List<String> mylist=Arrays.asList("hi","hello","polo");
		
		String s=value.toString();
		//int a=s.compareTo("hello");
		for(String str: mylist)
		{
			if(str.contains(s))
			{
				context.write(new Text(s),NullWritable.get());
			}
		}
	//	if(a==0)
	//	{
//			context.write(new Text(s),NullWritable.get());
		//}
	}
	public void run(Context context) throws IOException, InterruptedException {
		//Fourth Call.
		//calls setup method
		setup(context);
		while (context.nextKeyValue()) {
		//Calls nextkeyvalue() function of linerecordreader.If returns true then calls getprogress() and then calls getCurrentkey() and getCurrentValue() functions
		map(context.getCurrentKey(), context.getCurrentValue(), context);
		//If returns false then close() of linerecordreader is called.
		}
		cleanup(context);
		}
}
