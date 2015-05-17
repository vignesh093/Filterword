import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Filterworddriver
{
	public static void main(String args[]) throws Exception
	{
		if(args.length !=2)
		{
			System.err.println("Usage: Worddrivernewapi <input path> <output path>");
			System.exit(-1);
		}
		Job job=new Job();
		job.setJarByClass(Filterworddriver.class);
		job.setJobName("Filterworddriver");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.setMapperClass(Filterwordmapper.class);
		job.setInputFormatClass(MyTextInputFormat.class);
		
		job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}
