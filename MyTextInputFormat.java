import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/** An {@link InputFormat} for plain text files.  Files are broken into lines.
 * Either linefeed or carriage-return are used to signal end of line.  Keys are
 * the position in the file, and values are the line of text.. */
public class MyTextInputFormat extends TextInputFormat {
//First call.
//getsplits() is Called from client side.
//TextInputFormat main class is FileInputFormat and that implements InputFormat interface. That has two methods getsplits() and createrecordreader().
//So FileInputFormat should implement both methods. Since every inputformat wants to overwrite its own createrecordreader() method to let it know how to 
//read the input(because in textinputformat linerecordreader is used and in other some other record reader.
//Called from client side calls getsplits() first and this gets splitsize and returns the INputSplit[] to InputFormat. Then the splits are sent to JT,
//with the locations JT assigns that to TT.MT would call createrecordreader().
//If there are 2 splits then 2 MT would be assigned and createrecordreader() would be called once for each MT.
  public RecordReader<LongWritable, Text> 
    createRecordReader(InputSplit split,
                       TaskAttemptContext context) {
	  //Second call
	  //calls Linerecordreader class
    return new MyLineRecordReader();
  }



}
