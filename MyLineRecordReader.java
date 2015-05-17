import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

/**
 * Treats keys as offset in file and value as line. 
 */
public class MyLineRecordReader extends RecordReader<LongWritable, Text> {
  private static final Log LOG = LogFactory.getLog(MyLineRecordReader.class);

  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  private LineReader in;
  private int maxLineLength;
  private LongWritable key = null;
  private Text value = null;

  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
//Third Call
//Called once for each Inputsplit.
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                                    Integer.MAX_VALUE);
    System.out.println("maxLineLength is"+maxLineLength);
    //maxlinelength is integer's max value
    start = split.getStart();
    end = start + split.getLength();
    //get the split start and end position in bytes
    //Size of the split is determined by getsplits() method and teh splitsize is dependent on the newline. So if the filesize is 200MB and the splitsize
    //is 64 MB so there would be 4 splits(64+64+64+8) and hence 4 mappers.
    System.out.println("start is"+start);
    System.out.println("end is"+end);
    //Split 1 - start(0) and end(64)
    //Split 2 - start(64) and end(128)
    //Split 3 - start(128) and end(192)
    //Split 4 - Start(192) and end(200)
    final Path file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(file);

    // open the file and seek to the start of the split
    FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(split.getPath());
    boolean skipFirstLine = false;
    if (codec != null) {
      in = new LineReader(codec.createInputStream(fileIn), job);
      end = Long.MAX_VALUE;
    } else {
      if (start != 0) {
 //Will come inside the loop for split 2,3,4
        skipFirstLine = true;
        --start;
 //Goes back to 1 byte because if the line boundary is equal to the split boundary(if the record ends at the split boundary), then if we don't 
 //decrement then we would miss one record.
 //for ex: here split 2 ends at 128 MB and if the record also ends at 128 MB, then the start would be at 129th MB then the code that follows will look for 
 //the next newline taht would be somewhere at 130MB so we misses a record b/w 128MB and 130MB.
        fileIn.seek(start);
 //makes the file pointer to the start position.
      }
      in = new LineReader(fileIn, job);
    }
    if (skipFirstLine) {  // skip first line and re-establish "start".
//enters for split 2,3,4.
//Looks for the first newline character because other than the first split because the split doesn't always starts with a new record.
      start += in.readLine(new Text(), 0,
                           (int)Math.min((long)Integer.MAX_VALUE, end - start));
//And reallign the start position.
//So for ex: for split 2 assume the record starts at 130 MB so the new start is 130Mb and not 128Mb which is defined at the start.
    }
    this.pos = start;
// The next call is map's run() method.
  }
  
  public boolean nextKeyValue() throws IOException {
//Fifth call
//Called for each record. So called many times for a Split. If 4 records then called 4 times.
    if (key == null) {
      key = new LongWritable();
    }
    key.set(pos);
//Sets the key which is the offset.
    if (value == null) {
      value = new Text();
    }
    System.out.println("key is" +key);
    System.out.println("value is"+value.toString());
    int newSize = 0;
    while (pos < end) {
//Will come inside if the split end is not reached.
    	System.out.println("value is"+value.toString());
//Reads single line. And this returns value as well and that is set. And it returns how many bytes are read and that is stored in newSize.
      newSize = in.readLine(value, maxLineLength,
                            Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
                                     maxLineLength));
      System.out.println("value is"+value.toString());
      System.out.println("newsize is is" +newSize);  
      if (newSize == 0) {
//If the split boundary is same as that of the line boundary then this call is made(start != end because the last character would be newline). This break returns true.
        break;
      }
      pos += newSize;
//pos is reassigned before pos=start. Now pos=start+newSize to make it ease to read next record.
      System.out.println("value is"+value.toString());
      System.out.println("maxLineLength is"+maxLineLength);
      if (newSize < maxLineLength) {
 //will break till the record length is less than the integer size.This break is necessary to set it as true.
        break;
      }

      // line too long. try again
      LOG.info("Skipped line of size " + newSize + " at pos " + 
               (pos - newSize));
    }
//So any break in upper loops will bring the pointer here.
    if (newSize == 0) {
  // This loop is only when pos becomes greater than end i.e, if the split ends.
      key = null;
      value = null;
      return false;
    } 
//If the split doesn't end then the pointer is brought here that would return true,meaning there is more records in the split to be read.
    else {
      return true;
    }
  }

  @Override
  public LongWritable getCurrentKey() {
//Sixth Call.
//Called once for every record in the split.
//Once a single record is read then the call is made here. This would be called only if nextkeyvalue() returns true. 
    return key;
  }

  @Override
  public Text getCurrentValue() {
//Seventh Call.
//Called once for every record in the split.
//Once a single record is read then the call is made here. This would be called only if nextkeyvalue() returns true. 
	  return value;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() {
//Fifth Call
//Called once for every record in the split.
//Once a single record is read then the call is made here. This would be called only if nextkeyvalue() returns true. 
	  if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }
  
  public synchronized void close() throws IOException {
//Last call.
    if (in != null) {
      in.close(); 
    }
  }
}
