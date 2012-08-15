package stonehouselock.commoncrawl.mapreduce;

// Java classes
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

// Apache Project classes
import org.apache.log4j.Logger;

// Hadoop classes
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.record.CsvRecordOutput;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.Progressable;

// Common Crawl classes
import org.commoncrawl.hadoop.io.ARCInputFormat;
import org.commoncrawl.hadoop.io.JetS3tARCSource;
import org.commoncrawl.protocol.shared.ArcFileItem;

/*
 * MapReduce file to count the number of pages that have text matching a regular 
 * expression
 */
public class RegexCounter extends Configured {
  
  private static final Logger LOG = Logger.getLogger(RegexCounter.class);
  private static final String CC_BUCKET = "aws-publicdatasets";
  /*
   * Perform a counter mapping to a predefined set of regular expressions.  Max 
   * of one per website.
   */
  static class RegexCounterMapper 
      extends MapReduceBase
      implements Mapper<Text, ArcFileItem, Text, LongWritable> {
    
    // create a counter group for Mapper-specific stats
    private final String _counterGroup = "Custom Mapper Counters";
    private final Pattern _regex = Pattern.compile("(?i)Aaron Patterson");
    public final LongWritable one = new LongWritable(1);
    
    public void map(
        Text key, 
        ArcFileItem value, 
        OutputCollector<Text, LongWritable> output, 
        Reporter reporter) 
        throws IOException {
      // count records in
      reporter.incrCounter(this._counterGroup, "Records in", 1);
      try {
        // only parse text
        if(!value.getMimeType().contains("text")) {
          reporter.incrCounter(this._counterGroup, "Skipped - No Text", 1);
          return;
        }
        //Retreieves page content from the passed-in ArcFileItem
        ByteArrayInputStream inputStream = new ByteArrayInputStream(
            value.getContent().getReadOnlyBytes(), 0, 
            value.getContent().getCount());
        String match = new Scanner(inputStream).findWithinHorizon(_regex, 0);
        if(match != null) {
          output.collect(new Text(_regex.toString()), new LongWritable(1));
        }
      }
      catch (Exception ex) {
        LOG.error("Caught Exception", ex);
        reporter.incrCounter(this._counterGroup, "Exceptions", 1);
      }
    }
  }
  
  public static class CSVOutputFormat
      extends TextOutputFormat<Text, LongWritable> {
    
    public RecordWriter<Text, LongWritable> getRecordWriter(
        FileSystem ignored, JobConf job, String name, Progressable progress)
        throws IOException {
      Path file = FileOutputFormat.getTaskOutputPath(job, name);
      FileSystem fs = file.getFileSystem(job);
      FSDataOutputStream fileOut = fs.create(file, progress);
      return new CSVRecordWriter(fileOut);
    }

    protected static class CSVRecordWriter
        implements RecordWriter<Text, LongWritable> {
      protected DataOutputStream outStream;

      public CSVRecordWriter(DataOutputStream out) {
        this.outStream = out;
      }

      public synchronized void write(Text key, LongWritable value)
          throws IOException {
        CsvRecordOutput csvOutput = new CsvRecordOutput(outStream);
        csvOutput.writeString(key.toString(), "word");
        csvOutput.writeLong(value.get(), "occurrences");
      }
  
      public synchronized void close(Reporter reporter) throws IOException {
        outStream.close();
      }
    }
  }

  public static void main(String[] args) {
 // Parses command-line arguments.
    String awsCredentials = args[0];
    String awsSecret = args[1];
    String inputPrefixes = "common-crawl/crawl-002/" + args[2];
    String outputFile = args[3];

    // Echoes back command-line arguments.
    System.out.println("Using AWS Credentials: " + awsCredentials);
    System.out.println("Using S3 bucket paths: " + inputPrefixes);

    // Creates a new job configuration for this Hadoop job.
    JobConf conf = new JobConf();

    // Configures this job with your Amazon AWS credentials
    conf.set(JetS3tARCSource.P_INPUT_PREFIXES, inputPrefixes);
    conf.set(JetS3tARCSource.P_AWS_ACCESS_KEY_ID, awsCredentials);
    conf.set(JetS3tARCSource.P_AWS_SECRET_ACCESS_KEY, awsSecret);
    conf.set(JetS3tARCSource.P_BUCKET_NAME, CC_BUCKET);

    // Configures where the input comes from when running our Hadoop job,
    // in this case, gzipped ARC files from the specified Amazon S3 bucket
    // paths.
    ARCInputFormat.setARCSourceClass(conf, JetS3tARCSource.class);
    ARCInputFormat inputFormat = new ARCInputFormat();
    inputFormat.configure(conf);
    conf.setInputFormat(ARCInputFormat.class);

    // Configures what kind of Hadoop output we want.
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LongWritable.class);

    // Configures where the output goes to when running our Hadoop job.
    CSVOutputFormat.setOutputPath(conf, new Path(outputFile));
    CSVOutputFormat.setCompressOutput(conf, false);
    conf.setOutputFormat(CSVOutputFormat.class);

    // Allows some (10%) of tasks fail; we might encounter the 
    // occasional troublesome set of records and skipping a few 
    // of 1000s won't hurt counts too much.
    conf.set("mapred.max.map.failures.percent", "10");

    // Tells the user some context about this job.
    InputSplit[] splits = inputFormat.getSplits(conf, 0);
    if (splits.length == 0) {
      System.out.println("ERROR: No .ARC files found!");
      return;
    }
    System.out.println("Found " + splits.length + " InputSplits:");
    for (InputSplit split : splits) {
      System.out.println(" - will process file: " + split.toString());
    }

    // Tells Hadoop what Mapper and Reducer classes to use;
    // uses combiner since wordcount reduce is associative and commutative.
    conf.setMapperClass(RegexCounterMapper.class);
    conf.setReducerClass(LongSumReducer.class);

    // Tells Hadoop mappers and reducers to pull dependent libraries from
    // those bundled into this JAR.
    conf.setJarByClass(RegexCounter.class);

    // Runs the job.
    JobClient.runJob(conf);
  }

}
