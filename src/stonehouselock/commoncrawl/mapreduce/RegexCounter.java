package stonehouselock.commoncrawl.mapreduce;

// Java classes
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

// Common Crawl classes
import org.commoncrawl.hadoop.io.ARCInputFormat;
import org.commoncrawl.hadoop.io.JetS3tARCSource;
import org.commoncrawl.protocol.shared.ArcFileItem;

/*
 * MapReduce file to count the number of pages that have text matching a regular 
 * expression
 */
public class RegexCounter extends Configured implements Tool {
  
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

  public int run(String[] args) throws Exception {
    String awsCredentials = args[0];
    String awsSecret = args[1];
    String inputPrefixes = "common-crawl/crawl-002/" + args[2];
    String outputFile = args[3];
    
    //Echo Args
    System.out.println("Using AWS Credentials: " + awsCredentials);
    System.out.println("Using S3 bucket paths: " + inputPrefixes);
    
    // Create job configuration
    JobConf conf = new JobConf(this.getConf());
    
    conf.setJarByClass(RegexCounter.class);
    
    // Configures this job with your Amazon AWS credentials
    conf.set(JetS3tARCSource.P_INPUT_PREFIXES, inputPrefixes);
    conf.set(JetS3tARCSource.P_AWS_ACCESS_KEY_ID, awsCredentials);
    conf.set(JetS3tARCSource.P_AWS_SECRET_ACCESS_KEY, awsSecret);
    conf.set(JetS3tARCSource.P_BUCKET_NAME, CC_BUCKET);
    
    
    return 0;
  }

  public static void main(String[] args) {

  }

}
