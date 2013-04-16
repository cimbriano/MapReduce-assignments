import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class ExtractHourlyCountsAll extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractHourlyCountsAll.class);

  public static class AllTweetsMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    //    public static final PairOfStringInt DATE_HOUR= new PairOfStringInt();
    public static final Text DATE_HOUR = new Text();
    public static final IntWritable ONE = new IntWritable(1);

    public static String[] rawTweetAndMetadata = null;
    public static String[] rawDateTime = null;
    public static String month = null;
    public static String day = null;
    public static String hour = null;

    @Override
    public void map(LongWritable key, Text text, Context context) throws IOException, InterruptedException {

      
      
      rawTweetAndMetadata = text.toString().split("\t");
      
      if(rawTweetAndMetadata.length < 4) return;
//      LOG.info("rawTweetAndMetadata array: " + rawTweetAndMetadata);
//      LOG.info("Tweet array size: " + rawTweetAndMetadata.length);
      
      
//      LOG.info("rawDateTime: string" + rawTweetAndMetadata[1]);
      rawDateTime = rawTweetAndMetadata[1].split(" ");
//      LOG.info("******* rawDatTime pre split: '" + rawTweetAndMetadata[1] + "'");
//      for(String s : rawDateTime){
//        LOG.info(s);
//      }
      
//      LOG.info("rawDateTime: array" + rawDateTime);
      
//      LOG.info("******* Month: '" + month + "'");
      month = rawDateTime[1].contains("Jan") ? "01" : "02";
      day = rawDateTime[2];
      hour = rawDateTime[3].split(":")[0];

      DATE_HOUR.set( month + "/" + day + " " + hour);

      context.write(DATE_HOUR, ONE);
    }
  }

  public static class AllTweetsReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    public static final IntWritable COUNT = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
      int total = 0;

      for(IntWritable value : values){
        total += value.get();
      }

      COUNT.set(total);

      context.write(key, COUNT);
    }

  }




  /**
   * Creates an instance of this tool.
   */
  public ExtractHourlyCountsAll() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    //    Options options = new Options();
    //
    //    options.addOption(OptionBuilder.withArgName("path").hasArg()
    //        .withDescription("input path").create(INPUT));
    //    options.addOption(OptionBuilder.withArgName("path").hasArg()
    //        .withDescription("output path").create(OUTPUT));
    //    options.addOption(OptionBuilder.withArgName("num").hasArg()
    //        .withDescription("number of reducers").create(NUM_REDUCERS));
    //
    //    CommandLine cmdline;
    //    CommandLineParser parser = new GnuParser();
    //
    //    try {
    //      cmdline = parser.parse(options, args);
    //    } catch (ParseException exp) {
    //      System.err.println("Error parsing command line: " + exp.getMessage());
    //      return -1;
    //    }
    //
    //    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
    //      System.out.println("args: " + Arrays.toString(args));
    //      HelpFormatter formatter = new HelpFormatter();
    //      formatter.setWidth(120);
    //      formatter.printHelp(this.getClass().getName(), options);
    //      ToolRunner.printGenericCommandUsage(System.out);
    //      return -1;
    //    }
    //
    //    String inputPath = cmdline.getOptionValue(INPUT);
    //    String outputPath = cmdline.getOptionValue(OUTPUT);
    //    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ?
    //        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;


//    String inputPath = "/user/shared/tweets2011/tweets2011.txt";
    String inputPath = "tweets2011.txt";
    String outputPath = "imbriano_all_out";

    LOG.info("Tool: " + ExtractHourlyCountsAll.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(ExtractHourlyCountsAll.class.getSimpleName());
    job.setJarByClass(ExtractHourlyCountsAll.class);


    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(AllTweetsMapper.class);
    job.setReducerClass(AllTweetsReducer.class);


    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(conf).delete(outputDir, true);


    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ExtractHourlyCountsAll(), args);
  }
}