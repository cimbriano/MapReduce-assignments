import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;


public class PairsPMI extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  // First stage Mapper: emits pairs (token, 1) for each unique token in an input value,
  //                  and a pair (token pair, 1) for each unique pair of tokens
  private static class CountsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Objects for reuse
    private final static IntWritable ONE = new IntWritable(1);
    private final static Text KEY = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
         throws IOException, InterruptedException{

      KEY.set(value.toString());
      // Emit once per input just to count input size
      context.write(KEY, ONE);
    }
  }

  // First stage Reducer: Totals counts for each Token and Token Pair
  private static class CountsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    // Reuse objects
    private final static IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException{

    }
  }
  
  
  
  // Second stage mapper: Maps (A, TOTAL_A), (B, TOTAL_B) and (A_B, TOTAL_A_B) to the same reducer 
  //                via a common key 
  private static class TotalsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
  }
  
  // Second Stage reducer: Finalizes PMI Calculation given 
  private static class TotalsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    
  }
  
  
  


  public PairsPMI() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";

  @SuppressWarnings("static-access")
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of reducers").create(NUM_REDUCERS));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try{
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if(!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? 
        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

        LOG.info("Tool: " + PairsPMI.class.getSimpleName());
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + outputPath);
        LOG.info(" - number of reducers: " + reduceTasks);

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName(PairsPMI.class.getSimpleName());
        job.setJarByClass(PairsPMI.class);

        job.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        /*
         * Output key and value types for entire job (ie. reducers output) 
         */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(CountsMapper.class);
        job.setCombinerClass(CountsReducer.class);
        job.setReducerClass(CountsReducer.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception{
    ToolRunner.run(new PairsPMI(), args);
  }
}
