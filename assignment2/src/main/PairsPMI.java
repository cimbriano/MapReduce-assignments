import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;
import edu.umd.cloud9.io.pair.PairOfStrings;
import edu.umd.cloud9.io.triple.TripleOfInts;


public class PairsPMI extends Configured implements Tool {

  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  // First stage Mapper: emits pairs (token, 1) for each unique token in an input value,
  //                  and a pair (token pair, 1) for each unique pair of tokens
  private static class AppearanceCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Objects for reuse
    private final static Text KEY = new Text();
    private final static IntWritable ONE = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
         throws IOException, InterruptedException{
      
      String line = value.toString();
      StringTokenizer t = new StringTokenizer(line);
      Set<String> unique = new HashSet<String>();
      String token = "";
      
      while(t.hasMoreTokens()){
        token = t.nextToken();
        
        if(unique.add(token)){
          KEY.set(token);
          context.write(KEY, ONE);
        }
      }
    }
  }

  // First stage Reducer: Totals counts for each Token and Token Pair
  private static class AppearanceCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    // Reuse objects
    private final static IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException{
      
      
      int sum = 0;
      for(IntWritable value : values){
        sum += value.get();
      }
      
      SUM.set(sum);
      context.write(key, SUM);

    }
  }
  
  
  
  // Second stage mapper: Maps (A, TOTAL_A), (B, TOTAL_B) and (A_B, TOTAL_A_B) to the same reducer 
  //                via a common key 
  private static class PairsPMIMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
    
    // Objects for reuse
    private final static PairOfStrings PAIR = new PairOfStrings();
    private final static IntWritable ONE = new IntWritable(1);
    
    @Override
    public void map(LongWritable key, Text value, Context context){
      
    }
  }
  
  //TODO Write fill in combiner boilerplate
  //Combiner
  private static class PairsPMICombiner extends Reducer<PairOfStrings, IntWritable, Text, FloatWritable> {
    
    @Override
    public void reduce(PairOfStrings pair, Iterable<IntWritable> values, Context context){
      
    }
  }
  
  // Second Stage reducer: Finalizes PMI Calculation given 
  private static class PairsPMIReducer extends Reducer<PairOfStrings, IntWritable, Text, FloatWritable> {
    @Override
    public void setup(Context context){
      //TODO Read from intermediate output of first job
      // and build in-memory map of terms to their idividual totals
      
      
    }
    
    @Override
    public void reduce(PairOfStrings pair, Iterable<IntWritable> values, Context context ){
      // Recieving pair and pair counts -> Sum these for this pair's total
      
      // Look up individual totals for each member of pair
      
      // Calculate PMI emit Pair or Text as key and Float as value
      
    }
    
    @Override
    public void cleanup(Context context){
      //
    }
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
    
    //TODO This output path is for the 2nd job's.
    //    The fits job will have an intermediate output path from which the second job's reducer will read
    String outputPath = cmdline.getOptionValue(OUTPUT);
    String intermediatePath = "./appearance_totals";
    
    
    
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? 
        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

        LOG.info("Tool: " + PairsPMI.class.getSimpleName());
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + outputPath);
        LOG.info(" - number of reducers: " + reduceTasks);

        Configuration conf = getConf();
        Job job1 = Job.getInstance(conf);
        job1.setJobName(PairsPMI.class.getSimpleName() + " AppearanceCount");
        job1.setJarByClass(PairsPMI.class);

        job1.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(intermediatePath));

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setMapperClass(AppearanceCountMapper.class);
        job1.setCombinerClass(AppearanceCountReducer.class);
        job1.setReducerClass(AppearanceCountReducer.class);

        // Delete the output directory if it exists already.
        Path intermediateDir = new Path(intermediatePath);
        FileSystem.get(conf).delete(intermediateDir, true);

        long startTime = System.currentTimeMillis();
        job1.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        
        // Start second job
        
//        Job job2 = Job.getInstance(conf);
//        job2.setJobName(PairsPMI.class.getSimpleName() + " PairsPMICalcuation");
//        job2.setJarByClass(PairsPMI.class);
//        
//        job2.setNumReduceTasks(reduceTasks);
//        
//        FileInputFormat.setInputPaths(job2,  new Path(inputPath));
//        TextOutputFormat.setOutputPath(job2, new Path(outputPath));
//        
//        //TODO Which output key??
////        job2.setOutputKeyClass(Text.class or PairOfStrings.class);
//        job2.setOutputValueClass(FloatWritable.class);
//        job2.setMapperClass(PairsPMIMapper.class);
//        job2.setCombinerClass(PairsPMICombiner.class);
//        job2.setReducerClass(PairsPMIReducer.class);
//        
//        Path outputDir = new Path(outputPath);
//        FileSystem.get(conf).delete(outputDir, true);
        
        
        return 0;
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws Exception{
    ToolRunner.run(new PairsPMI(), args);
  }
}
