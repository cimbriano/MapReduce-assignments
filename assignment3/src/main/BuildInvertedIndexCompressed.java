/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */


import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfWritables;
import edu.umd.cloud9.util.fd.Object2IntFrequencyDistribution;
import edu.umd.cloud9.util.fd.Object2IntFrequencyDistributionEntry;
import edu.umd.cloud9.util.pair.PairOfObjectInt;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

  private static class MyMapper extends Mapper<LongWritable, Text, TextIntWritablePairComparable, IntWritable> {
    private static final Text WORD = new Text();
    private static final IntWritable DOCNO = new IntWritable();

    private static final TextIntWritablePairComparable KEY_PAIR = new TextIntWritablePairComparable();
    private static final IntWritable TERM_FREQ = new IntWritable();

    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<String>();

    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      String text = doc.toString();
      COUNTS.clear();

      String[] terms = text.split("\\s+");

      // First build a histogram of the terms.
      for (String term : terms) {
        if (term == null || term.length() == 0) {
          continue;
        }

        COUNTS.increment(term);
      }

      // Emit postings.
      for (PairOfObjectInt<String> e : COUNTS) {
        WORD.set(e.getLeftElement());
        DOCNO.set((int)docno.get());
        KEY_PAIR.set(WORD, DOCNO);

        TERM_FREQ.set(e.getRightElement());

        context.write(KEY_PAIR, TERM_FREQ);

      }
    }
  }

  protected static class MyPartitioner extends Partitioner<TextIntWritablePairComparable, IntWritable> {
    @Override
    public int getPartition(TextIntWritablePairComparable key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  public static class MyKeyComparator extends WritableComparator {
    protected MyKeyComparator() {
      super(TextIntWritablePairComparable.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2){
      TextIntWritablePairComparable pair1 = (TextIntWritablePairComparable) w1;
      TextIntWritablePairComparable pair2 = (TextIntWritablePairComparable) w2;


      //      LOG.info("Comparing keys: " + pair1.getLeftElement().toString() + " & " + pair2.getLeftElement().toString());

      int compareTo = pair1.compareTo(pair2);
      //      LOG.info("Result is: " + compareTo);

      return compareTo;
    }
  }

  private static class MyReducer extends
  Reducer<TextIntWritablePairComparable, IntWritable, Text, PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>> {

    private final static IntWritable DF_WRITABLE = new IntWritable();
    private final static ArrayListWritable<PairOfInts> POSTINGS = new ArrayListWritable<PairOfInts>();
    private final static PairOfInts SINGLE_POSTING = new PairOfInts();
    private final static Text TERM = new Text();
    
    private static int lastDocno = 0;
    private static int thisDocno = 0;
    private static int dGapInt = 0;
    
    
    private static int termFreq = 0;
    private static int docFreq = 0;
    private static String currentTerm = null;
    
    @Override
    public void reduce(TextIntWritablePairComparable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

      //Get doc number
      thisDocno = key.getRightElement().get();
      
      String incomingTerm = key.getKey().toString();
      if(incomingTerm.equals(currentTerm) || currentTerm == null){
        
        currentTerm = key.getKey().toString();
        
        //Iterate through the values
        Iterator<IntWritable> iter = values.iterator();
        while(iter.hasNext()){
          docFreq++;
          termFreq = iter.next().get();
          
          dGapInt = thisDocno - lastDocno;
          
          SINGLE_POSTING.set(dGapInt, termFreq);
          POSTINGS.add(SINGLE_POSTING.clone());
          lastDocno = thisDocno;
        }

      } else {
        // Emit the current posting list and reset everything for next term
        DF_WRITABLE.set(docFreq);
        TERM.set(currentTerm);

        context.write(TERM, 
            new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>(DF_WRITABLE, POSTINGS) );

        //Reset counters
        docFreq = 0;
        lastDocno = 0;
        currentTerm = key.getKey().toString();
        POSTINGS.clear();
        
        //Iterate through the values (first posting for new word)
        Iterator<IntWritable> iter = values.iterator();
        while(iter.hasNext()){
          docFreq++;
          termFreq = iter.next().get();
          
          dGapInt = thisDocno - lastDocno;
          
          SINGLE_POSTING.set(dGapInt, termFreq);
          POSTINGS.add(SINGLE_POSTING.clone());
          lastDocno = thisDocno;
        }
      }

    }
  
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException{

      DF_WRITABLE.set(docFreq);
      TERM.set(currentTerm);
      context.write(TERM, 
          new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>(DF_WRITABLE, POSTINGS) );
      
    }
    
  }

  private BuildInvertedIndexCompressed() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
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

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
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

        LOG.info("Tool name: " + BuildInvertedIndexCompressed.class.getSimpleName());
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + outputPath);
        LOG.info(" - num reducers: " + reduceTasks);

        Job job = Job.getInstance(getConf());
        job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
        job.setJarByClass(BuildInvertedIndexCompressed.class);

        job.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapOutputKeyClass(TextIntWritablePairComparable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairOfWritables.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        //Use this one v. Using TextOutput just to test output
//        job.setOutputFormatClass(MapFileOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setSortComparatorClass(MyKeyComparator.class);
        job.setReducerClass(MyReducer.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(outputPath);
        FileSystem.get(getConf()).delete(outputDir, true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexCompressed(), args);
  }
}
