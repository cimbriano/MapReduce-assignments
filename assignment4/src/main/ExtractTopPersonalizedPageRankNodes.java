import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.util.TopNScoredObjects;
import edu.umd.cloud9.util.pair.PairOfObjectFloat;


public class ExtractTopPersonalizedPageRankNodes implements Tool {
	private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);


	private static final String INPUT = "input";
	private static final String TOP = "top";
	private static final String SOURCES = "sources";

	@SuppressWarnings("static-access")
	@Override
	public int run(String[] args) throws Exception {

		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input path").create(INPUT));
		options.addOption(OptionBuilder.withArgName("num").hasArg()
				.withDescription("top n").create(TOP));
		options.addOption(OptionBuilder.withArgName("sources").hasArg()
				.withDescription("source nodes").create(SOURCES));

		CommandLine cmdline;
		CommandLineParser parser = new GnuParser();

		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: " + exp.getMessage());
			return -1;
		}

		if (		!cmdline.hasOption(INPUT) 
				||  !cmdline.hasOption(TOP)
				||  !cmdline.hasOption(SOURCES)) {
			System.out.println("args: " + Arrays.toString(args));
			HelpFormatter formatter = new HelpFormatter();
			formatter.setWidth(120);
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String inputPath = cmdline.getOptionValue(INPUT);
		int n = Integer.parseInt(cmdline.getOptionValue(TOP));
		String sourcesString = cmdline.getOptionValue(SOURCES);

		LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
		LOG.info(" - input: " + inputPath);
		LOG.info(" - top: " + n);
		LOG.info(" - sources: " + sourcesString);

		getTopNodes(inputPath, sourcesString, n);

		return 0;
	}

	private void getTopNodes(String inputPathString, String sourcesString, int numResults) throws IOException, InstantiationException, IllegalAccessException {
		String[] sources = sourcesString.split(",");
		ArrayList<TopNScoredObjects<Integer>> queueList = new ArrayList<TopNScoredObjects<Integer>>();
		for(int i = 0; i < sources.length; i++){
			queueList.add(i, new TopNScoredObjects<Integer>(numResults));
		}
		
		
		Configuration conf = new Configuration();
		Path inputPath = new Path(inputPathString);
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(inputPathString + "/_SUCCESS"), true);
		
		for(FileStatus stat : fs.listStatus(inputPath)){

			Path filePath = stat.getPath();
			
			@SuppressWarnings("deprecation")
			SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), filePath, conf);
			
			IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
			PageRankNodeExtended value = (PageRankNodeExtended) reader.getValueClass().newInstance();
			
			while(reader.next(key, value)){
				for(int i = 0; i < sources.length; i++){
					queueList.get(i).add(key.get(), value.getPageRank(i));
				}
			}
			reader.close();
		}
		
		
		for(int i = 0; i < sources.length; i++){
			System.out.println("Source: " + sources[i]);
			//Print out top k
			TopNScoredObjects<Integer> list = queueList.get(i);
			for(PairOfObjectFloat<Integer> pair : list.extractAll()){

				int nodeid = ((Integer) pair.getLeftElement());
				float pagerank = (float) Math.exp(pair.getRightElement());
				System.out.println(String.format("%.5f %d", pagerank, nodeid));
			}
			System.out.println("");
		}
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
	}

	@Override
	public Configuration getConf() {
		return null;
	}

	@Override
	public void setConf(Configuration arg0) {
	}



}
