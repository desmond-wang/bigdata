/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs451.a1;

import java.io.*;
import java.util.*;
import java.lang.Math;
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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.pair.PairOfStrings;
import io.bespin.java.util.Tokenizer;



public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

  // First stage(count number of lines): emit the pair with (key, 1) for each unique pair,
  // key[*,*] as a single line, key[x,*] as times x occur.

  private static final class CountMapper extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {
    private static final FloatWritable ONE = new FloatWritable(1);
    private static final PairOfStrings BIGRAM = new PairOfStrings();


    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      StringTokenizer tokens = new StringTokenizer(line);

      Set<String> sortedWords = new TreeSet<String>();
      int count = 0;
      while(tokens.hasMoreTokens() && count <= 40){
          String w = tokens.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
          if (w.length() == 0) continue;
        sortedWords.add(w);
        count++;
      }


      // count the total line
      BIGRAM.set("*","*");
      context.write(BIGRAM, ONE);


      String[] words = new String[sortedWords.size()];
      sortedWords.toArray(words);

      for (int i = 0; i < sortedWords.size(); i++ ) {
        BIGRAM.set(words[i], "*");
        context.write(BIGRAM, ONE);
      }
    }

  }

  // First stage(count number of lines): reduce to count the total number of line.

  private static final class CountReducer extends
          Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
    private static final FloatWritable SUM = new FloatWritable();
    private int threshold = 10;

    @Override
    public void setup(Context context) {
      threshold = context.getConfiguration().getInt("threshold", 10);
    }

    @Override
    public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
      float sum = 0.0f;
      Iterator<FloatWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      SUM.set(sum);
      context.write(key,SUM);
    }
  }

// second stage: map the pair ([A,*], count), ([B, *], count) and ([A,B], count)
  private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> {
    private static final FloatWritable ONE = new FloatWritable(1);
    private static final PairOfStrings BIGRAM = new PairOfStrings();


    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      StringTokenizer tokens = new StringTokenizer(line);

      Set<String> sortedWords = new TreeSet<String>();
      int count = 0;
      while(tokens.hasMoreTokens() && count <= 40){
          String w = tokens.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
          if (w.length() == 0) continue;
        sortedWords.add(w);
        count++;
      }

      // init
      String left = "";
      String right = "";

      String[] words = new String[sortedWords.size()];
      sortedWords.toArray(words);

      if(sortedWords.size() < 2) return;
      for (int i = 0; i < sortedWords.size(); i++) {
        for (int j = 0; j < sortedWords.size(); j++){
          if (i == j){
            continue;
          } else {
            left = words[i];
            right = words[j];
            BIGRAM.set(left,right);
            context.write(BIGRAM, ONE);
          }
        }
      }
    }
  }



  private static final class MyCombiner extends
      Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
    private static final FloatWritable SUM = new FloatWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      Iterator<FloatWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  // second stage reducer: calculate the PMI in pairs
  private static final class MyReducer extends
      Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {
    private static final FloatWritable VALUE = new FloatWritable();
    private float marginal = 0.0f;
    private int threshold = 10;
    private static final HashMap<String, Float> X_Star_Map = new HashMap<String, Float>();
    @Override
    public void setup(Context context) throws IOException {
      threshold = context.getConfiguration().getInt("threshold", 10);
      FileSystem fs = FileSystem.get(new Configuration());
      Path inFile = new Path("./temp/pairsPMI/part-r-00000");
      if (!fs.exists(inFile)) {
        throw new IOException("File Not Found: " + inFile.toString());
      }
      try {
        FileStatus[] status = fs.listStatus(inFile);
        for (int i = 0; i < status.length; i++) {
          BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
          String line;
          line = br.readLine();

          while (line != null) {
            line = line.replaceAll("\\(", "").replaceAll("\\)", "").replaceAll("(\\,)(\\s+)(\\*)", " ");
            String[] countNum;
            countNum = line.split("\\s+");

            String left = countNum[0];

            String right = countNum[1];
            float f = Float.parseFloat(right);

            X_Star_Map.put(left, f);

            line = br.readLine();

          }
        }
      } catch (Exception e) {
        throw new IOException("------Exception thrown when trying to open file.-----");
      }
    }

    @Override
    public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      float sum = 0.0f;
      Iterator<FloatWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      if (sum >= threshold) {
        float total = X_Star_Map.get("*");

        float xyprob = sum / total;
        float xprob = X_Star_Map.get(key.getRightElement()) / total;
        float yprob = X_Star_Map.get(key.getLeftElement()) / total;
        float pmi = (float)Math.log10(xyprob / (xprob * yprob));

        VALUE.set(pmi);
        context.write(key, VALUE);
      }
    }
  }

//  private static final class MyPartitioner extends Partitioner<PairOfStrings, FloatWritable> {
//    @Override
//    public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
//      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
//    }
//  }

  /**
   * Creates an instance of this tool.
   */
  public PairsPMI() {}

  private static final String input = "input";
  private static final String output = "output";
  private static final String numReducers = "reducers";
  private static final String THRESHOLD = "threshold";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] argv) throws Exception {
      Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input path").create(input));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path").create(output));
    options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("number of reducers").create(numReducers));
    options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("number of threshold").create(THRESHOLD));


    CommandLine cmdline;
    CommandLineParser parser = new GnuParser(); 
    try{
        cmdline = parser.parse(options, argv);
    } catch (ParseException exp) {
        System.err.println("Error parsing command line: " + exp.getMessage());
        return -1;
    }
      if(!cmdline.hasOption(input) || !cmdline.hasOption(output)) {
      System.out.println("args: " + Arrays.toString(argv));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(input);
    String outputPath = cmdline.getOptionValue(output);
    String intermediatePath = "./temp/pairsPMI/";
    int reduceTasks = cmdline.hasOption(numReducers) ?
            Integer.parseInt(cmdline.getOptionValue(numReducers)) : 1;

    int thresholdTask = cmdline.hasOption(THRESHOLD) ?
            Integer.parseInt(cmdline.getOptionValue(THRESHOLD)) : 10;

    LOG.info("Tool name: " + PairsPMI.class.getSimpleName()+ "Phase 1");
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + intermediatePath);
    LOG.info(" - num reducers: " + 1);
    LOG.info(" - num threshold: " + thresholdTask);
//    LOG.info(" - text output: " + args.textOutput);

    Job job1 = Job.getInstance(getConf());
    job1.setJobName(PairsPMI.class.getSimpleName() + "Count");
    job1.setJarByClass(PairsPMI.class);

    job1.getConfiguration().setInt("threshold", thresholdTask);

    job1.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(job1, new Path(inputPath));
    FileOutputFormat.setOutputPath(job1, new Path(intermediatePath));

    job1.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job1.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job1.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job1.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    job1.setMapOutputKeyClass(PairOfStrings.class);
    job1.setMapOutputValueClass(FloatWritable.class);
    job1.setOutputKeyClass(PairOfStrings.class);
    job1.setOutputValueClass(FloatWritable.class);
//    if (args.textOutput) {
      job1.setOutputFormatClass(TextOutputFormat.class);
//    } else {
//      job1.setOutputFormatClass(SequenceFileOutputFormat.class);
//    }

    job1.setMapperClass(CountMapper.class);
//    job1.setCombinerClass(MyCombiner.class);
    job1.setReducerClass(CountReducer.class);
//    job1.setPartitionerClass(MyPartitioner.class);


    // Delete the output directory if it exists already.
    Path intermediateDir = new Path(intermediatePath);
    FileSystem.get(getConf()).delete(intermediateDir, true);

    long startTime = System.currentTimeMillis();
    job1.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");


    // Start Second job
    LOG.info("Tool name: " + PairsPMI.class.getSimpleName()+ "Phase 2");
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + intermediatePath);
    LOG.info(" - num reducers: " + reduceTasks);
    LOG.info(" - num threshold: " + thresholdTask);

    Job job2 = Job.getInstance(getConf());
    job2.setJobName(PairsPMI.class.getSimpleName() + "calculation");
    job2.setJarByClass(PairsPMI.class);

    job2.getConfiguration().setInt("threshold", thresholdTask);

    job2.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job2, new Path(inputPath));
    FileOutputFormat.setOutputPath(job2, new Path(outputPath));

    job2.setMapOutputKeyClass(PairOfStrings.class);
    job2.setMapOutputValueClass(FloatWritable.class);
    job2.setOutputKeyClass(PairOfStrings.class);
    job2.setOutputValueClass(FloatWritable.class);
//    if (args.textOutput) {
      job2.setOutputFormatClass(TextOutputFormat.class);
//    } else {
//      job2.setOutputFormatClass(SequenceFileOutputFormat.class);
//    }

    job2.setMapperClass(MyMapper.class);
    job2.setCombinerClass(MyCombiner.class);
    job2.setReducerClass(MyReducer.class);

//    job2.setPartitionerClass(MyPartitioner.class);

    job2.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job2.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job2.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job2.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(getConf()).delete(outputDir, true);

    startTime = System.currentTimeMillis();
    job2.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args);
  }
}
