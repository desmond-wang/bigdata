package ca.uwaterloo.cs451.a4;

import com.google.common.base.Preconditions;
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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import tl.lin.data.array.ArrayListOfIntsWritable;
import tl.lin.data.array.ArrayListOfFloatsWritable;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Iterator;

public class RunPersonalizedPageRankBasic extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(RunPersonalizedPageRankBasic.class);

    private enum PageRank {
        nodes, edges, massMessages, massMessagesReceived, missingStructure
    }

    private static class MapClass extends
            Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {

        private static final IntWritable neighbor = new IntWritable();
        private static final PageRankNode intermediateMass = new PageRankNode();
        private static final PageRankNode intermediateStructure = new PageRankNode();
        private static int numSources = 0;

        @Override
        public void setup(Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode>.Context context) {
            String[] stringSources = context.getConfiguration().getStrings("MultiSource");
            numSources = stringSources.length;
        }

        @Override
        public void map(IntWritable nid, PageRankNode node, Context context)
                throws IOException, InterruptedException {
            intermediateStructure.setNodeId(node.getNodeId());
            intermediateStructure.setType(PageRankNode.Type.Structure);
            intermediateStructure.setAdjacencyList(node.getAdjacenyList());

            context.write(nid, intermediateStructure);

            int massMessages = 0;

            if (node.getAdjacenyList().size() > 0) {
                ArrayListOfIntsWritable list = node.getAdjacenyList();

                ArrayListOfFloatsWritable mass = new ArrayListOfFloatsWritable();
                for (int i = 0; i < numSources; i++) {
                    mass.add(node.getPageRank().get(i) - (float) StrictMath.log(list.size()));
                }

                context.getCounter(PageRank.edges).increment(list.size());
				
                for (int i = 0; i < list.size(); i++) {
                    neighbor.set(list.get(i));
                    intermediateMass.setNodeId(list.get(i));
                    intermediateMass.setType(PageRankNode.Type.Mass);
                    intermediateMass.setPageRank(mass);
                    context.write(neighbor, intermediateMass);
                    massMessages++;
                }
            }
            context.getCounter(PageRank.nodes).increment(1);
            context.getCounter(PageRank.massMessages).increment(massMessages);
        }
    }

    private static class CombineClass extends
            Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {
        private static final PageRankNode intermediateMass = new PageRankNode();
        private static int numSources = 0;

        @Override
        public void setup(Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode>.Context context) {
            String[] stringSources = context.getConfiguration().getStrings("MultiSource");
            numSources = stringSources.length;
        }

        @Override
        public void reduce(IntWritable nid, Iterable<PageRankNode> values, Context context)
                throws IOException, InterruptedException {
            int massMessages = 0;

            ArrayListOfFloatsWritable mass = new ArrayListOfFloatsWritable();
            for (int i = 0; i < numSources; i++) {
                mass.add(Float.NEGATIVE_INFINITY);
            }

            for (PageRankNode n : values) {
                if (n.getType() == PageRankNode.Type.Structure) {
                    context.write(nid, n);
                } else {
                    for (int i = 0; i < numSources; i++) {
                        mass.set(i, sumLogProbs(mass.get(i), n.getPageRank().get(i)));
                    }

                    massMessages++;
                }
            }

            if (massMessages > 0) {
                intermediateMass.setNodeId(nid.get());
                intermediateMass.setType(PageRankNode.Type.Mass);
                intermediateMass.setPageRank(mass);

                context.write(nid, intermediateMass);
            }
        }
    }

    private static class ReduceClass extends
            Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {
        private ArrayListOfFloatsWritable totalMass = new ArrayListOfFloatsWritable();
        private static int numSources = 0;

        @Override
        public void setup(Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode>.Context context) {
            String[] stringSources = context.getConfiguration().getStrings("MultiSource");
            numSources = stringSources.length;
            for (int i = 0; i < numSources; i++) {
                totalMass.add(Float.NEGATIVE_INFINITY);
            }
        }

        @Override
        public void reduce(IntWritable nid, Iterable<PageRankNode> iterable, Context context)
                throws IOException, InterruptedException {
            Iterator<PageRankNode> values = iterable.iterator();

            PageRankNode node = new PageRankNode();

            node.setType(PageRankNode.Type.Complete);
            node.setNodeId(nid.get());

            int massMessagesReceived = 0;
            int structureReceived = 0;

            ArrayListOfFloatsWritable mass = new ArrayListOfFloatsWritable();
            for (int i = 0; i < numSources; i++) {
                mass.add(Float.NEGATIVE_INFINITY);
            }

            while (values.hasNext()) {
                PageRankNode n = values.next();

                if (n.getType().equals(PageRankNode.Type.Structure)) {
                    ArrayListOfIntsWritable list = n.getAdjacenyList();
                    structureReceived++;

                    node.setAdjacencyList(list);
                } else {
                    for (int i = 0; i < numSources; i++) {
                        mass.set(i, sumLogProbs(mass.get(i), n.getPageRank().get(i)));
                    }

                    massMessagesReceived++;
                }
            }

            node.setPageRank(mass);
            context.getCounter(PageRank.massMessagesReceived).increment(massMessagesReceived);

            if (structureReceived == 1) {
                context.write(nid, node);
                for (int i = 0; i < numSources; i++) {
                    totalMass.set(i, sumLogProbs(totalMass.get(i), mass.get(i)));
                }
            } else if (structureReceived == 0) {
                context.getCounter(PageRank.missingStructure).increment(1);
                LOG.warn("No structure received for nodeid: " + nid.get() + " mass: "
                        + massMessagesReceived);
            } else {
                throw new RuntimeException("Multiple structure received for nodeid: " + nid.get()
                        + " mass: " + massMessagesReceived + " struct: " + structureReceived);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String taskId = conf.get("mapred.task.id");
            String path = conf.get("PageRankMassPath");

            Preconditions.checkNotNull(taskId);
            Preconditions.checkNotNull(path);

            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);

            for (int i = 0; i < numSources; i++) {
                out.writeFloat(totalMass.get(i));
            }
            out.close();
        }
    }

    private static class MapPageRankMassDistributionClass extends
            Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {
        private ArrayListOfFloatsWritable missingMass = new ArrayListOfFloatsWritable();
        private static int[] intSources = null;
        private static int numSources;

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();

            String[] stringSources = conf.getStrings("MultiSource");
            String[] missingMassString = conf.getStrings("MissingMass");
            numSources = stringSources.length;
            intSources = new int[numSources];

            for (int i = 0; i < numSources; i++) {
                missingMass.add(Float.parseFloat(missingMassString[i]));
                intSources[i] = Integer.parseInt(stringSources[i]);
            }
        }

        @Override
        public void map(IntWritable nid, PageRankNode node, Context context)
                throws IOException, InterruptedException {
            ArrayListOfFloatsWritable p = node.getPageRank();

            for (int i = 0; i < numSources; i++) {
                if (nid.get() == intSources[i]) {
                    float jump = (float) Math.log(ALPHA);
                    float mMass = missingMass.get(i);
                    float link;
                    if (mMass == 0.0f) {
                        link = (float) Math.log(1.0f - ALPHA) + sumLogProbs(p.get(i), Float.NEGATIVE_INFINITY);
                    } else {
                        link = (float) Math.log(1.0f - ALPHA) + sumLogProbs(p.get(i), (float) Math.log(mMass));
                    }
                    p.set(i, sumLogProbs(jump, link));
                } else {
                    float link = (float) Math.log(1.0f - ALPHA) + p.get(i);
                    p.set(i, link);
                }
            }
            node.setPageRank(p);

            context.write(nid, node);
        }
    }

    private static float ALPHA = 0.15f;
    private static NumberFormat formatter = new DecimalFormat("0000");

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RunPersonalizedPageRankBasic(), args);
    }

    public RunPersonalizedPageRankBasic() {}

    private static final String BASE = "base";
    private static final String NUM_NODES = "numNodes";
    private static final String START = "start";
    private static final String END = "end";
    private static final String SOURCES = "sources";
    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("base path").create(BASE));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("start iteration").create(START));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("end iteration").create(END));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of nodes").create(NUM_NODES));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("sources").create(SOURCES));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(BASE) || !cmdline.hasOption(START) ||
                !cmdline.hasOption(END) || !cmdline.hasOption(NUM_NODES) || !cmdline.hasOption(SOURCES)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String basePath = cmdline.getOptionValue(BASE);
        int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
        int s = Integer.parseInt(cmdline.getOptionValue(START));
        int e = Integer.parseInt(cmdline.getOptionValue(END));
        String sources = cmdline.getOptionValue(SOURCES);

        LOG.info("Tool name: RunPageRank");
        LOG.info(" - base path: " + basePath);
        LOG.info(" - num nodes: " + n);
        LOG.info(" - start iteration: " + s);
        LOG.info(" - end iteration: " + e);
        LOG.info("- sources: " + sources);

        for (int i = s; i < e; i++) {
            iteratePageRank(i, i + 1, basePath, n, sources);
        }

        return 0;
    }

    private void iteratePageRank(int i, int j, String basePath, int numNodes, String sources) throws Exception {
        String[] eachSource = sources.split("\\,");
        int numSources = eachSource.length;
        // Job 1: distribute PageRank mass along outgoing edges.
        ArrayListOfFloatsWritable totalMass = phase1(i, j, basePath, numNodes, sources, numSources);

        ArrayListOfFloatsWritable missingMass = new ArrayListOfFloatsWritable();
        for (int m = 0; m < numSources; m++) {
            float mMass = 1.0f - (float) StrictMath.exp(totalMass.get(m));
            if (mMass <= 0.0f) {
                missingMass.add(0);
            } else {
                missingMass.add(mMass);
            }
        }

        // Job 2: distribute missing mass, take care of random jump factor.
        phase2(j, missingMass, basePath, numNodes, sources, numSources);
    }

    private ArrayListOfFloatsWritable phase1(int i, int j, String basePath, int numNodes,
                                             String sources, int numSources) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("PageRank:Basic:iteration" + j + ":Phase1");
        job.setJarByClass(RunPersonalizedPageRankBasic.class);

        String in = basePath + "/iter" + formatter.format(i);
        String out = basePath + "/iter" + formatter.format(j) + "t";
        String outm = out + "-mass";

        int numPartitions = 0;
        for (FileStatus s : FileSystem.get(getConf()).listStatus(new Path(in))) {
            if (s.getPath().getName().contains("part-"))
                numPartitions++;
        }

        LOG.info("PageRank: iteration " + j + ": Phase1");
        LOG.info(" - input: " + in);
        LOG.info(" - output: " + out);
        LOG.info(" - nodeCnt: " + numNodes);
        LOG.info("computed number of partitions: " + numPartitions);

        int numReduceTasks = numPartitions;

        job.getConfiguration().setInt("NodeCount", numNodes);
        job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
        job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
        job.getConfiguration().set("PageRankMassPath", outm);
        job.getConfiguration().setStrings("MultiSource", sources);

        job.setNumReduceTasks(numReduceTasks);

        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PageRankNode.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PageRankNode.class);

        job.setMapperClass(MapClass.class);

        job.setCombinerClass(CombineClass.class);

        job.setReducerClass(ReduceClass.class);

        FileSystem.get(getConf()).delete(new Path(out), true);
        FileSystem.get(getConf()).delete(new Path(outm), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        ArrayListOfFloatsWritable totalMass = new ArrayListOfFloatsWritable();
        for (int m = 0; m < numSources; m++) {
            totalMass.add(Float.NEGATIVE_INFINITY);
        }

        FileSystem fs = FileSystem.get(getConf());
        for (FileStatus f : fs.listStatus(new Path(outm))) {
            FSDataInputStream fin = fs.open(f.getPath());
            for (int n = 0; n < numSources; n++) {
                totalMass.set(n, sumLogProbs(totalMass.get(n), fin.readFloat()));
            }
            fin.close();
        }

        return totalMass;
    }

    private void phase2(int j, ArrayListOfFloatsWritable missingMass, String basePath, int numNodes,
                        String sources, int numSources) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("PageRank:Basic:iteration" + j + ":Phase2");
        job.setJarByClass(RunPersonalizedPageRankBasic.class);

        StringBuilder missingMassStringBuilder = new StringBuilder();
        missingMassStringBuilder.append(missingMass.get(0));
        for (int m = 1; m < numSources; m++) {
            missingMassStringBuilder.append(",").append(missingMass.get(m));
        }
        String missingMassSting = missingMassStringBuilder.toString();

        String in = basePath + "/iter" + formatter.format(j) + "t";
        String out = basePath + "/iter" + formatter.format(j);

        LOG.info("PageRank: iteration " + j + ": Phase2");
        LOG.info("missing PageRank mass: " + missingMassSting);
        LOG.info(" - input: " + in);
        LOG.info(" - output: " + out);

        job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
        job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
        job.getConfiguration().setStrings("MissingMass", missingMassSting);
        job.getConfiguration().setStrings("MultiSource", sources);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PageRankNode.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PageRankNode.class);

        job.setMapperClass(MapPageRankMassDistributionClass.class);

        FileSystem.get(getConf()).delete(new Path(out), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }

    private static float sumLogProbs(float a, float b) {
        if (a == Float.NEGATIVE_INFINITY)
            return b;

        if (b == Float.NEGATIVE_INFINITY)
            return a;

        if (a < b) {
            return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
        }

        return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
    }
}