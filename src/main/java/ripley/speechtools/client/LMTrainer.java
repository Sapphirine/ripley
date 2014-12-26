package ripley.speechtools.client;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

import ripley.speechtools.mapper.NgramMapper;
import ripley.speechtools.partitioner.NgramOrderPartitioner;
import ripley.speechtools.reducer.NgramReducer;

/**
 * The LMTrainer attempts to process input text documents to extract the
 * necessary features from them to construct a language model representing the
 * domain of the processed documents.
 * 
 * Author: Kyle White
 */
public class LMTrainer extends Configured implements Tool {
  public static final int NGRAM_COUNT = 3;

  /**
   * The main method below represents the entry point for the hadoop tool
   * application to go from text files with independent line entries to an
   * intermediate ngram count document.  The count document formatted as shown
   * in file comments for class ripley.speechtools.LMCompiler.MRKVTextReader.
   * The input and ngram counts documents are stored in HDFS
   * (Hadoop Distributed File System).
   * Possible Usage:  hadoop --config [hadoopConfigDir] jar [ripleyjar] \
   *                  ripley.speechtools.client.LMTrainer \
   *                  [inputFilePath] [outputFilePath]
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    // ToolRunner handles generic command-line options 
    Configuration conf = new Configuration();
    LMTrainer lmt = new LMTrainer();
    int res = ToolRunner.run(conf, lmt, args);

    System.exit(res);
  }

  /**
   * The Tool interface supports easy handling of command-line options, and is
   * used here to pass on only the custom command line arguments to run().
   * Run is called from ToolRunner in the main() method.
   * @param allArgs
   * @return
   * @throws Exception
   */
  public int run(String[] allArgs) throws Exception {

  // Parse function arguments
  String[] args = new GenericOptionsParser(getConf(), allArgs).getRemainingArgs();
  
  // Ensure correct number of inputs.
  if (2 != args.length) {
    System.out.println("Excepted usage:"
        + "hadoop --config [hadoopConfigDir] jar [ripleyjar] "
        + "ripley.speechtools.client.LMTrainer "
        + "[inputFilePath] [outputFilePath]");
    return 1;
  }

  // Configuration processed by ToolRunner
  Configuration conf = getConf();

  Job job = Job.getInstance(conf);

  // Set Hadoop Client class that initiates the job
  job.setJarByClass(LMTrainer.class);

  // Set the format of the reducer output
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(IntWritable.class);

  // Set the Mapper and Reducer classes for MapReduce framework usage
  job.setMapperClass(NgramMapper.class);
  job.setReducerClass(NgramReducer.class);
  ///job.setNumReduceTasks(NGRAM_COUNT);

  // The ripley.speechtools.partitioner.NgramOrderPartitioner
  // class may be used to aid in MultipleOutputs with a single Reducer
  // or, to instead burden the nodes creating output from Mapper tasks,
  // the class can parameterize a Partitioner.

  // Set the InputFormat class which splits input files into logical splits
  // that are fed to Mapper objects.  By default the TextInputFormat class
  // breaks inputs files into lines.  This is reasonable for a language model
  // being trained from speech transcripts, where the input contains one
  // transcription per line.  Generally speaking, the InputFormat class reads
  // input data and transforms it into splits of key/value pairs per record 
  // the Mapper objects
  job.setInputFormatClass(TextInputFormat.class);

  // The OutputFormat class takes Reducer output and writes plain text files.
  // While the already available TextInputFormat class suited the purposes
  // of this implementation of a language model trainer, the TextOutputFormat
  // class could be swapped for a class which takes the Reducer output
  // and created an ARPA format ngram language model document (see:
  // http://www.speech.sri.com/projects/srilm/manpages/ngram.1.html).
  // Otherwise it creates an intermediate file format that can be converted
  // into ARPA ngram format, possibly after some additional processing.  
  MultipleOutputs.addNamedOutput(job,
      NgramOrderPartitioner.Partitions.UNIGRAM.partitionName(),
      TextOutputFormat.class,
      Text.class, IntWritable.class);
  MultipleOutputs.addNamedOutput(job,
      NgramOrderPartitioner.Partitions.BIGRAM.partitionName(),
      TextOutputFormat.class,
      Text.class, IntWritable.class);
  MultipleOutputs.addNamedOutput(job,
      NgramOrderPartitioner.Partitions.TRIGRAM.partitionName(),
      TextOutputFormat.class,
      Text.class, IntWritable.class);
  
  // Set the input file or directory containing input files with training
  // data.  Input data will be read by the TextInputFormat class.
  FileInputFormat.setInputPaths(job, new Path(args[0]));

  // Set the output file to be utilized by the TextOutputFormat class.
  FileOutputFormat.setOutputPath(job, new Path(args[1]));

  // Submit the job to MapReduce framework.  Changes the system
  // state to RUNNING if successful, and returns regardless.
  job.waitForCompletion(true);

  return 0;
  }
}
