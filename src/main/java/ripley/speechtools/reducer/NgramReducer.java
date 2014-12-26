package ripley.speechtools.reducer;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import ripley.speechtools.partitioner.*;

/**
 * The NgramReducer is currently a generic class that resembles a number of
 * word count map reduce examples.  The nature of the final language model
 * processing steps as initially tried in this project conflict with the
 * constraints for implementation as a Reduce step.  The creation of
 * a language model with final probabilities will be done after the writing
 * of ngram counts to file via a map reduce job.
 * 
 * @author kyle
 *
 */
public class NgramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  private NgramOrderPartitioner ngramPartitioner;
  private MultipleOutputs<Text, IntWritable> mos;
  private Configuration conf;
  
  public NgramReducer() {
    ngramPartitioner = new NgramOrderPartitioner();
  }
  
  /**
   * The NgramReducer is setup on initialization with the given context.
   * @param context
   */
  @Override
  public void setup(Context context) {
    try {
      super.setup(context);
      this.conf = context.getConfiguration();
    } catch(Exception ex) {
      // Caught exception.  Logging not yet incorporated into Package.
    }
    mos = new MultipleOutputs<Text, IntWritable>(context);
  }

  /**
   * The reduce function is utilized the the Hadoop MapReduce framework to
   * reduce sets of values associated with a particular key to a single key/value
   * output.  Results are shuffled from the map phases to map their output to
   * the appropriate Reducers, where the intermediate results are sorted and
   * merged according to key.  Afterwards a Reducer is called on each merged
   * set of intermediate results on a key grouped basis.
   * @param key
   * @param values
   * @param context
   * @throws IOException
   * @throws InterruptedException
   */
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context) throws IOException, InterruptedException {
    String namedOutput = null;
    int sum = 0;
    int partition = 0;
    IntWritable result;
    
    for (IntWritable val : values) {
      sum += val.get();
    }
    result = new IntWritable(sum);
    
    // Temporarily hard coded to three partitions for trigram lm creation
    partition = ngramPartitioner.getPartition(key, result,
        NgramOrderPartitioner.Partitions.TRIGRAM.partitionID());
    
    
    switch(partition) {
      case 1:
        namedOutput = NgramOrderPartitioner.Partitions.UNIGRAM.partitionName();
        break;
      case 2:
        namedOutput = NgramOrderPartitioner.Partitions.BIGRAM.partitionName();
        break;
      case 3:
        namedOutput = NgramOrderPartitioner.Partitions.TRIGRAM.partitionName();
        break;
      default:
        // Place data in largest ngram partition, as there are known, additional
        // dangers in placing a high order ngram in lower order ngram partitions
        namedOutput = NgramOrderPartitioner.Partitions.TRIGRAM.partitionName();
        break;
    }
    
    mos.write(namedOutput, key, result, "/user/" + context.getUser() + "/" + namedOutput);
  }
  
}