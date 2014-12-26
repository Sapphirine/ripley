package ripley.speechtools.partitioner;

import org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedPartitioner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * The NgramOrderPartitioner class is intended to group the ngrams counted by
 * the Mapper classes by ngram order.  This is done for the
 * benefit of language modeling tools that prefer seeing lower order ngrams
 * before higher order ngrams (e.g., berkeleylm v1.1.5). 
 * 
 * Comment:  This class can either be used as a traditional partitioner or as a
 * helper class along with the 
 * org.apache.hadoop.mapreduce.lib.output.MultipleOutputs class.  The latter
 * burdens the nodes running the reducer, while the former generally runs a
 * Partitioner on the node which has created Mapper output.  The partitioner
 * will help determine how the intermediate results from the Mapper class
 * should be routed.  A decision can be made based on your particular problem
 * and bottleneck.
 * 
 * @author kyle
 *
 */
public class NgramOrderPartitioner extends KeyFieldBasedPartitioner<Text,IntWritable> {
  

  public enum Partitions {
    UNIGRAM   (1, "unigram"),
    BIGRAM    (2, "bigram"),
    TRIGRAM   (3, "trigram"),
    TETRAGRAM (4, "tetragram"),
    PENTAGRAM (5, "pentagram"),
    HEXAGRAM  (6, "hexagram"),
    HEPTAGRAM (7, "heptagram"),
    OCTAGRAM  (8, "octagram"),
    NONAGRAM  (9, "nonagram"),
    DECAGRAM  (10, "decagram");

    // Partition Identifier
    private final int partitionID;
    private final String partitionName;
    
    Partitions(int partitionID, String partitionName) {
        this.partitionID = partitionID;
        this.partitionName = partitionName;
    }
    
    public int partitionID() { return this.partitionID; }
    public String partitionName() { return this.partitionName; }
}
  
  /**
   * Determines the partition number for a record.  The ripley package recommends
   * for impact in language model training that the number of reduce tasks be
   * equal to the max ngram size used (i.e., if training a trigram language
   * model, create 3 reducer tasks).
   */
  @Override
  public int getPartition(Text key, IntWritable value, int numReduceTasks) {
    int partitionID = 0;

      String k = key.toString().trim();
      if ( true == k.isEmpty() ) {
        // Default to sending this to the unigram partition
        partitionID = Partitions.UNIGRAM.partitionID % numReduceTasks;
      } else {
        partitionID = k.split("\\s+").length % numReduceTasks;
      }
    
    return partitionID;
  }
  
}
