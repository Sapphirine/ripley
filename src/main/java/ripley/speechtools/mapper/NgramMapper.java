package ripley.speechtools.mapper;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import ripley.speechtools.analyzer.NgramTranscriptAnalyzer;

/**
 * The NgramMapper class defines the Mapper for the ngram counting component
 * of the language model training process.
 * 
 * @author kyle
 *
 */
public class NgramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  
  /**
   * The map function is utilized by the Hadoop MapReduce framework to map
   * instances of key/value pairs to be input to the generally co-located 
   * shuffle phase.  The map function here takes as input a single,
   * independent transcript sentence and attempts to construct word-based
   * ngrams from the text via tokenization by the Lucene library for
   * text analysis capabilities.
   * @param key
   * @param value
   * @param context
   * @throws IOException
   * @throws InterruptedException
   */
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    TokenStream tokenStream = null;
    String inputText = value.toString();
    String token;
    
    // The (2, 3) input parameters currently hard code the Ngram analyzer to
    // tokenize the input in preparation for a trigram language model
    Analyzer analyzer = new NgramTranscriptAnalyzer(2, 3);
    
    try {
      tokenStream = analyzer.tokenStream("text", inputText);
      tokenStream.addAttribute(CharTermAttribute.class);

      // Resets this stream to the beginning. (Required)
      tokenStream.reset();
      while (tokenStream.incrementToken()) {
        token = tokenStream.getAttribute(CharTermAttribute.class).toString();
        context.write(new Text(token), new IntWritable(1));
      }
      
      // Perform end-of-stream operations, e.g. set the final offset.
      tokenStream.end();   
    } finally {
      if (null != tokenStream) {
        tokenStream.close(); // Release resources associated with this stream.
      }
    }
    
    // Close resources
    analyzer.close();
    
  } // End map(...)
  
}
