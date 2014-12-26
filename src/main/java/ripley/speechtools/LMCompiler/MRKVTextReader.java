package ripley.speechtools.LMCompiler;

import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Iterator;

import edu.berkeley.nlp.lm.StringWordIndexer;
import edu.berkeley.nlp.lm.io.IOUtils;
import edu.berkeley.nlp.lm.io.LmReader;
import edu.berkeley.nlp.lm.io.LmReaderCallback;
import edu.berkeley.nlp.lm.util.LongRef;

/**
 * The MapReduce KV text reader is intended to read results from the
 * ripley.speechtools mapreduce distributed ngram counting task.
 * The expected format of the input text file is shown below, and
 * the document must be ordered such that all unigrams occur in the document
 * before any bigrams, and generally that all order M grams occur before any
 * order (M+1) grams in the document.
 * 
 * unigram  [oberserved count in training data]
 * unigram  [oberserved count in training data]
 * ...
 * bigramWord1 bigramWord2 [oberserved count in training data]
 * bigramWord1 bigramWord2 [oberserved count in training data]
 * ... 
 * trigramWord1 trigramWord2 trigramWord3 [oberserved count in training data]
 * trigramWord1 trigramWord2 trigramWord3 [oberserved count in training data]
 * ...
 * @author kyle
 *
 */
public class MRKVTextReader implements LmReader<LongRef, LmReaderCallback<LongRef>> {
  private final StringWordIndexer sWordIndexer;
  private final Iterator<String> lineIterator;
  
  public MRKVTextReader(final InputStream is, final StringWordIndexer swi) {
    this(getLineIterator(is), swi);
  }
  
  public MRKVTextReader(Iterator<String> lineIterator, final StringWordIndexer swi) {
    this.lineIterator = lineIterator;
    this.sWordIndexer = swi;
  }
  
  /**
   * Reads newline-separated plain text from a single input file with the 
   * format represented in the file comments above. Writes the ngrams with
   * observed count values to the provided LmReaderCallback object.
   * 
   * @param inputFiles
   * @param outputFile
   */
  public void parse(final LmReaderCallback<LongRef> callback) {
    int[] indexedNgram;
    long observedCount;
    String[] words;
    String line;
    
    while (true == this.lineIterator.hasNext()) {
      // Create ngram int array containing word indexes for each word in
      // the ngram by splitting on whitespace.
      line = lineIterator.next().trim();
      words = line.split("\\s+");
      
      // Consider line garbage if it does not contain at least two fields
      if (words.length < 2) {
        continue;
      }
      
      // Get observed count value and package in KneserNeyCounts object
      try {
        observedCount = Long.parseLong(words[words.length-1]);
      } catch (Exception ex) {
        // Failed number parse.  Consider line garbage.
        continue;
      }
      
      // Utilize WordIndexer to fill in cells of int index array
      indexedNgram = new int[words.length - 1];
      for (int i = 0; i < (words.length - 1); i++) {
        indexedNgram[i] = this.sWordIndexer.getOrAddIndexFromString(words[i]);
      }
      
      // Sanity check the observed count
      if (observedCount < 1) {
        // Consider line of input to be garbage
        continue;
      }
      
      // Add ngram to HashNgramMap
      callback.call(indexedNgram, 0, indexedNgram.length, new LongRef(observedCount), line);
    }
    
    callback.cleanup();
  }
  
  /**
   * Creates an Iterator over lines of the the input.  Currently only supports
   * usage of a single file.  The IOUtils class can support flattening of
   * multiple iterators into a single Iterable objects.
   * 
   * @param is
   * @return
   */
  private static Iterator<String> getLineIterator(final InputStream is) {
    
    // Create BufferedReader to allow access to more file systems (e.g., 
    // hadoop hdfs)
    BufferedReader br = new BufferedReader(
        new InputStreamReader(is));
    
    // Use IOUtils from berkeleylm to create an iterator over the lines
    // in the InputStream
    try {
      return IOUtils.lineIterator(br);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}
