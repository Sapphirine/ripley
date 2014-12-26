package ripley.speechtools.analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.TokenStream;

import java.io.Reader;

/**
 * The Ngram transcript analyzer is intended to act as the text analyzer for
 * a single string of input text.  The Analyzer is the class that wraps a 
 * sequence of Tokenizer and TokenFilter objects under the Lucene Analyzer API
 * for application to text.
 * 
 * @author kyle
 *
 */
public class NgramTranscriptAnalyzer extends Analyzer {
  private static final int MIN_MIN_NGRAM_SIZE = 2;
  private static final int MAX_MIN_NGRAM_SIZE = 10;
  private static final int MIN_MAX_NGRAM_SIZE = 2;
  private static final int MAX_MAX_NGRAM_SIZE = 10;
  
  private int minNgramSize = 2;
  private int maxNgramSize = 3;
  
  public NgramTranscriptAnalyzer(int minNgramSize, int maxNgramSize) {
    this.setMinNgramSize(minNgramSize);
    this.setMaxNgramSize(maxNgramSize);
  }
  
  public int getMinNgramSize() {
    return minNgramSize;
  }
  
  public int getMaxNgramSize() {
    return maxNgramSize;
  }
  
  /**
   * Sets the minimum NGram size to be used by the NGram analyzer.  If the
   * minimum NGram size could not be set as the user requested, the function
   * returns false.
   * @param minNgramSize
   * @return
   */
  public boolean setMinNgramSize(int minNgramSize) {
    boolean requestFlag = true;
    
    // Bound the input parameter
    if (MIN_MIN_NGRAM_SIZE > minNgramSize) {
      minNgramSize = MIN_MIN_NGRAM_SIZE;
      requestFlag = false;
    } else if (MAX_MIN_NGRAM_SIZE < minNgramSize) {
      minNgramSize = MAX_MIN_NGRAM_SIZE;
      requestFlag = false;
    }
    
    this.minNgramSize = minNgramSize;
    
    return requestFlag;
  }
  
  /**
   * Sets the maximum NGram size to be used by the NGram analyzer.  If the
   * maximum NGram size could not be set as the user requested, the function
   * returns false.
   * @param maxNgramSize
   * @return
   */
  public boolean setMaxNgramSize(int maxNgramSize) {
    boolean requestFlag = true;
    
    // Bound the input parameter
    if (MIN_MAX_NGRAM_SIZE > maxNgramSize) {
      maxNgramSize = MIN_MAX_NGRAM_SIZE;
      requestFlag = false;
    } else if (MAX_MAX_NGRAM_SIZE < maxNgramSize) {
      maxNgramSize = MAX_MAX_NGRAM_SIZE;
      requestFlag = false;
    }
    
    this.maxNgramSize = maxNgramSize;
    
    return requestFlag;
  }
  
  /**
   * The chosen sequence of TokenStream objects for the NGram analyzer:
   * WhitespaceTokenizer -> LowerCaseFilter -> ShingleFilter
   * Shingles are the word based NGram filter option available in the Lucene
   * analysis package, as opposed to the word based Ngram options available.
   * 
   * Caution:  Some transcript markup done for specific speech toolkits may not
   * pass through this TokenStream as the consumer intended.  This analyzer
   * was written with an assumption of no meta markup text in the transcripts.
   * @param fieldName
   * @param reader
   * @return
   */
  @Override
  protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
    Tokenizer source = new WhitespaceTokenizer(reader);
    TokenStream filter = new LowerCaseFilter(source);
    filter = new ShingleFilter(filter, this.minNgramSize, this.maxNgramSize);
    return new TokenStreamComponents(source, filter);
  }
  
}

