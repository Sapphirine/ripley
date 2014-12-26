package ripley.speechtools.LMCompiler;

import java.util.Arrays;

import edu.berkeley.nlp.lm.ConfigOptions;
import edu.berkeley.nlp.lm.WordIndexer;
import edu.berkeley.nlp.lm.io.KneserNeyLmReaderCallback;
import edu.berkeley.nlp.lm.map.HashNgramMap;
import edu.berkeley.nlp.lm.util.LongRef;
import edu.berkeley.nlp.lm.values.KneserNeyCountValueContainer;
import edu.berkeley.nlp.lm.values.KneserNeyCountValueContainer.KneserNeyCounts;

/**
 * The RipleyKneserNeyLmReaderCallback class modifies the method
 * used by the extended class for communicating ngrams and
 * associated information to provided ArpaLmReaderCallback
 * objects.  The class acts as a data filter in a sequence of
 * data handlers, where the source is of type LmReader and the
 * sink is of type LmReaderCallback.  A LmReader is either provided
 * or loads by itself information associated with language model
 * construction.  The parse function for an LmReader then
 * processes this data per the data filtering responsibilities of
 * this object in the sequence of data filters, and passes the
 * ngram information to the next data handler in the sequence.
 *  
 * @author kyle
 *
 */
public class RipleyKneserNeyLmReaderCallback
    extends KneserNeyLmReaderCallback<String> {

  public RipleyKneserNeyLmReaderCallback(final WordIndexer<String> wordIndexer, final int maxOrder) {
    super(wordIndexer, maxOrder);
  }
  
  public RipleyKneserNeyLmReaderCallback(final WordIndexer<String> wordIndexer, final int maxOrder, final ConfigOptions opts) {
    super(wordIndexer, maxOrder, opts);
  }
  
  /**
   * The call function is overwritten such that the LmReader calling this
   * LmReaderCallback function, where the LmReader is intended to be the class
   * MRKVTextReader in this implementation, communicates ngram observations
   * through this call function.  
   * 
   * The call takes a performance hit by not caching the context and suffix
   * offsets and instead relying on the helper function in HashNgramMap
   * further down in the processing pipeline to recreate those values during
   * runtime.
   */
  @Override
  public void call(int[] ngram, int startPos, int endPos, LongRef value,
      String words) {
    addNgram(ngram, startPos, endPos, value, words);
  }
  
  /**
   * This addNgram function is preferred by this class because the words in the
   * provided array represent a single ngram, and are not intended to be
   * processed to extract possibly multiple ngrams.
   * 
   * For example, a line from an input file may read "delta fifty four ninety
   * six atlanta tower cleared to land runway two six right".  Earlier
   * processing is reponsible for breaking up this larger string into ngrams,
   * one of which might be 'delta fifty four'.  That ngram needs to be added to
   * the HashNgramMap contained in the this.super class, so addNgram(...) will
   * be called with the example input below.  Notice only a single ngram should
   * be added to the HashNgramMap for each call to this function.  The function
   * could be simplified, as in its current form it was largely coped from
   * the berkeleylm project and constrained in its processing goals.
   * Example input:  ngram[] = { index("delta"), index("fifty"), index("four") }
   * Created ngram:  'delta fifty four'
   * 
   * @param ngram - The ngram array contains the word indexes for each word
   *                that was in the string provided for ngram creation
   * @param startPos - Inclusive start index of ngram of interest in ngram array
   * @param endPos - Exclusive end index of ngram of interest in ngram array
   * @param value - Generally frequency count of ngram in training data
   * @param words - The input line processed to create the int[] ngram array and
   *                extract ngrams
   */
  public void addNgram(final int[] ngram, final int startPos, final int endPos,
      final LongRef value, @SuppressWarnings("unused") final String words) {
    
    // Return if no ngram information is present
    if (0 == ngram.length) {
      return;
    }

    final KneserNeyCounts knCounts = new KneserNeyCounts();
    knCounts.tokenCounts = value.value;
    
    // Consider removing rehashIfNecessary function.  Does not seem necessary. 
    //ngrams.rehashIfNecessary(endPos - startPos);
    long putResult = ngrams.put(ngram, startPos, endPos, knCounts);
    
  }
}
