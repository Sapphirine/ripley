package ripley.speechtools.test;

import ripley.speechtools.analyzer.NgramTranscriptAnalyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Test code pulled from Apache Lucene project examples where possible.
 * The tests below are provided as examples of usage of different Lucene
 * analyzers for the task of ngram production from text.  This is done
 * as the analyzer for a task may be different than the one I selected
 * and this class may encourage and allow sime simple testing of your text
 * across Lucene Analyzer classes.
 * 
 * @author kyle
 *
 */
public class AnalyzerTest {
  private static final String NEW_LINE = System.lineSeparator();
  
  public static void main (String args[]) {
    String testString1 = "a Blue Streak two seventy eight Dulles tower wind two"
        + " zero at six runway one center cleared to land";
    String testString2 = "the Acey forty one ninety two turn left heading two seven"
        + " zero contact Potomac departure";
    String testString3 = "Beechjet one delta romeo runway three zero cleared for"
        + " takeoff turn left heading two four five he's on your right";
    
    // Create empty stop words set
    CharArraySet caSet = new CharArraySet(Collections.EMPTY_SET, true);

    // The Analyzer provides the ability to tokenize text and provide a
    // TokenStream for the given field name

    //Analyzer simpleAnalyzer = new SimpleAnalyzer();
    // The SimpleAnalyzer is not appropriate for LM transcript processing
    // due to deforming contractions due to its use of a LetterTokenizer
    
    // Analyzer analyzer = new StandardAnalyzer(caSet);
    // The SimpleAnalyzer is currently selected for LM transcript processing
    // because although it appears to be working reasonably well now that the
    // StandardFilter does nothing, it previously took inappropriate actions
    // such as modifying contractions.  It may do so again.
    
    // Analyzer analyzer = new WhitespaceAnalyzer();
    // The WhitespaceAnalyzer not appropriate for LM transcript processing
    // due to not having a LowecaseFilter in the pipeline of TokenFilters.
    // The whitespace tokenizer seemed reasonable for this application.
    
    // Analyzer analyzer = new EnglishAnalyzer(caSet);
    // The WhitespaceAnalyzer not appropriate for LM transcript processing
    // due to commonly modifying the text due to actions like stemming.
    
    Analyzer analyzer = new NgramTranscriptAnalyzer(2, 3);
    
    try {
      AnalyzerTest.printTokenStream(analyzer, testString1);
      AnalyzerTest.printTokenStream(analyzer, testString2);
      AnalyzerTest.printTokenStream(analyzer, testString3);
    } catch (IOException ioe) {
      System.out.println("Caught exception attempting to print token stream"
          + " for test string.  Message: " + ioe.getMessage());
    }

  }

  /**
   * Print the tokenized output for the given Analyzer and test String to
   * standard output as a simple test process to understand the tokenization
   * done on the input, and perhaps test alternative tokenization schemes with
   * Lucene Analyzers.
   * @param analyzer
   * @param testString
   * @throws IOException
   */
  private static void printTokenStream(Analyzer analyzer, String testString)
      throws IOException {
    List<String> tokenList = new ArrayList<String>();
    TokenStream tokenStream = null;
    StringBuilder sb = new StringBuilder();
    String token;

    try {
      tokenStream = analyzer.tokenStream("text", testString);
      tokenStream.addAttribute(CharTermAttribute.class);

      // Resets this stream to the beginning. (Required)
      tokenStream.reset();
      while (tokenStream.incrementToken()) {
        token = tokenStream.getAttribute(CharTermAttribute.class).toString();
        tokenList.add(token);
      }
      
      // Perform end-of-stream operations, e.g. set the final offset.
      tokenStream.end();   
    } finally {
      if (null != tokenStream) {
        tokenStream.close(); // Release resources associated with this stream.
      }
    }
    
    // Print out input string and the tokenization as a set to Std out
    sb.append("Input string for tokenization: ");
    sb.append(testString);
    sb.append(NEW_LINE);
    sb.append("The tokenized set {");
    for (String stringElement: tokenList) {
      sb.append(stringElement);
      sb.append(", ");
    }
    sb.append("}.");
    sb.append(NEW_LINE);
    
    System.out.println(sb);

  } // End printTokenStream()
}
