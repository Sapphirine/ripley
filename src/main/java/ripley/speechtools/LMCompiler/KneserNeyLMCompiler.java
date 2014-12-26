package ripley.speechtools.LMCompiler;

import java.io.InputStream;
import java.io.PrintWriter;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

import edu.berkeley.nlp.lm.StringWordIndexer;
import edu.berkeley.nlp.lm.io.KneserNeyFileWritingLmReaderCallback;

/**
 * The KneserNeyLMCompiler is intended to transform the key/value pairs that
 * the reduce method outputs to the output records into a language model
 * in a standard format (ARPA n-gram format) with final n-gram probabilities
 * determined using Kneser-Ney probability smoothing.  The class currently
 * assumes construction of a trigram language model.
 * 
 * The class should be able to be run via the hadoop command, and thus
 * implements the Tool interface (i.e., run via 'hadoop jar [ripleyjar] ...').
 * 
 * @author kyle
 *
 */
public class KneserNeyLMCompiler extends Configured implements Tool {
  // The following symbols are currently dependent on the input.
  // That is, no START or END symbols will be automatically generated as in
  // the berkeleylm toolkit.  If you wish to utilize start and end symbols,
  // they must be embedded in the training data such as with Sphinx v1.06b.
  // Example:  <s> Execjet twenty nine thirty two cleared for rnp approach </s>
  private static final String WORD_INDEXER_START_SYMBOL = "<s>";
  private static final String WORD_INDEXER_END_SYMBOL = "</s>";
  private static final String WORD_INDEXER_UNKNOWN_SYMBOL = "<unk>";

  // This value should match the largest ngram you wish to read from the
  // intermediate ngram count document.
  private static final int HARD_CODED_MAX_LM_ORDER = 3;

  private StringWordIndexer sWordIndexer;
  private int maxLmOrder;

  /**
   * The constructor primarily sets the configuration and WordIndexer to be used
   * during the LM creation process.  It also sets the LM order of the process,
   * which should match the largest ngram read from the intermediate ngram
   * count document.
   * @param swi
   * @param conf
   */
  public KneserNeyLMCompiler(StringWordIndexer swi, Configuration conf) {
    this.setConf(conf);
    this.sWordIndexer = swi;
    this.sWordIndexer.setStartSymbol(WORD_INDEXER_START_SYMBOL);
    this.sWordIndexer.setEndSymbol(WORD_INDEXER_END_SYMBOL);
    this.sWordIndexer.setUnkSymbol(WORD_INDEXER_UNKNOWN_SYMBOL);
    this.sWordIndexer.getOrAddIndexFromString(WORD_INDEXER_START_SYMBOL);
    this.sWordIndexer.getOrAddIndexFromString(WORD_INDEXER_END_SYMBOL);
    this.sWordIndexer.getOrAddIndexFromString(WORD_INDEXER_UNKNOWN_SYMBOL);
    maxLmOrder = HARD_CODED_MAX_LM_ORDER;
  }

  /**
   * Tool interface receives custom arguments from command lines, and triggers
   * the LM creation process, which can be viewed as multiple data handlers/
   * processors along a linear data flow path.
   * @param allArgs
   * @return
   * @throws Exception
   */
  public int run(String[] allArgs) throws Exception {

    // Parse function arguments utilizing hadoop parser
    String[] args = new GenericOptionsParser(getConf(),
        allArgs).getRemainingArgs();

    // Ensure correct number of inputs.
    if (2 != args.length) {
      System.out.println("Expected Usage: "
          + "hadoop --config [hadoopConfigDir] jar [ripleyjar] "
          + "ripley.speechtools.LMCompiler.KneserNeyLMCompiler "
          + "[inputFilePath] [outputFilePath]");
      return 1;
    }

    // Trigger the sequence of actors in the stream of objects
    // that process the language model information in the act
    // of going from hadoop mapreduce ngram count documents to a
    // ARPA format ngram language model document
    triggerDataPipeline(args);
    return 0;
  }

  /**
   * The data pipeline analogy and method is meant to represent a number
   * of data handlers/processors arranged in linear fashion along a linear
   * data flow path.  The data handlers are linked using the berkeleylm
   * classes LmReader and LmReaderCallback (i.e., source and sink, respectively)
   * 
   * @param args - custom arguments from hadoop jar command line call
   * @throws Exception
   */
  public void triggerDataPipeline(String[] args) throws Exception {

    // Set up input and output file paths
    Path sourceFile = new Path(args[0]);
    Path targetFile = new Path(args[1]);
    FileSystem fs = FileSystem.get(this.getConf());
    InputStream is = fs.open(sourceFile);

    // Create file to read input file in hdfs line by line, assuming one ngram
    // is present per line.
    final MRKVTextReader lmReader = new MRKVTextReader(is, this.sWordIndexer);

    // Create the LmReaderCallback object to receive data from the
    // MRKVTextReader
    RipleyKneserNeyLmReaderCallback kneserNeyReader = 
        new RipleyKneserNeyLmReaderCallback(this.sWordIndexer, maxLmOrder);

    // Parse the input file with the LmReader object, outputting the parse
    // results to the registered callback object.
    lmReader.parse(kneserNeyReader);

    // Prepare for writing Ngram in ARPA format to destination Path
    OutputStream fsos = null;
    PrintWriter pw = null;
    try {
      // Set up PrintWriter for target file
      fsos = FileSystem.create(fs, targetFile, FsPermission.getFileDefault());
      pw = new PrintWriter(fsos);

      // Reuse berkeleylm class KneserNeyFileWritingLmReaderCallback as
      // a LmReaderCallback class with no modification for writing out the
      // ARPA format LM document
      kneserNeyReader.parse(new KneserNeyFileWritingLmReaderCallback<String>(
          pw, this.sWordIndexer));
    } finally {
      pw.close();
    }

  }

  /**
   * The main method below represents the entry point for the hadoop tool
   * application to go from an intermediate ngram document to an ARPA format
   * language model document stored, where the input and output files are
   * stored in HDFS (Hadoop Distributed File System).
   * Possible Usage:  hadoop --config [hadoopConfigDir] jar [ripleyjar] \
   *                  ripley.speechtools.LMCompiler.KneserNeyLMCompiler \
   *                  [inputFilePath] [outputFilePath]
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    // ToolRunner handles generic command-line options 
    Configuration conf = new Configuration();
    StringWordIndexer swi = new StringWordIndexer();
    KneserNeyLMCompiler knlc = new KneserNeyLMCompiler(swi, conf);
    int res = ToolRunner.run(conf, knlc, args);
    System.exit(res);
  }
}
