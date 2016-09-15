/**
 * A Tool to calculate TF-IDF of a collection of documents
 * @author Rushdi Shams
 * @version 1.0, 13/09/2016
 */

package calculator.idf.tf;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;

public class TFIDFCalculator {
	private File inputDir, outputFile;
	private String[] stopWords;
	private List<List<String>> docs; //contains documents. documents are in form of list of words
	private File[] inputFiles;
	List<String> output;

	//---------------------------------------------------------------------
	//Constructor
	//---------------------------------------------------------------------
	public TFIDFCalculator(String inDir, String outFile, String stopFile){
		inputDir = new File(inDir);
		outputFile = new File(outFile);

		String stops = "";
		try {
			System.out.print("[loading stopwords...");
			stops = FileUtils.readFileToString(new File(stopFile), "UTF-8");
		} catch (IOException e) {
			System.out.println("ERROR!");
		}

		if(stops.length() > 0){
			stopWords = stops.replaceAll("\'", "").split("\r\n");
			System.out.println("DONE!]");
		}

		output = new ArrayList<String>();
	}//end constructor

	//---------------------------------------------------------------------
	//Method Section
	//---------------------------------------------------------------------

	/**
	 * Method to read input documents
	 */
	public void processInputDocuments(){
		inputFiles = inputDir.listFiles();
		Stream<String>[] inputStreams = new Stream[inputFiles.length];
		docs = new ArrayList<List<String>>(inputFiles.length);
		String content = "";
		int i = 0;

		//stop word list
		List<String> stopWordList = new ArrayList<String>(Arrays.asList(stopWords));
		for(File file:inputFiles){
			try {
				System.out.print("[Reading " + file.getAbsolutePath() + "...");
				content = FileUtils.readFileToString(file, "UTF-8");
			} catch (IOException e) {
				System.out.println("ERROR!]");
			}
			System.out.println("DONE!]");
			
			if(content.length() > 0){
				System.out.print("[Processing...");
				//word stream where word is lower cased, numbers and punctuations are stripped off.
				System.out.print("Cleaning...");
				inputStreams[i] = Stream.of(content.toLowerCase().replaceAll("[^a-zA-Z ]", "").split("\\W+")).parallel();
				String[] docWordArray = inputStreams[i].toArray(size -> new String[size]);
				//document bag of words
				List<String> docWordList = new ArrayList<String>(Arrays.asList(docWordArray));
				//removing stop words
				System.out.print("Removing stopwods...");
				docWordList.removeAll(stopWordList);
				//adding document to our list
				docs.add(docWordList);
				System.out.println("DONE!]");
			}//text processing complete
			i++;
		}//next input file
	}//end method to read the input files

	/**
	 * Method to calculate and return terms, their frequency in each document and in a collection, inverse document frequency in a collection, normalized TF-Idf and raw F-IDF
	 * @param inputFile file for which stats will be calculated
	 * @param doc contains the list of words for inputFile
	 * @param docs collection of doc
	 * @return the statistics in csv String format
	 */
	public String collectStats(File inputFile, List<String> doc, List<List<String>> docs) {
		System.out.print("[Collecting stats for " + inputFile.getAbsolutePath() + "...");
		Map<String, Long> docMap = getTf(doc); //key-frequency map
		StringBuilder csv = new StringBuilder(); //output string
		//for each word in the current document-->
		for(String key: docMap.keySet()){
			long frequency = docMap.get(key); //get word frequency
			int df = getDf (key, docs); //get word's document frequency
			double idf = Math.log10(docs.size() / (double)df); //get word's inverse document frequency
			csv.append(inputFile.getAbsolutePath() + "," + key + "," + frequency + "," + df + "," + idf + "," + (1 + Math.log10(frequency)) + "," + (frequency * idf) + "," + ((1 + Math.log10(frequency)) * idf)  +"\n");
		}//next word in the document
		System.out.println("DONE!]");
		return csv.toString();
	}//end method that calculates TF-IDF of words in a document

	/**
	 * Method to calculate and return IDF
	 * @param term is sent as a string. The method calculates its IDF in the collection of docs
	 * @param docs is the collection of documents
	 * @return document frequency as integer
	 */
	private int getDf (String term, List<List<String>> docs) {
		int df = 0;
		//for each document in the collection-->
		for (List<String> doc : docs) {
			//for each word in a document-->
			for (String word : doc) {
				if (term.equalsIgnoreCase(word)) { //if the document contains the word, increase document frequency; no need to search further
					df++;
					break;
				}
			}//next word in a document
		}//next document in a collection
		return df;
	}//end method that calculates DF

	/**
	 * Method that calculates term frequency in a list
	 * @param doc is the list of words in a document
	 * @return raw frequency map of words in a document
	 */
	private Map<String, Long> getTf(List<String> doc) {
		String listString = String.join(", ", doc);
		Stream<String> wordStream = Stream.of(listString.toLowerCase().split("\\W+")).parallel();
		Map<String, Long> frequency = wordStream
				.collect(Collectors.groupingBy(String::toString,Collectors.counting()));
		return frequency;
	}//end method that c

	public void generateStats(TFIDFCalculator calculator){
		System.out.println("[Generating Stats...");
		calculator.output.add(",Term,F,DF, IDF,TF,F X IDF,TF X IDF\n");
		for(int i = 0; i < calculator.docs.size(); i++){
			calculator.output.add(calculator.collectStats(calculator.inputFiles[i], calculator.docs.get(i), calculator.docs));
		}
		System.out.println("DONE!]");
	}

	public void writeOutput(TFIDFCalculator calculator){
		try {
			System.out.print("[Writing output...");
			FileUtils.writeLines(calculator.outputFile, calculator.output, false);
		} catch (IOException e) {
			System.out.println("ERROR!");
		}
		System.out.println("DONE!]");
	}
	public static void main(String[] args){
		TFIDFCalculator calculator = new TFIDFCalculator(args[0], args[1], args[2]);
		calculator.processInputDocuments();
		calculator.generateStats(calculator);
		calculator.writeOutput(calculator);
	}
}
