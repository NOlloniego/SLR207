package slr207;

import java.util.HashMap;
import java.util.Scanner;
import java.util.Map;
import java.io.PrintWriter;

/**

 * This class defines the methods needed for counting the occurrences of words in a given text.

 * @author: Nicolas Olloniego

 * @version: 26/04/2020

 */
public class Comptage {
	
	//Use HashMap because it allows to map a string to an integer and modify that value.
	//It does not allow to repeat keys.
	private HashMap<String, Integer> words;	//Stores counters of words
	
	/**
     * Class constructor. Initializes the HashMap.
     */
	public Comptage() {
		words = new HashMap<String, Integer>();
	}
	
	/**
	 * This method adds one to the value of a given key. If the key is not present, it adds it.
	 * @param word Key
	 */
	public void sumOne(String word) {
		if (this.words.containsKey(word))
			this.words.put(word, this.words.get(word)+1);
		else this.words.put(word, 1);
	}
	
	/**
	 * This method sorts and prints into a file the keys and values of the HashMap.
	 */
	public void sortResults() {
		long startTime = 0;
		long endTime = 0;
		try {
		PrintWriter pw = new PrintWriter("output.txt");
		startTime = System.currentTimeMillis();
		words.entrySet()
		  .stream()
		  .sorted(Map.Entry.<String, Integer>comparingByKey())
		  .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
		  .forEach(entry -> {
			  pw.println(entry.getKey() + " " + Integer.toString(entry.getValue()));});
		endTime = System.currentTimeMillis();
		pw.close();
		} catch(Exception e) {e.printStackTrace();}
		long totalTime = endTime - startTime;
		System.out.println(totalTime);
	}
	
	/**
	 * This method counts the occurrency of words in a file.
	 * @param sc Scanner object that reads the file
	 */	public void countWords(Scanner sc) {
		long startTime = System.currentTimeMillis();
		while(sc.hasNext()) {
			this.sumOne(sc.next());
		}
		long endTime = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		 System.out.println(totalTime);
	}
	
}
