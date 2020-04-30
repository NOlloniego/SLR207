package slr207;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Scanner;

/**

 * This class defines the Main program for counting the occurrences of words in given texts in a sequential manner.

 * @author: Nicolas Olloniego

 * @version: 26/04/2020

 */
public class Main {
	
	/**
	 * Main method. Counts the occurrences of words in given texts in a sequential manner.
	 * @param args[0] Name of the file to work with
	 */
	public static void main(String[] args) {
		Comptage count = new Comptage();
		FileReader fr = null;
		try {
			fr = new FileReader(args[0]);
			BufferedReader br = new BufferedReader(fr);
			Scanner sc= new Scanner(br);
			count.countWords(sc);
		} catch(Exception e) {
		e.printStackTrace();
		} finally {
			try { fr.close();} catch(Exception e) {}
		}
		count.sortResults();
	}
}
