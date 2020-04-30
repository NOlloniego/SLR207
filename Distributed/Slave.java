import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.net.UnknownHostException;
import java.io.File;
import java.util.Scanner;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**

 * This class defines a Slave that can execute the 3 phases of the MapReduce method.

 * @author: Nicolas Olloniego

 * @version: 26/04/2020

 */
public class Slave {
	
	/**
     * Empty constructor
     */
	public Slave() {}
	
	/**
	 * This method creates a directory, allowing to decide between doing it in the local machine or in a distant one
	 * @param machine Machine in which it has to create the directory.
	 * @param dir Name of the directory to create
	 * @param local Flag that indicates if the directory should be created in the local machine or in a distant one
	 * @param MaxNumberOfTries Maximum number of times we may try to establish an ssh connection
	 */
	public void makeDir(String machine, String dir, boolean local, int MaxNumberOfTries) {
		
		ProcessBuilder pb;
		Process process;
		BufferedReader errorReader;
		Boolean b = false;
		int i = 0;
		
		while((!b)&&(i<MaxNumberOfTries)) {
			
			if(local)
				pb = new ProcessBuilder("mkdir", "-p", dir);
			else pb = new ProcessBuilder("ssh", machine, "mkdir", "-p", dir);
			
			try {
							
	           process = pb.start();
	           
	           errorReader =
	                    new BufferedReader(new InputStreamReader(process.getErrorStream()));
	           
	          	           
	           b = (errorReader.readLine()==null);
	           
	           i++;
	        
	        process.waitFor();
	
	        } catch (IOException e) {
	            e.printStackTrace();
	        } catch (InterruptedException e) {
	            e.printStackTrace();
	        }
		}
	}
	
	/**
	 * This method executes the map phase
	 * @param dir Name of the directory to create in order to store the produced map
	 * @param file Name of the split to work with
	 */
	public void map(String dir, String file) {
		
		this.makeDir("", dir, true, 3);
		
		String fileName = file.substring(file.lastIndexOf("/")+1);
		int fileNumber = Integer.parseInt(fileName.substring(1, fileName.lastIndexOf("."))); 
		
		FileReader fr = null;
		
		try {  
			fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			Scanner sc= new Scanner(br);
			PrintWriter pw = new PrintWriter(dir + "/UM" + Integer.toString(fileNumber) + ".txt");
			while(sc.hasNext()) {
				pw.println(sc.next() + " 1");
			}
            pw.close();
            sc.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
			try { fr.close();} catch(Exception e) {}
		}
			
	}		
	
	/**
	 * This method executes the shuffle phase
	 * @param dir Name of the directory to create in order to store the produced shuffles
	 * @param file Name of the map to work with
	 */
	public void shuffle(String dir, String file) {
		
		this.makeDir("", dir + "/shuffle", true, 3);
			
		PrintWriter pw = null;
		int hash = 0;
		String word = null;
		int lines = this.countLines(dir + "/machines.txt");
		String machine = null;
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		String hostname = null;
		
		try {
			hostname = java.net.InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		
		try {
			Scanner sc= new Scanner(new BufferedReader(new FileReader(file)));
			while(sc.hasNextLine()) {
				word = sc.next();
				sc.nextLine();
				if (map.containsKey(word))
					map.put(word, map.get(word)+1);
				else map.put(word, 1);
			}
			sc.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
		
		Iterator<Map.Entry<String, Integer>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, Integer> pair = (Map.Entry<String, Integer>)it.next();
		    hash = pair.getKey().hashCode();
		    try {
		    	pw = new PrintWriter(new FileOutputStream(dir + "/shuffle/" + Integer.toString(hash) + "-" + hostname + ".txt", true));
		        for(int i = 0; i<pair.getValue(); i++)
		        	pw.println(pair.getKey() + " 1");
		        pw.close();
		    } catch(IOException e) {
		       	e.printStackTrace();
		    } 
			machine = this.getMachine(dir, Math.abs(hash) % lines);
			this.makeDir(machine, dir + "/shufflesreceived", false, 3);
			this.copyFile(machine, dir + "/shuffle/",  dir + "/shufflesreceived/", Integer.toString(hash) + "-" + hostname + ".txt", 3);
			it.remove(); 
	    }	
	}
	
	/**
	 * This method counts lines in a given file.
	 * @param file Name of the file
	 */
	public int countLines(String file) {
		
		FileReader fr = null;
		Scanner sc = null;
		int lines = 0;
		
		try {
			fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			sc= new Scanner(br);
			while(sc.hasNextLine()) {
				sc.nextLine();
				lines++;
			}
		} catch(Exception e) {
		e.printStackTrace();
		} finally {
			try { fr.close(); sc.close();} catch(Exception e) {}
		}
		
		return lines;
		
	}
	
	/**
	 * This method gets a machine from a certain position in a file
	 * @param dir Name of the directory where the file is
	 * @param position Line in which the machine is
	 */
	public String getMachine(String dir, int position) {
		
		FileReader fr = null;
		Scanner sc = null;
		int lines = 0;
		String machine = null;
		
		try {
			fr = new FileReader(dir + "/machines.txt");
			BufferedReader br = new BufferedReader(fr);
			sc= new Scanner(br);
			while(lines<position) {
				sc.nextLine();
				lines++;
			}
			machine = sc.next();
		} catch(Exception e) {
		e.printStackTrace();
		} finally {
			try { fr.close(); sc.close();} catch(Exception e) {}
		}
		
		return machine;
				
	}
	
	/**
	 * This method copies a file into a distant one
	 * @param machine Machine in which it has to create the directory.
	 * @param path Path of the file in the local machine
	 * @param destDir Name of the remote directory
	 * @param file Name of the file in the local machine
	 * @param MaxNumberOfTries Maximum number of times we may try to establish an ssh connection
	 */
	public void copyFile(String machine, String path, String destDir, String file, int MaxNumberOfTries) {
		
		ProcessBuilder pb;
		Process process;
		BufferedReader errorReader;
		Boolean b = false;
		int i = 0;
		
		while((!b)&&(i<MaxNumberOfTries)) {
			
			pb = new ProcessBuilder("scp", path + file , machine + ":" + destDir + file);
			
			try {
							
	           process = pb.start();
	           
	           errorReader =
	                    new BufferedReader(new InputStreamReader(process.getErrorStream()));
	           
	          	           
	           b = (errorReader.readLine()==null);
	           
	           i++;
	        
	        process.waitFor();
	
	        } catch (IOException e) {
	            e.printStackTrace();
	        } catch (InterruptedException e) {
	            e.printStackTrace();
	        }
		}
	}
	
	/**
	 * This method executes the reduce phase
	 * @param dir Name of the directory to create in order to store the produced reduce files
	 */
	public void reduce(String dir) {
		
		this.makeDir("", dir + "/reduce", true, 3);
		
		HashMap<String, Integer> words = new HashMap<String, Integer> ();
		Scanner sc = null;
		String name = null;
		String fileName = null;
		
		File folder = new File(dir + "/shufflesreceived/");
		File[] listOfFiles = folder.listFiles();
		
		for(int i = 0; i<listOfFiles.length; i++) {
			
			fileName = listOfFiles[i].getName();
						
			try {
				sc= new Scanner(new BufferedReader(new FileReader(dir + "/shufflesreceived/" + fileName)));
				name = sc.next();
			} catch(Exception e) {
				e.printStackTrace();
			} finally {
				try {sc.close();} catch(Exception e) {}
			}
			
			if(!words.containsKey(name))
				words.put(name, this.countLines(dir + "/shufflesreceived/" + fileName));
			else words.put(name, words.get(name)+this.countLines(dir + "/shufflesreceived/" + fileName));
		}
		
		Iterator<Map.Entry<String, Integer>> it = words.entrySet().iterator();
		while (it.hasNext()) {
	        Map.Entry<String, Integer> pair = (Map.Entry<String, Integer>)it.next();
	        try {
	        	FileOutputStream fos = new FileOutputStream(dir + "/reduce/" + Integer.toString(pair.getKey().hashCode()) + ".txt", true);
	        	PrintWriter pw = new PrintWriter(fos);
	        	pw.print(pair.getKey() + " " + pair.getValue() + "\r\n");
	        	pw.close();
	        	fos.close();
	        } catch(IOException e) {
	        	e.printStackTrace();
	        } 
	        it.remove(); 
	    }
			
	}
	
	/**
	 * Main method. Executes a phase of the MapReduce method
	 * @param args[0] Number that indicates which phase to execute
	 * @param args[1] Number of file to work with (for map and shuffle). Name of the working directory (reduce)
 	 * @param args[2] Name of the working directory (map and shuffle)
	 */
    public static void main(String args[]){
    	
    	int mode = 1;
    	
    	Slave slave = new Slave();
    	
    	try {
    		mode = Integer.parseInt(args[0]);
    	} catch (NumberFormatException e){
    		e.printStackTrace();
    	}
    	
    	if(mode == 0) {
    		slave.map(args[2] + "/maps", args[1]);
    	} else if(mode == 1){
    		slave.shuffle(args[2], args[1]);
    	} else if(mode == 2) {
    		slave.reduce(args[1]);
    	}
    	    	
    }
}
