import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Scanner;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**

 * This class defines a Master that counts the occurrencies of words in a given text file by running a Slave
 * program over distant machines. 

 * @author: Nicolas Olloniego

 * @version: 26/04/2020

 */
public class Master {
	
	 /**
     * Empty constructor
     */
	public Master() {}
	
	/**
	 * This method tests SSH connection with the machines of the file of available machines.
	 * @param machines List of machines that were tested positive.
	 * @param username Username for establishing tKeyhe ssh connection
	 * @param file Name of the file that contains the list of available machines.
	 * @param maxThreads Maximum size of the ThreadPoolExecuor 
	 */
	public void testSSH(ConcurrentLinkedQueue<String> machines, String username, String file, int maxThreads) { 
			
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxThreads);	
		FileReader fr = null;
		Scanner sc = null;
		
		try {
			fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			sc= new Scanner(br);
			while(sc.hasNext()) {
				try {
					Command cmd = new Command(machines, sc.next(), username);
					executor.execute(cmd);
				} catch (Exception e) {
					e.printStackTrace();
				}	
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try { fr.close(); sc.close();} catch(Exception e) {}
		}
			
		executor.shutdown();
		
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
			while(!executor.isTerminated());	
		
	}
	
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
			else pb = new ProcessBuilder("ssh", "-q", machine, "mkdir", "-p", dir);
			
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
	 * This method puts a file in a distant machine. If the directory does not exist, it creates it.
	 * @param machines List of machines
	 * @param path Local path of the file
	 * @param file Name of the file
	 * @param destDir Name of the directory in which it has to copy the file
	 * @param destFile Name of the file in the distant machine
	 * @param amount Number of machines to which it has to copy the file
	 * @param max_threads Maximum size of the ThreadPoolExecuor
	 * @param MaxNumberOfTries Maximum number of times we may try to establish an ssh connection
	 */
	public void putFile(ConcurrentLinkedQueue<String> machines, String path, String file, String destDir, String destFile, int amount, int max_threads, int MaxNumberOfTries) {
		
		ConcurrentLinkedQueue<String> rewriteMachines = new ConcurrentLinkedQueue<String>(machines);
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(max_threads);
		int initialSize = rewriteMachines.size();
				
		while(((initialSize-rewriteMachines.size()) < amount)&&(rewriteMachines.size()>0)) {
				
			String machine = rewriteMachines.remove();
							
			try {
				FileCopier fc = new FileCopier(machine, path, destDir, file, destFile, MaxNumberOfTries);
				executor.execute(fc);
			} catch (Exception e) {
				e.printStackTrace();
			}	
			
		}
		executor.shutdown();
		
		try {
			  executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		while(!executor.isTerminated());	
				
	}
	 
	/**
	 * This method runs the map phase of the slave.
	 * @param machines List of machines
	 * @param destDir Name of the working directory
	 * @param amount Number of machines to which it has to copy the file
	 * @param maxThreads Maximum size of the ThreadPoolExecuor
	 * @param MaxNumberOfTries Maximum number of times we may try to establish an ssh connection
	 */
	public void runSlaveMap(ConcurrentLinkedQueue<String> machines, String destDir, int maxThreads, int amount, int MaxNumberOfTries) {
		
		ConcurrentLinkedQueue<String> rewriteMachines = new ConcurrentLinkedQueue<String>(machines);
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxThreads);
				
		for(int i = 0; i < amount; i++) {
			
			if(rewriteMachines.size() == 0) {
				rewriteMachines = new ConcurrentLinkedQueue<String>(machines);
				while(executor.getActiveCount()>0);
			}
			
			String machine = rewriteMachines.remove();
			
			try {
				SlaveRunner sr = new SlaveRunner(machine, destDir, i, MaxNumberOfTries);
				executor.execute(sr);
			} catch (Exception e) {
				e.printStackTrace();
			}	
			
		}
		executor.shutdown();
		try {
			  executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		while(!executor.isTerminated());	
		
		System.out.println("MAP FINISHED");

	}
	
	/**
	 * This method runs the shuffle phase of the slave.
	 * @param machines List of machines
	 * @param destDir Name of the working directory
	 * @param amount Number of machines to which it has to copy the file
	 * @param maxThreads Maximum size of the ThreadPoolExecuor
	 * @param MaxNumberOfTries Maximum number of times we may try to establish an ssh connection
	 */
	public void runSlaveShuffle(ConcurrentLinkedQueue<String> machines, String destDir, int maxThreads, int amount, int MaxNumberOfTries) {
		
		ConcurrentLinkedQueue<String> rewriteMachines = new ConcurrentLinkedQueue<String>(machines);
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxThreads);
				
		for(int i = 0; i < amount; i++) {
			
			if(rewriteMachines.size() == 0) {
				rewriteMachines = new ConcurrentLinkedQueue<String>(machines);
				while(executor.getActiveCount()>0);
			}
			
			String machine = rewriteMachines.remove();
			
			try {
				SlaveShuffle sf= new SlaveShuffle(machine, destDir, i, MaxNumberOfTries);
				executor.execute(sf);
			} catch (Exception e) {
				e.printStackTrace();
			}	
			
		}
		executor.shutdown();
		try {
			  executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		while(!executor.isTerminated());	
		
		System.out.println("SHUFFLE FINISHED");

	}

	/**
	 * This method runs the reduce phase of the slave.
	 * @param machines List of machines
	 * @param destDir Name of the working directory 
	 * @param maxThreads Maximum size of the ThreadPoolExecuor
	 * @param MaxNumberOfTries Maximum number of times we may try to establish an ssh connection
	 */
	public void runSlaveReduce(ConcurrentLinkedQueue<String> machines, String destDir, int maxThreads, int MaxNumberOfTries) {
		
		ConcurrentLinkedQueue<String> rewriteMachines = new ConcurrentLinkedQueue<String>(machines);
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxThreads);
				
		while(rewriteMachines.size()>0) {
						
			String machine = rewriteMachines.remove();
			
			try {
				SlaveReduce sr= new SlaveReduce(machine, destDir, MaxNumberOfTries);
				executor.execute(sr);
			} catch (Exception e) {
				e.printStackTrace();
			}	
			
		}
		executor.shutdown();
		try {
			  executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		while(!executor.isTerminated());	
		
		System.out.println("REDUCE FINISHED");

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
	 * This method generates approximatively numberOfSplits splits with the same amount of lines.
	 * @param inputFile Name of the file to divide
	 * @param destDir Name of the directory in which the splits will be placed
	 * @param numberOfSplits Amount of splits to generate -1
	 */
	public int generateSplits(String inputFile, String destDir, int numberOfSplits) {
		
		this.makeDir("", destDir, true, 1);

		int totalLines = countLines(inputFile);
		int splitSize = (int)Math.ceil(totalLines/numberOfSplits);
			
		int sufixLength = String.valueOf(numberOfSplits).length();
			
		try {
			Process process = Runtime.getRuntime().exec(new String[]{"bash","-c","split " + inputFile + " -d --lines=" + splitSize + " -a " + sufixLength + " " + destDir + "/S"});
	                    				            
			process.waitFor();

		} catch (IOException e) {
	    	e.printStackTrace();
	    } catch (InterruptedException e) {
	    	e.printStackTrace();
	    }
		return sufixLength;
		
	}
	
	/**
	 * This method distributes created splits in distant machines indicated in the queue. It writes in a file the 
	 * names of the machines that are being used.
	 * @param machines List of machines
	 * @param file Name of the file in which it has to write the list of used machines
	 * @param sourceDir Name of the directory in which the splits were be placed
 	 * @param maxThreads Maximum size of the ThreadPoolExecuor
 	 * @param suffixLength Number of digits each split has
	 * @param MaxNumberOfTries Maximum number of times we may try to establish an ssh connection	 
	 * @return Amount of splits
	 */
	public int distributeSplits(ConcurrentLinkedQueue<String> machines, String file, String sourceDir, int maxThreads, int sufixLength, int MaxNumberOfTries) {
		
		File folder = new File(sourceDir);
		File[] listOfFiles = folder.listFiles();
		
		ConcurrentLinkedQueue<String> rewriteMachines = new ConcurrentLinkedQueue<String>(machines);
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxThreads);
		PrintWriter pw = null;
		boolean written = false;
		String zeros = "";
		int iLength = 0;
		
		try {
			pw = new PrintWriter(file);
			for(int i = 0; i < listOfFiles.length; i++) {
				
				if(rewriteMachines.size() == 0) {
					rewriteMachines = new ConcurrentLinkedQueue<String>(machines);
					written = true;
				}
				
				String machine = rewriteMachines.remove();
								
				if(!written)
					pw.println(machine);

				try {
					iLength = String.valueOf(i).length();
					if((sufixLength > 1)&&(iLength < sufixLength)) {
						for (int j = 0; j <  sufixLength-iLength; j++)
							zeros = zeros.concat("0");	
					}
					
					FileCopier fc = new FileCopier(machine, sourceDir + "/", sourceDir +"/", "S" + zeros + Integer.toString(i),  "S" + Integer.toString(i) + ".txt", MaxNumberOfTries);
					executor.execute(fc);
				} catch (Exception e) {
					e.printStackTrace();
				}	
				zeros = "";
			}
		} catch(IOException e) {
			e.printStackTrace();
		} finally {pw.close();}
		
		executor.shutdown();
		
		try {
			  executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		while(!executor.isTerminated());	
				
		return listOfFiles.length;
	}
	
	/**
	 * This method gets the reduce results from distant machines.
	 * @param machines List of machines
	 * @param sourceDir Name of the directory in which the results were be placed
	 * @param destDir Name of the directory in which the results will be placed
 	 * @param maxThreads Maximum size of the ThreadPoolExecuor
	 * @param MaxNumberOfTries Maximum number of times we may try to establish an ssh connection	 
	 */
	public void retreiveResults(ConcurrentLinkedQueue<String> machines, String sourceDir, String destDir, int maxThreads, int MaxNumberOfTries) {
		
		ConcurrentLinkedQueue<String> rewriteMachines = new ConcurrentLinkedQueue<String>(machines);
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxThreads);
		
		this.makeDir("", destDir, true, MaxNumberOfTries);
				
		while(rewriteMachines.size() > 0) {
						
			String machine = rewriteMachines.remove();
				
			try {
				FileRetreiver fc = new FileRetreiver(machine, sourceDir, destDir, MaxNumberOfTries);
				executor.execute(fc);
			} catch (Exception e) {
				e.printStackTrace();
			}	
			
		}
		executor.shutdown();
		
		try {
			  executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		while(!executor.isTerminated());	
				
	}
	
	/**
	 * This method concatenates the retrieved files and orders the results.
	 * @param dir Name of the directory in which the results were be placed
	 * @param destDir Name of the directory in which the result will be placed
 	 * @param destFile Name of the file that will contain the results
	 */
	public void outputResults(String dir, String destDir, String destFile) {
		
		try {
			Process process = Runtime.getRuntime().exec(new String[]{"bash","-c","cat " + dir + "*.txt " + ">" + destDir + destFile});
		                       			
		    process.waitFor();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
				
		try {
			Process process2 = Runtime.getRuntime().exec(new String[]{"bash","-c","sort -k 2nr -o " + destDir + destFile + " " + destDir + destFile});
		                                  
			process2.waitFor();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
		
	}
	
	/**
	 * Main method. Tests ssh connection, generates and distributes splits. Runs all MapReduce phases in distant machines.
	 * Retrieves and outputs results.
	 * @param args[0] Name of the file that contains the list of available machines
	 * @param args[1] Name of the file to work with
 	 * @param args[2] Name of the working directory
 	 * @param args[3] SSH username
 	 * @param args[4] Amount of splits to generate -1
	 */
	public static void main(String[] args) {
		
		long startTime, endTime, totalTime;
		int amount = 0;
		
		ConcurrentLinkedQueue<String> machines = new ConcurrentLinkedQueue<String>();
		
		Master master = new Master();
		
		master.testSSH(machines, args[3], args[0], 60);
		
		int sufix = master.generateSplits(args[1], args[2] + "/splits", Integer.parseInt(args[4]));
		
		System.out.println("Splits generated");
		
		amount = master.distributeSplits(machines, args[2] + "/machines.txt", args[2] + "/splits", 70, sufix, 10);
		
		System.out.println("Splits distributed");
		
		master.putFile(machines, args[2] + "/", "machines.txt", args[2] + "/", "machines.txt", amount, 70, 10);
		
		startTime = System.currentTimeMillis();
		
		master.runSlaveMap(machines, args[2], 70, amount, 10);
		
		endTime = System.currentTimeMillis();
		totalTime = endTime - startTime;
		System.out.println("Map time is " + totalTime + " milliseconds");
		
		startTime = System.currentTimeMillis();
		
		master.runSlaveShuffle(machines, args[2], 70, amount, 10);
		
		endTime = System.currentTimeMillis();
		
		totalTime = endTime - startTime;
		System.out.println("Shuffle time is " + totalTime + " milliseconds");
		
		startTime = System.currentTimeMillis();
		
		master.runSlaveReduce(machines, args[2], 70, 10);
		
		endTime = System.currentTimeMillis();
		totalTime = endTime - startTime;
		System.out.println("Reduce time is " + totalTime + " milliseconds");
		
		master.retreiveResults(machines, args[2] + "/reduce",args[2] + "/retreivedResults/", 70, 10);
		
		System.out.println("Results retreived");
		
		master.outputResults(args[2] + "/retreivedResults/", args[2] + "/", "results.txt");
		System.out.println("Results ready");
		
	}
	
}
