import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Scanner;
import java.lang.ProcessBuilder;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.io.PrintWriter;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Executors;

/**

 * This class defines a Deploy program that distributes a Slave.jar file to a list of available machines.

 * @author: Nicolas Olloniego

 * @version: 26/04/2020

 */
public class Deploy {
	
	/**
     * Empty constructor
     */
	public Deploy() {};
	
	/**
	 * This method finds the list of machines that are on in Telecom Paris.
	 * @param file Name of the file in which to write the list of available machines.
	 * @param username Username for establishing the ssh connection
	 */
	public void findAllMachines(String file, String username) {
		
		ProcessBuilder pb = new ProcessBuilder("ssh", username + "@ssh.enst.fr", "tp_up");
				
		try {
			
	           Process process = pb.start();
	           
	           BufferedReader outputReader =
	                    new BufferedReader(new InputStreamReader(process.getInputStream()));
	           	           
	           String line;
	           
	           PrintWriter pw = new PrintWriter(file);
	           while ((line = outputReader.readLine()) != null) {
	        	   pw.println(username + "@" + line);
	        	}
	           pw.close();
	            
	        } catch (IOException e) {
	            e.printStackTrace();
	        } 
		

	}
	
	/**
	 * This method tests SSH connection with the machines of the file of available machines.
	 * @param machines List of machines that were tested positive.
	 * @param username Username for establishing the ssh connection
	 * @param file Name of the file that contains the list of available machines.
	 * @param maxThrads Maximum size of the ThreadPoolExecuor 
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
	 * Main method. Finds the list of machines that are on and tests ssh connection. Puts Slave.jar in all machines that were 
	 * tested positive.
	 * @param args[0] Name of the file that contains the list of available machines
 	 * @param args[1] SSH username
 	 * @param args[2] Working directory
	 */
	public static void main(String[] args) {
		
		ConcurrentLinkedQueue<String> machines = new ConcurrentLinkedQueue<String>();
		
		Deploy deploy = new Deploy();

		deploy.testSSH(machines, args[1], args[0], 60);
		
		deploy.putFile(machines, args[2] + "/", "Slave.jar", args[2] + "/", "Slave.jar", machines.size(), 70, 3);	

	} 
	
}
