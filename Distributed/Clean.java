import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;

/**

 * This class defines a Clean program that erases created directories from the list of available machines.

 * @author: Nicolas Olloniego

 * @version: 26/04/2020

 */
public class Clean {
	
	/**
     * Empty constructor
     */
	public Clean() {}
	
	/**
	 * This method removes all remotely created directories.
	 * @param machines List of machines to clean
	 * @param dir Directory to erase
	 * @param maxThreads Maximum size of the ThreadPoolExecuor
	 * @param MaxNumberOfTries Maximum number of times we may try to establish an ssh connection
	 */
	public void clean(ConcurrentLinkedQueue<String> machines, String dir, int maxThreads, int MaxNumberOfTries) {
		
		ConcurrentLinkedQueue<String> rewriteMachines = machines;
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(maxThreads);
		
		while(rewriteMachines.size()!=0){
			String machine = rewriteMachines.remove();
			try {
				CleanRunnable cr = new CleanRunnable(machine, dir, MaxNumberOfTries);
				executor.execute(cr);
			} catch(Exception e) {
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
	 * This method removes a local directory.
	 * @param dir Directory to erase
	 */
	public void cleanLocalDirectory(String dir) {
		
		ProcessBuilder pb = new ProcessBuilder("rm", "-rf", dir + "/");
		
		try {
						
           Process process = pb.start();
           
           process.waitFor();

	    } catch (IOException e) {
	        e.printStackTrace();
	    } catch (InterruptedException e) {
	        e.printStackTrace();
	    }
	}
	
	/**
	 * Main method. Tests ssh connection. Erases all generated directories both locally and remotely. 
	 * @param args[0] Name of the file that contains the list of available machines
 	 * @param args[1] SSH username
 	 * @param args[2] Working directory
	 */
	public static void main(String[] args) {
		
		ConcurrentLinkedQueue<String> machines = new ConcurrentLinkedQueue<String>();
		
		Deploy deploy = new Deploy();
		
		deploy.findAllMachines(args[0], args[1]);
	
		deploy.testSSH(machines, args[1], args[0], 60);
		
		Clean cleanObjet = new Clean();
		
		cleanObjet.cleanLocalDirectory(args[2] + "/retreivedResults");
		
		cleanObjet.cleanLocalDirectory(args[2] + "/splits");
		
		cleanObjet.clean(machines, args[2] + "/", 70, 3);
		
	}
	
}
