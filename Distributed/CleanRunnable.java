import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**

 * This class implements the Runnable interface. It removes a given directory from a remote machine.

 * @author: Nicolas Olloniego

 * @version: 26/04/2020

 */
public class CleanRunnable implements Runnable{
	
	private String machine;
	private int MaxNumberOfTries;
	private String dir;
	
	/**
     * Class constructor
     * @param machine Remote machine to connect to
     * @param dir Directory to erase
     * @param MaxNumberOfTries Maximum number of times we may try to establish an ssh connection
     */
	public CleanRunnable(String machine, String dir, int MaxNumberOfTries) {
		this.machine = machine;
		this.MaxNumberOfTries = MaxNumberOfTries;
		this.dir = dir;
	}
	
	/**
     * Run method. Deletes given directory from remote machine
     */
	public void run() {
		
		ProcessBuilder pb;
		Process process;
		BufferedReader errorReader;
		Boolean b = false;
		int i = 0;
		
		while((!b)&&(i<this.MaxNumberOfTries)) {
			
			pb = new ProcessBuilder("ssh", "-q",  this.machine, "rm", "-rf", this.dir + "/");
			
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
	
}