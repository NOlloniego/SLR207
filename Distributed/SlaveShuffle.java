import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**

 * This class implements the Runnable interface. It runs the Shuffle phase of the Slave.jar.

 * @author: Nicolas Olloniego

 * @version: 26/04/2020

 */
public class SlaveShuffle implements Runnable{
	
	private String machine;
	private String destDir;
	private int um;
	private int MaxNumberOfTries;
	
	/**
     * Class constructor
     * @param machine Remote machine to connect to
     * @param destDir Name of the working directory in remote machine, passed as an argument to the Slave
     * @param um Number of map to work with, passed as an argument to the Slave
     * @param MaxNumberOfTries Maximum number of times we may try to establish an ssh connection
     */
	public SlaveShuffle(String machine, String destDir, int um, int MaxNumberOfTries) {
		this.machine = machine;
		this.destDir = destDir;
		this.um = um;
		this.MaxNumberOfTries = MaxNumberOfTries;
	}
	
	/**
	 * Run method. Runs the Shuffle phase of the Slave.jar.
	 */
	public void run() {
			
		ProcessBuilder pb;
		Process process;
		BufferedReader errorReader;
		Boolean b = false;
		int i = 0;
		
		while((!b)&&(i<this.MaxNumberOfTries)) {
			
			pb = new ProcessBuilder("ssh", "-q", this.machine, "java", "-jar", this.destDir + "/Slave.jar", "1", this.destDir + "/maps/UM"+Integer.toString(this.um)+".txt", this.destDir);
			
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