import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**

 * This class implements the Runnable interface. It retrieves the reduce files from a given machine

 * @author: Nicolas Olloniego

 * @version: 26/04/2020

 */
public class FileRetreiver implements Runnable{
	
	private String machine;
	private String sourceDir;
	private String destFolder;
	private int MaxNumberOfTries;

	/**
     * Class constructor
     * @param machine Remote machine to connect to
     * @param sourceDir Name of the remote directory were the results are
     * @param destFolder Name of the local directory in which results will be stored
     * @param MaxNumberOfTries Maximum number of times we may try to establish an ssh connection
     */
	public FileRetreiver(String machine, String sourceDir, String destFolder, int MaxNumberOfTries) {
		this.machine = machine;
		this.sourceDir = sourceDir;
		this.destFolder = destFolder;
		this.MaxNumberOfTries = MaxNumberOfTries;
	}
		
	public void run() {
		
			ProcessBuilder pb;
			Process process;
			BufferedReader errorReader;
			Boolean b = false;
			int i = 0;
			
			while((!b)&&(i<this.MaxNumberOfTries)) {
				
				pb = new ProcessBuilder("scp", "-q", "-r", this.machine + ":" + this.sourceDir + "/*",  this.destFolder );
				
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
