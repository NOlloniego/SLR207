import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**

 * This class implements the Runnable interface. It copies a given file to a remote machine.

 * @author: Nicolas Olloniego

 * @version: 26/04/2020

 */
public class FileCopier implements Runnable{
	
	private String machine;
	private String path;
	private String file;
	private String destDir;
	private String destFile;
	private int MaxNumberOfTries;

	/**
     * Class constructor
     * @param machine Remote machine to connect to
     * @param path Path of the file to copy
     * @param destDir Name of the directory in which the file will be put
     * @param file Name of the file to copy
     * @param destFile Name of the file in the remote machine
     * @param MaxNumberOfTries Maximum number of times we may try to establish an ssh connection
     */
	public FileCopier(String machine, String path, String destDir, String file, String destFile, int MaxNumberOfTries) {
		this.machine = machine;
		this.path = path;
		this.file = file;
		this.destFile = destFile;
		this.MaxNumberOfTries = MaxNumberOfTries;
		this.destDir = destDir;
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
	 * This method copies a file into a distant one
	 * @param machine Machine in which it has to create the directory.
	 * @param path Path of the file in the local machine
	 * @param file Name of the file in the local machine
	 * @param destDir Name of the directory in which the file will be put
	 * @param destFile Name of the file in the remote machine
	 * @param MaxNumberOfTries Maximum number of times we may try to establish an ssh connection
	 */	public void copyFile(String machine, String path, String file, String destDir, String destFile, int MaxNumberOfTries) {
			
			ProcessBuilder pb;
			Process process;
			BufferedReader errorReader;
			Boolean b = false;
			int i = 0;
			
			while((!b)&&(i<MaxNumberOfTries)) {
				
				pb = new ProcessBuilder("scp", "-q", path + file , machine + ":" + destDir + "/" + destFile);
				
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
	  * Run method. Copies a given file to a remote machine.
	  */
	public void run() {
		this.makeDir(this.machine, destDir, false, this.MaxNumberOfTries);
		this.copyFile(this.machine, this.path, this.file, this.destDir, this.destFile, this.MaxNumberOfTries);
	}
	
}

