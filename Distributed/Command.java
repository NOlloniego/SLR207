import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**

 * This class implements the Runnable interface. It gets the hostname of a remote machine.

 * @author: Nicolas Olloniego

 * @version: 26/04/2020

 */
public class Command implements Runnable{
	
	private String machine;
	private ConcurrentLinkedQueue<String> machines;
	private String username;

	/**
     * Class constructor
     * @param machine Remote machine to connect to
     * @param machines List of machines to add the machine if connection is successful
     * @param username SSH username
     */
	public Command(ConcurrentLinkedQueue<String> machines, String machine, String username) {
		this.machine = machine;
		this.machines = machines;
		this.username = username;
	}
	
	/**
     * Run method. Gets hostname of remote machine.
     */
	public void run() {
		
		ProcessBuilder pb = new ProcessBuilder("ssh", this.machine, "hostname");
		
		try {
						
            Process process = pb.start();

            BufferedReader outputReader =
                    new BufferedReader(new InputStreamReader(process.getInputStream()));
                        
            boolean b = process.waitFor(5, TimeUnit.SECONDS);
            
            String line = outputReader.readLine();

            if (this.machine.equals(username + "@"+line))
            	b = true;          
            else 
            	b = false;
            System.out.println(this.machine + ": " + b);
            
            if (b)
            	machines.add(this.machine);


        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
	}
	
}
