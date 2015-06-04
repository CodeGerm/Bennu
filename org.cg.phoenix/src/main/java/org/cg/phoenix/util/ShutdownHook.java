package org.cg.phoenix.util;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

public class ShutdownHook {

	public ShutdownHook ( ) {

		Thread t = new ShutdownHookThread ( "Here I am !!!" );

		Runtime.getRuntime().addShutdownHook (t);

		System.out.println ( "Now shut me down …" );

		while ( true ) {
			System.out.print ( ".");
			try {
				Thread.sleep ( 100 );
			} catch ( InterruptedException ie ) { }
		}

	}

	public static void main ( String args[] ) throws FileNotFoundException, UnsupportedEncodingException { 
		
		
		new ShutdownHook ();
	}

}

class ShutdownHookThread extends Thread {

	protected String message;

	public ShutdownHookThread ( String message ) {
		this.message = message;
	}

	public void run ( ) {
		PrintWriter writer;
		try {
			writer = new PrintWriter("the-file-name.txt", "UTF-8");	
			writer.println(message);
			writer.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}

} 