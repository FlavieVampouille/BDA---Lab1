package ecp.bigdata.Tutorial1;


import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class display {

	public static void main(String[] args) throws IOException {
		
		Path filename = new Path("/home/cloudera/Documents/BD_Algo/arbres.csv");
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);
		
		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			while (line !=null){
				String[] splitted = line.toString().split(";");
				// Process of the current line
				System.out.println("Annee : " + splitted[5]);
				System.out.println("Hauteur : " + splitted[6] + "\n");
				// go to the next line
				line = br.readLine();
			}
		}
		finally{
			//close the file
			inStream.close();
			fs.close();
		}
		
	}

}
