package ecp.bigdata.Tutorial1;


import java.io.*;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class content {

	public static void main(String[] args) throws IOException {
		
		Path filename = new Path("/home/cloudera/Documents/BD_Algo/isd-history.txt");
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);
		
		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line except the first 22 lines
			int count = 0;
			String line = br.readLine();
			while (line !=null){
				count++;
				// Process of the current line
				char[] Arr = line.toString().toCharArray();
				// go to the next line
				line = br.readLine();
				if (count > 22) {
					// print the USAF code, name, country FIP and elevation of each station
					System.out.println(" ");
					System.out.print("USAF code : ");
					for (int i = 0; i < 6 ; i++) {
						System.out.print(Arr[i]);
					}
					System.out.println(" ");
					System.out.print("Name : ");
					for (int i = 13; i < 43 ; i++) {
						System.out.print(Arr[i]);
					}
					System.out.println(" ");
					System.out.print("Country : ");
					for (int i = 43; i < 45 ; i++) {
						System.out.print(Arr[i]);
					}
					System.out.println(" ");
					System.out.print("Elevation : ");
					for (int i = 74; i < 81 ; i++) {
						System.out.print(Arr[i]);
					}
					System.out.println(" ");
				}
			}
		}
		finally{
			//close the file
			inStream.close();
			fs.close();
		}
		
	}

}

