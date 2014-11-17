package com.IDS594.PageRank;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Scanner;

public class GenerateGraph {
/**
 * This method constructs a graph and stores in input folder ready for map reduce
 * Sample Output:
	node1, 0.3333333333333333	node2,node3
	node2, 0.3333333333333333	node1
	node3, 0.3333333333333333	
 * @param args
 */
	public static void main(String[] args) {
		Scanner scan = new Scanner(System.in);
		Scanner scan2 = new Scanner(System.in);
		System.out.println("Enter Total Number of Nodes: ");
		int nodeCount = scan.nextInt();
		String fileName = "input//graphInput.txt";
		File file = new File(fileName);
		double initialRank =1/(double)nodeCount;
		try {
			FileOutputStream fop = new FileOutputStream(file);
			if (!file.exists()) {
				file.createNewFile();
			}
			for(int node = 1; node <= nodeCount; node++){
				System.out.println("Enter Outlinks for node"+ node +" separated by comma(,). Hit 'enter' if there are no outlinks");
				String outlinks = scan2.nextLine();
				String nodewithInitialRank = "node"+Integer.toString(node)+", "+Double.toString(initialRank);
				String nodewithOutlinks = outlinks;
				String totalNodeContent = nodewithInitialRank + "	" +nodewithOutlinks+"\n";
				byte[] contentInBytes = totalNodeContent.toString().getBytes();
				fop.write(contentInBytes);
			}
			fop.close();
			scan.close();
			scan2.close();
			System.out.println("Input Graph for map reduce costructed. It is available in input folder");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
