package com.IDS594.PageRank;


public class Test {

	public static void main(String[] args) {
		String a = "node1, 0.3333333333333333	node2,node3";
		String line = a.toString();
		String[] content = line.split("	");
		if(content.length > 1){
			String[] outlinks = content[1].split(",");
			String[] nodeConsidered = content[0].split(",");
			int Nn = outlinks.length;
			//double initRank = Double.parseDouble(nodeConsidered[1]);
			double Ninit = (double)Nn/Double.parseDouble(nodeConsidered[1]);
			//for each outlink pagek
			for(int index = 0; index < Nn; index++){
				String rhs = nodeConsidered[0]+","+ Double.toString(Ninit);
				String lhs = outlinks[index];
				System.out.println(rhs);;
				System.out.println(lhs);
			}
		}
	}

}
