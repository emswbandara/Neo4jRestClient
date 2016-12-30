package org.parallelfilereader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.LinkedList;

public class Main {

	public static String fileName = "roadNet-PA.txt";
	public static long startTime;

	public static void main(String[] args) {
		RandomAccessFile file = null;
		int NUM_PARALLEL_THREADS = 4;
		NUM_PARALLEL_THREADS++;
		Thread[] threadArray = new Thread[NUM_PARALLEL_THREADS - 1];
 		long size = 0;
 		long partitionSize = 0;
		LinkedList<Long> listLocations = new LinkedList<Long>();


		//First we mark at which locations we should read the data

		try {
			file = new RandomAccessFile(fileName, "r");
			size = file.length();
			partitionSize = 10000;  // NUM_PARALLEL_THREADS;
			System.out.println("File size : " + size);
			System.out.println("Partition Size : " + partitionSize);
			Seeker seeker = new Seeker(fileName);

			long startPos = 0l;
			long endPos = seeker.seek(startPos, partitionSize);
			long readSize = endPos-startPos;
			//System.out.println("start:" + startPos + " end:" + endPos + " size:" + readSize);
			listLocations.add(endPos);
			
			while(endPos != -1l) {
				startPos = endPos + 1;
				endPos = seeker.seek(startPos, partitionSize);
				if(endPos != -1) {
					readSize = endPos-startPos;
					//System.out.println("start:" + startPos + " end:" + endPos + " size:" + readSize);
					listLocations.add(endPos);
				} else{
					endPos = size;
					readSize = endPos-startPos;
					listLocations.add(endPos);
					//System.out.println("start:" + startPos + " end:" + endPos + " size:" + readSize);
					endPos = -1l;
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		//Second we read the data using N number of parallel threads using the list of marked indices.

		//The assumption is that we will read exactly N number of partitions using N threads so that each thread
		//handles one partition.
		long startPos = 0l;
		long endPos = -1l;
		int k = 0;
        	 startTime = System.currentTimeMillis();

		for (int i = 0; i < threadArray.length; i++) {
			ArrayList<Long> batchOfPositions = new ArrayList();
			batchOfPositions.add(startPos);
			
			for (int j = 0; j < listLocations.size() / NUM_PARALLEL_THREADS; j++) {
				endPos = listLocations.get(k);
				batchOfPositions.add(endPos);
				k++;
			}
			startPos = endPos + 1;
			
			try {
				//System.out.println("Thread " + i + " with " + batchOfPositions.size() + " of sets");
				threadArray[i] = new Thread(new Reader(fileName,batchOfPositions));
				threadArray[i].start();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		
		/*
		for(int i=0; i < listLocations.size(); i++) {
			endPos = listLocations.get(i);
			try {
				threadArray[i] = new Thread(new Reader(startPos, endPos));
				System.out.println("Thread "+i+"start"+startPos + " end:" + endPos);
				threadArray[i].start();
			} catch (IOException e) {
				e.printStackTrace();
			}

			startPos = endPos + 1;
		}

		try {
			Thread.currentThread().sleep(100000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}*/
	}

	public static void executionTime(){

		long stopTime = System.currentTimeMillis();
            	long elapsedTime = (stopTime - startTime)/1000;
            	System.out.println("time taken : "+elapsedTime + " seconds.");
	}

}
