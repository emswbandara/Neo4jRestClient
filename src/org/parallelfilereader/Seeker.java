package org.parallelfilereader;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class Seeker {
	private RandomAccessFile file;
	private String fileName;
	private long size;
	private String out;
	private long pos;
	private long end;
	
	public Seeker(String fileName) throws FileNotFoundException {
		this.fileName = fileName;
		file = new RandomAccessFile(fileName, "r");
	}
	
	public long seek(long pos, long size) {
		long result = -1l;

		try {
			long startPos, tmpStartPos, tmpEndPos, endPos;
			byte[] in;
			
			//Setting the starting possition
			
			if (pos == 0) {			//Beginning of the file, so nothing to check
				startPos = pos;
			} else {				//Else we reade byte by byte until we find '\n'
				tmpStartPos = pos;
				byte d = file.readByte();
				tmpStartPos++;
				
				if (d == '\n') {
					startPos = tmpStartPos;
				} else {
					while (d != '\n') {
						d = file.readByte();
						tmpStartPos++;
					}
					tmpStartPos--;
					startPos = tmpStartPos;			//set the starting position
				}
			}
			
			endPos = pos + size;		//initialize end position
			tmpEndPos = endPos;
			file.seek(endPos);
			
			if (endPos != end) {		//check end position with the index of the last byte of the file
				byte c = file.readByte();
				tmpEndPos++;
				//read byte by byte until we find '\n'
				if (c == '\n') {
					endPos = tmpEndPos;
				} else {
					while(c != '\n') {
						//System.out.println("StarPos : " + pos + " In endPos while : "+tmpEndPos);
						c = file.readByte();
						tmpEndPos++;
					}
					tmpEndPos--;
					endPos = tmpEndPos;
				}
			}

			result = endPos;

			//Set the seek to start position
			//file.seek(startPos);
			//byte array size
			//size = endPos - startPos + 1;
			//in = new byte[(int) size];
			//read file into a byte array
			//file.read(in);

			//out = new String(in);
			//System.out.println("Starting at " + pos + "\n" + out);
		} catch (EOFException e) {
			System.out.println("end of file reached.");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return result;
	}
	
	public String getOut(){
		return out;
	}
}
