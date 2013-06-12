package ru.curs.celesta;

import java.io.InputStream;

public class CelestaMain {

	/**
	 * @param args
	 * @throws ParseException
	 */
	public static void main(String[] args) throws ParseException {
		InputStream input = CelestaMain.class.getResourceAsStream("test.txt");
		CelestaParser cp = new CelestaParser(input);
		GrainModel m = cp.model();
		System.out.println(m.toString());
	}
}
