package ru.curs.celesta;

import java.io.IOException;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

public class InitTest {

	@BeforeClass
	public static void init() throws IOException, CelestaException {
		Properties params = new Properties();
		params.load(InitTest.class.getResourceAsStream("test.properties"));//celesta.oracle.properties
		Celesta.initialize(params);
	}

	@Test
	public void test1() throws CelestaException {
		Celesta.getInstance();
	}

}
