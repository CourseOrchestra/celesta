package ru.curs.celesta;

import java.io.IOException;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import ru.curs.celesta.Celesta;
import ru.curs.celesta.CelestaException;

public class InitTest {

	@BeforeClass
	public static void init() throws IOException, CelestaException {
		Properties params = new Properties();
		params.load(InitTest.class.getResourceAsStream("test.properties"));
		Celesta.initialize(params);
	}

	@Test
	public void test1() throws CelestaException {
		Celesta.getInstance();
	}

}
