package ru.curs.celesta;

import java.io.IOException;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import ru.curs.celesta.Celesta;
import ru.curs.celesta.CelestaCritical;

public class InitTest {

	@BeforeClass
	public static void init() throws IOException, CelestaCritical {
		Properties params = new Properties();
		params.load(InitTest.class.getResourceAsStream("test.properties"));
		Celesta.initialize(params);
	}

	@Test
	public void test1() throws CelestaCritical {
		Celesta.getInstance();
	}

}
