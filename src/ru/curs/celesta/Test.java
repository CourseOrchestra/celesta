package ru.curs.celesta;

public class Test {
	public static void main(String[] args) throws CelestaException {
		Celesta.getInstance().login("mysession", "user2");

		Celesta.getInstance().runPython("mysession", "g1.testses.proc1");
		// Celesta.getInstance().clearInterpretersPool();
		Celesta.getInstance().runPython("mysession", "g1.testses.proc2");
		Celesta.getInstance().clearInterpretersPool();
		Celesta.getInstance().runPython("mysession", "g1.testses.proc2");
		Celesta.getInstance().logout("mysession", false);
	}
}
