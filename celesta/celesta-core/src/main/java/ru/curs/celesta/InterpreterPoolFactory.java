package ru.curs.celesta;

public class InterpreterPoolFactory {

  private InterpreterPoolFactory() {
    throw new AssertionError();
  }

  public static PythonInterpreterPool create(InterpreterPoolConfiguration configuration)
      throws CelestaException {
    //В будущем будет использоваться для создания разных интерпретаторов
    return new PythonInterpreterPool(
        configuration.getCelesta(),
        configuration.getScore(),
        configuration.getJavaLibPath(),
        configuration.getScriptLibPath()
    );
  }

}
