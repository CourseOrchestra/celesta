package ru.curs.celesta;

import ru.curs.celesta.score.Score;

public class InterpreterPoolConfiguration {
  private Celesta celesta;
  private Score score;
  private String javaLibPath;
  private String scriptLibPath;

  public Celesta getCelesta() {
    return celesta;
  }

  public InterpreterPoolConfiguration setCelesta(Celesta celesta) {
    this.celesta = celesta;
    return this;
  }

  public Score getScore() {
    return score;
  }

  public InterpreterPoolConfiguration setScore(Score score) {
    this.score = score;
    return this;
  }

  public String getJavaLibPath() {
    return javaLibPath;
  }

  public InterpreterPoolConfiguration setJavaLibPath(String javaLibPath) {
    this.javaLibPath = javaLibPath;
    return this;
  }

  public String getScriptLibPath() {
    return scriptLibPath;
  }

  public InterpreterPoolConfiguration setScriptLibPath(String scriptLibPath) {
    this.scriptLibPath = scriptLibPath;
    return this;
  }
}
