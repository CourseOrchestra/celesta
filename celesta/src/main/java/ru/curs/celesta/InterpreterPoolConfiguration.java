package ru.curs.celesta;

import ru.curs.celesta.score.Score;

public class InterpreterPoolConfiguration {
  private Score score;
  private String javaLibPath;
  private String scriptLibPath;

  public Score getScore() {
    return score;
  }

  public void setScore(Score score) {
    this.score = score;
  }

  public String getJavaLibPath() {
    return javaLibPath;
  }

  public void setJavaLibPath(String javaLibPath) {
    this.javaLibPath = javaLibPath;
  }

  public String getScriptLibPath() {
    return scriptLibPath;
  }

  public void setScriptLibPath(String scriptLibPath) {
    this.scriptLibPath = scriptLibPath;
  }
}
