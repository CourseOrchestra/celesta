package ru.curs.celesta.unit;

import org.python.core.PyFunction;
import org.python.core.PyObject;

public abstract class CelestaTestCase {

  public void assertEquals(Object expected, Object actual) {
    org.junit.jupiter.api.Assertions.assertEquals(expected, actual);
  }

  public void assertNotEquals(Object expected, Object actual) {
    org.junit.jupiter.api.Assertions.assertNotEquals(expected, actual);
  }

  public void assertTrue(boolean condition) {
    org.junit.jupiter.api.Assertions.assertTrue(condition);
  }

  public void assertFalse(boolean condition) {
    org.junit.jupiter.api.Assertions.assertFalse(condition);
  }


  public <T extends Throwable> T assertThrows(Class<T> expectedType, PyFunction func, PyObject... args) {
    return org.junit.jupiter.api.Assertions.assertThrows(
        expectedType,
        () -> {
          try {
            func.__call__(args);
          } catch (Exception e) {
            if (expectedType.isInstance(e.getCause())) {
              throw e.getCause();
            }
            throw e;
          }
        }
    );
  }

}
