package ru.curs.celesta.unit;

import org.python.core.PyFunction;
import org.python.core.PyStringMap;
import org.python.core.PyType;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by ioann on 15.09.2017.
 */
public class TestClass {

  public static Map<PyType, Set<String>> testTypesAndTheirMethods = new LinkedHashMap<>();

  public TestClass(PyType pyType) {
    PyStringMap dict = (PyStringMap)pyType.fastGetDict();

    Set<String> testMethods = Arrays.stream(dict.keys().getArray())
        .filter(k -> String.valueOf(k).startsWith("test") && dict.get(k) instanceof PyFunction)
        .map(String::valueOf)
        .collect(Collectors.toSet());

      testTypesAndTheirMethods.put(pyType, testMethods);
  }
}
