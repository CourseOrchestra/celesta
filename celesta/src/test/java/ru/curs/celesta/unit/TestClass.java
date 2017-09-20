package ru.curs.celesta.unit;

import org.python.core.PyFunction;
import org.python.core.PyStringMap;
import org.python.core.PyType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by ioann on 15.09.2017.
 */
public class TestClass {

  public TestClass(PyType pyType) {
    PyStringMap dict = (PyStringMap)pyType.fastGetDict();

    List<String> testMethods = Arrays.stream(dict.keys().getArray())
        .filter(k -> String.valueOf(k).startsWith("test") && dict.get(k) instanceof PyFunction)
        .map(String::valueOf)
        .collect(Collectors.toList());

    CelestaScriptsTest.testTypesAndTheirMethods.put(pyType, testMethods);
  }
}
