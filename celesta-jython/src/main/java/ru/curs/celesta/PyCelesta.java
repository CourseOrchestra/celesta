package ru.curs.celesta;

import org.python.core.PyObject;

import java.util.concurrent.Future;

public interface PyCelesta extends ICelesta {

    /**
     * Запуск питоновской процедуры.
     *
     * @param sesId идентификатор пользователя, от имени которого производится
     *              изменение
     * @param proc  Имя процедуры в формате <grain>.<module>.<proc>
     * @param param Параметры для передачи процедуры.
     * @return PyObject
     */
    PyObject runPython(String sesId, String proc, Object... param);

    /**
     * Запуск питоновской процедуры.
     *
     * @param sesId идентификатор пользователя, от имени которого производится
     *              изменение
     * @param rec   приёмник сообщений.
     * @param sc    Контекст Showcase.
     * @param proc  Имя процедуры в формате <grain>.<module>.<proc>
     * @param param Параметры для передачи процедуры.
     * @return PyObject Результат вызова питон-процедуры.
     */
    PyObject runPython(String sesId, CelestaMessage.MessageReceiver rec, ShowcaseContext sc, String proc,
                       Object... param);


    Future<PyObject> runPythonAsync(String sesId, String proc, long delay, Object... param);

}
