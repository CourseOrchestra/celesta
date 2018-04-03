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
     * @throws CelestaException В случае, если процедура не найдена или в случае ошибки
     *                          выполненения процедуры.
     */
    PyObject runPython(String sesId, String proc, Object... param) throws CelestaException;

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
     * @throws CelestaException В случае, если процедура не найдена или в случае ошибки
     *                          выполненения процедуры.
     */
    PyObject runPython(String sesId, CelestaMessage.MessageReceiver rec, ShowcaseContext sc, String proc,
                       Object... param) throws CelestaException;


    Future<PyObject> runPythonAsync(String sesId, String proc, long delay, Object... param);

}
