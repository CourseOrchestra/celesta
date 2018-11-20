package ru.curs.celesta.dbutils;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.score.GrainElement;

public interface IPermissionManager {
    /**
     * Имя роли, обладающей правами на редактирование всех таблиц.
     */
    String EDITOR = "editor";
    /**
     * Имя роли, обладающей правами на чтение всех таблиц.
     */
    String READER = "reader";


    /**
     * Разрешено ли действие.
     *
     * @param c
     *            контекст вызова.
     * @param t
     *            таблица.
     *
     * @param a
     *            тип действия
     */
    boolean isActionAllowed(CallContext c, GrainElement t, Action a) ;
}
