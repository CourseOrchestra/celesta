package ru.curs.celesta.dbutils;

import ru.curs.celesta.CallContext;
import ru.curs.celesta.score.GrainElement;

public interface IPermissionManager {
    /**
     * Role name that has rights to edit all tables.
     */
    String EDITOR = "editor";
    /**
     * Role name that has rights to read from all tables.
     */
    String READER = "reader";

    /**
     * Whether the action is allowed.
     *
     * @param c  call context
     * @param t  table
     * @param a  action type
     * @return
     */
    boolean isActionAllowed(CallContext c, GrainElement t, Action a);
}
