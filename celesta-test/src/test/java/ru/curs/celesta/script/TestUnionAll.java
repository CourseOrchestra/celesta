package ru.curs.celesta.script;

import org.junit.jupiter.api.TestTemplate;
import ru.curs.celesta.CallContext;
import unionAll.MessageCursor;
import unionAll.RoleCursor;
import unionAll.RolePermissionsCursor;
import unionAll.SecurityCursor;
import unionAll.UserCursor;
import unionAll.UserRoleCursor;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestUnionAll implements ScriptTest {
    @TestTemplate
    public void testSecurityView(CallContext ctx) {
        UserCursor user = new UserCursor(ctx);
        user.setId(1);
        user.setName("John");
        user.insert();
        user.setId(2);
        user.setName("Mary");
        user.insert();

        RoleCursor role = new RoleCursor(ctx);
        role.setId(1);
        role.setName("admin");
        role.insert();

        //Assign super permission to admin role
        RolePermissionsCursor rp = new RolePermissionsCursor(ctx);
        rp.setRoleId(1);
        rp.setDescription("super");
        rp.insert();

        //Assign Mary to admin role
        UserRoleCursor ur = new UserRoleCursor(ctx);
        ur.setUserId(2);
        ur.setRoleId(1);
        ur.insert();

        //John & Mary send their messages
        MessageCursor msg = new MessageCursor(ctx);
        msg.setMessageId(1);
        msg.setUserId(1);
        msg.insert();
        msg.setMessageId(2);
        msg.setUserId(2);
        msg.insert();

        //John's messages
        SecurityCursor security = new SecurityCursor(ctx, Collections.singletonMap("uid", 1));
        assertEquals(1, security.count());
        security.first();
        assertEquals(1, security.getMessageId());

        //Mary's messages
        security = new SecurityCursor(ctx, Collections.singletonMap("uid", 2));

        Set<Integer> recordIds = new HashSet<>();
        for (SecurityCursor c: security){
            recordIds.add(c.getMessageId());
        }
        Set<Integer> expected = new HashSet<>();
        expected.add(1);
        expected.add(2);

        assertEquals(expected, recordIds);

    }
}
