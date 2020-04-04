package ru.curs.celesta.script;

import org.junit.jupiter.api.TestTemplate;
import ru.curs.celesta.CallContext;
import unionAll.MessageCursor;
import unionAll.RoleCursor;
import unionAll.Role_permissionsCursor;
import unionAll.SecurityCursor;
import unionAll.UserCursor;
import unionAll.User_roleCursor;

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
        Role_permissionsCursor rp = new Role_permissionsCursor(ctx);
        rp.setRole_id(1);
        rp.setDescription("super");
        rp.insert();

        //Assign Mary to admin role
        User_roleCursor ur = new User_roleCursor(ctx);
        ur.setUser_id(2);
        ur.setRole_id(1);
        ur.insert();

        //John & Mary send their messages
        MessageCursor msg = new MessageCursor(ctx);
        msg.setMessage_id(1);
        msg.setUser_id(1);
        msg.insert();
        msg.setMessage_id(2);
        msg.setUser_id(2);
        msg.insert();

        //John's messages
        SecurityCursor security = new SecurityCursor(ctx, Collections.singletonMap("uid", 1));
        assertEquals(1, security.count());
        security.first();
        assertEquals(1, security.getMessage_id());

        //Mary's messages
        security = new SecurityCursor(ctx, Collections.singletonMap("uid", 2));

        Set<Integer> recordIds = new HashSet<>();
        for (SecurityCursor c: security){
            recordIds.add(c.getMessage_id());
        }
        Set<Integer> expected = new HashSet<>();
        expected.add(1);
        expected.add(2);

        assertEquals(expected, recordIds);

    }
}
