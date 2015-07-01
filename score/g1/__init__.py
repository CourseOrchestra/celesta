from . import _g1_orm
from . import hello
import initcontext
_g1_orm.aaCursor.onPreInsert.append(hello.testTrigger)

print "g1 initialization"
print initcontext()