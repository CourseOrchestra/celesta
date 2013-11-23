from . import _g1_orm
from . import hello

_g1_orm.aaCursor.onPreInsert.append(hello.testTrigger)

print "g1 initialization"