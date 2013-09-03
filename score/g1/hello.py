from g1._g1_orm import aaCursor 

def hello(context, arg):
    print 'Hello, world from Celesta Python procedure.'
    print 'user %s' % context.userId
    print 'Argument passed was "%s".' % arg
    aa = aaCursor(context)
    aa.deleteAll()
    for i in range(1, 12):
        aa.idaa = i
        aa.idc = i * i
        aa.insert()
   
    while aa.next():
        print "%s : %s" % (aa.idaa , aa.idc)
    print 'Python procedure finished.'