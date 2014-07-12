# coding=UTF-8
print "hello unit called"

from g1._g1_orm import aaCursor 
from g1._g1_orm import adressesCursor
from g2._g2_orm import bCursor
from g3._g3_orm import cCursor
from g1._g1_orm import testviewCursor
import java.io.OutputStreamWriter as OutputStreamWriter
import java.io.InputStreamReader as InputStreamReader
import java.io.BufferedReader as BufferedReader
import random

def proc1(context):
    print "Hello from proc1!"
    context.data['myvalue'] = 'some shared text data value'

def proc2(context):
    print "Hello from proc2!"
    print context.data['myvalue']
