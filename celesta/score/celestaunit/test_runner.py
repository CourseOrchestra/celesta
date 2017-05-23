# coding=UTF-8

import unittest
import os

'''Runner всех jython тестов. 
    Запускается автоматически во время полной сборки проекта,
    или вручную.'''
if __name__ == '__main__':
    testsuite = unittest.TestLoader().discover(
        start_dir=os.path.join(os.path.dirname(__file__), "..")
    )
    result = unittest.TextTestRunner(verbosity=1).run(testsuite)

    if result.failures or result.errors:
        raise RuntimeError("FAIL: Errors/Failures in Jython tests")