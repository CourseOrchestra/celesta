# coding=UTF-8

import unittest
import os

if __name__ == '__main__':
    testsuite = unittest.TestLoader().discover(
        start_dir=os.path.join(os.path.dirname(__file__), "..")
    )
    unittest.TextTestRunner(verbosity=1).run(testsuite)
