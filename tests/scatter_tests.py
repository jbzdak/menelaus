# -*- coding: utf-8 -*-
import shutil
from copy import deepcopy
import unittest
from menelaus.scatter_frame import ScatterGatherFrame
from tempfile import mkdtemp


class ScatterGatherDF(unittest.TestCase):

    def setUp(self):
        self.dir = mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.dir)

    def scatter_test(self):
        frame = ScatterGatherFrame(self.dir + "/foo")
        frame.scatter()
        f1 = deepcopy(frame)
        f2 = deepcopy(frame)
        f1.append([{'a': 1, 'b': 10}])
        f2.append([{'a': 2, 'b': 20}])
        f1.save()
        f2.save()
        frame.gather()
        self.assertEqual([1, 2], sorted(frame.frame.a))
        self.assertEqual([10, 20], sorted(frame.frame.b))

