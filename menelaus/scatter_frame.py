# -*- coding: utf-8 -*-

import os
import uuid
import pandas as pd
from pandas.io.pytables import HDFStore


class ScatterGatherFrame(object):

    """
    A class that performs scatter-gather operations: first some computation
    is done on parralel, each node saves data separately and then
    results can be gathered
    """

    @classmethod
    def root_path(self):
        return "."

    def __init__(self, root_name, scatter = False, pd_kwargs=None):
        if not os.path.isabs(root_name):
            root_name = os.path.join(self.root_path(), root_name)
        self.scatter_dir = root_name
        self.root_name = root_name
        if os.path.exists(self.scatter_dir):
            if not os.path.isdir(self.scatter_dir):
                raise ValueError("This path should be a directory {}".format(root_name))
        else:
            os.makedirs(root_name)
        self._scatter = scatter
        if pd_kwargs is None:
            pd_kwargs = {}
        self.frame = pd.DataFrame(**pd_kwargs)
        self.scatter_key = None

    def append(self, *args, **kwargs):
        self.frame = self.frame.append(*args, **kwargs)

    def __savefile(self):
        if self._scatter:
            if self.scatter_key is None:
                scatter_key = str(uuid.uuid4())
            else:
                scatter_key = self.scatter_key
            savefile = os.path.join(self.scatter_dir, scatter_key)
        else:
            savefile = self.root_name
        return savefile

    def save(self, format="csv"):

        savefile = self.__savefile()

        if format == "csv":
            self.frame.to_csv(savefile + ".csv", mode='w')
        elif format == "hdf":
            self.frame.to_hdf(savefile + ".hdf", "data", complevel=5, complib="lzo", mode='w')

    def load(self, format='csv'):
        savefile = self.__savefile()
        if format == "csv":
            self.frame.from_csv(savefile + ".csv")
        elif format == "hdf":
            store = HDFStore(savefile + ".hdf")
            try:
                self.frame = store['data']
            finally:
                store.close()

    def scatter(self):
        self._scatter = True

    def gather(self):
        self._scatter = False
        frames = []
        for f in os.listdir(self.scatter_dir):
            if f.endswith("csv"):
                frames.append(pd.read_csv(os.path.join(self.scatter_dir, f)))
        self.frame = pd.concat(frames)
