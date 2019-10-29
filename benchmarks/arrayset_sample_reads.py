# Write the benchmarking functions here.
# See "Writing benchmarks" in the asv docs for more information.
import numpy as np
from hangar import Repository
from tempfile import mkdtemp
from shutil import rmtree
from hangar.utils import folder_size


# ------------------------- fixture functions ----------------------------------


class UINT16_DType(object):

    params = ['hdf5_00', 'numpy_10']
    param_names = ['backend']
    processes = 2
    number = 2
    repeat = 2
    warmup_time = 0

    def setup(self, backend):
        self.tmpdir = mkdtemp()
        self.repo = Repository(path=self.tmpdir, exists=False)
        self.repo.init('tester', 'foo@test.bar', remove_old=True)
        co = self.repo.checkout(write=True)

        aint = np.hamming(100).reshape(100, 1)
        bint = np.hamming(100).reshape(1, 100)
        cint = np.round(aint * bint * 1000).astype(np.uint16)
        arrint = np.zeros((100, 100), dtype=cint.dtype)
        arrint[:, :] = cint

        backend_code = backend[-2:]
        try:
            aset_int = co.arraysets.init_arrayset(
                'aset_int', prototype=arrint, backend_opts=backend_code)
        except TypeError:
            aset_int = co.arraysets.init_arrayset(
                'aset_int', prototype=arrint, backend=backend_code)
        with aset_int as cm_aset_int:
            for i in range(5000):
                arrint += 1
                cm_aset_int[i] = arrint
        co.commit('first commit')
        co.close()
        self.co = self.repo.checkout(write=False)

    def teardown(self, backend):
        self.co.close()
        self.repo._env._close_environments()
        rmtree(self.tmpdir)

    def time_read_5000_samples(self, backend):
        aset = self.co.arraysets['aset_int']
        with aset as cm_aset:
            for i in range(5000):
                arr = cm_aset[i]

    def track_repo_size_5000_samples(self, backend):
        return folder_size(self.repo._env.repo_path, recurse=True)

    track_repo_size_5000_samples.unit = 'bytes'


class FLOAT32_DType(object):

    params = ['hdf5_00', 'numpy_10']
    param_names = ['backend']
    processes = 2
    number = 2
    repeat = 2
    warmup_time = 0

    def setup(self, backend):
        self.tmpdir = mkdtemp()
        self.repo = Repository(path=self.tmpdir, exists=False)
        self.repo.init('tester', 'foo@test.bar', remove_old=True)
        co = self.repo.checkout(write=True)

        afloat = np.hamming(100).reshape(100, 1).astype(np.float32)
        bfloat = np.hamming(100).reshape(1, 100).astype(np.float32)
        cfloat = np.round(afloat * bfloat * 1000)
        arrfloat = np.zeros((100, 100), dtype=cfloat.dtype)
        arrfloat[:, :] = cfloat

        backend_code = backend[-2:]
        try:
            aset_float = co.arraysets.init_arrayset(
                'aset_float', prototype=arrfloat, backend_opts=backend_code)
        except TypeError:
            aset_float = co.arraysets.init_arrayset(
                'aset_float', prototype=arrfloat, backend=backend_code)
        with aset_float as cm_aset_float:
            for i in range(5000):
                arrfloat += 1
                cm_aset_float[i] = arrfloat
        co.commit('first commit')
        co.close()
        self.co = self.repo.checkout(write=False)

    def teardown(self, backend):
        self.co.close()
        self.repo._env._close_environments()
        rmtree(self.tmpdir)

    def time_read_5000_samples(self, backend):
        aset = self.co.arraysets['aset_float']
        with aset as cm_aset:
            for i in range(5000):
                arr = cm_aset[i]

    def track_repo_size_5000_samples(self, backend):
        return folder_size(self.repo._env.repo_path, recurse=True)

    track_repo_size_5000_samples.unit = 'bytes'