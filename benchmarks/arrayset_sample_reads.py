# Write the benchmarking functions here.
# See "Writing benchmarks" in the asv docs for more information.
from tempfile import mkdtemp
from shutil import rmtree
import numpy as np
from hangar import Repository
from os import getcwd


# ------------------------- fixture functions ----------------------------------


def repo_co_setup_cache_30000_samples_hdf5_00_default():
    tmpdir_hdf5_00 = getcwd()
    repo_hdf5_00 = Repository(path=tmpdir_hdf5_00, exists=False)
    repo_hdf5_00.init(user_name='tester', user_email='foo@test.bar', remove_old=True)
    co = repo_hdf5_00.checkout(write=True)
    aint = np.hamming(100).reshape(100, 1)
    bint = np.hamming(100).reshape(1, 100)
    arrint = np.round(aint * bint * 1000).astype(np.uint32)
    afloat = np.hamming(100).reshape(100, 1).astype(np.float32)
    bfloat = np.hamming(100).reshape(1, 100).astype(np.float32)
    arrfloat = afloat * bfloat
    try:
        aset_int = co.arraysets.init_arrayset('aset_int', prototype=arrint, backend_opts='00')
        aset_float = co.arraysets.init_arrayset('aset_float', prototype=arrfloat, backend_opts='00')
    except TypeError:
        aset_int = co.arraysets.init_arrayset('aset_int', prototype=arrint, backend='00')
        aset_float = co.arraysets.init_arrayset('aset_float', prototype=arrfloat, backend='00')
    with aset_int as cm_aset_int, aset_float as cm_aset_float:
        for i in range(30_000):
            arrfloat += 1
            arrint += 1
            cm_aset_float[i] = arrfloat
            cm_aset_int[i] = arrint
    co.commit('first commit')
    co.close()


def repo_co_setup_cache_30000_samples_numpy_10_default():
    tmpdir_np_10 = getcwd()
    repo_np_10 = Repository(path=tmpdir_np_10, exists=False)
    repo_np_10.init(user_name='tester', user_email='foo@test.bar', remove_old=True)
    co = repo_np_10.checkout(write=True)
    aint = np.hamming(100).reshape(100, 1)
    bint = np.hamming(100).reshape(1, 100)
    arrint = np.round(aint * bint * 1000).astype(np.uint32)
    afloat = np.hamming(100).reshape(100, 1).astype(np.float32)
    bfloat = np.hamming(100).reshape(1, 100).astype(np.float32)
    arrfloat = afloat * bfloat
    try:
        aset_int = co.arraysets.init_arrayset('aset_int', prototype=arrint, backend_opts='10')
        aset_float = co.arraysets.init_arrayset('aset_float', prototype=arrfloat, backend_opts='10')
    except TypeError:
        aset_int = co.arraysets.init_arrayset('aset_int', prototype=arrint, backend='10')
        aset_float = co.arraysets.init_arrayset('aset_float', prototype=arrfloat, backend='10')
    with aset_int as cm_aset_int, aset_float as cm_aset_float:
        for i in range(30_000):
            arrfloat += 1
            arrint += 1
            cm_aset_float[i] = arrfloat
            cm_aset_int[i] = arrint
    co.commit('first commit')
    co.close()


def repo_co_setup_30000_samples_hdf5_00_default():
    global co
    tmpdir_hdf5_00 = getcwd()
    repo_hdf5_00 = Repository(tmpdir_hdf5_00, exists=True)
    co = repo_hdf5_00.checkout(write=False)


def repo_co_setup_30000_samples_numpy_10_default():
    global co
    tmpdir_np_10 = getcwd()
    repo_np_10 = Repository(tmpdir_np_10, exists=True)
    co = repo_np_10.checkout(write=False)


def repo_co_teardown():
    global co
    co.close()


# ------------------------- adding samples tests ---------------------------------


def time_read_uint32_30000_samples_hdf5_00_default():
    aset = co.arraysets['aset_int']
    with aset as cm_aset:
        for i in range(30_000):
            arr = cm_aset[i]
time_read_uint32_30000_samples_hdf5_00_default.setup_cache = repo_co_setup_cache_30000_samples_hdf5_00_default
time_read_uint32_30000_samples_hdf5_00_default.setup = repo_co_setup_30000_samples_hdf5_00_default
time_read_uint32_30000_samples_hdf5_00_default.teardown = repo_co_teardown
time_read_uint32_30000_samples_hdf5_00_default.processes = 1
time_read_uint32_30000_samples_hdf5_00_default.number = 1
time_read_uint32_30000_samples_hdf5_00_default.repeat = 1
time_read_uint32_30000_samples_hdf5_00_default.warmup_time = 0.000001


def time_read_float32_30000_samples_hdf5_00_default():
    aset = co.arraysets['aset_float']
    with aset as cm_aset:
        for i in range(30_000):
            arr = cm_aset[i]
time_read_float32_30000_samples_hdf5_00_default.setup_cache = repo_co_setup_cache_30000_samples_hdf5_00_default
time_read_float32_30000_samples_hdf5_00_default.setup = repo_co_setup_30000_samples_hdf5_00_default
time_read_float32_30000_samples_hdf5_00_default.teardown = repo_co_teardown
time_read_float32_30000_samples_hdf5_00_default.processes = 1
time_read_float32_30000_samples_hdf5_00_default.number = 1
time_read_float32_30000_samples_hdf5_00_default.repeat = 1
time_read_float32_30000_samples_hdf5_00_default.warmup_time = 0.000001


def time_read_uint32_30000_samples_numpy_10_default():
    aset = co.arraysets['aset_int']
    with aset as cm_aset:
        for i in range(30_000):
            arr = cm_aset[i]
time_read_uint32_30000_samples_numpy_10_default.setup_cache = repo_co_setup_cache_30000_samples_numpy_10_default
time_read_uint32_30000_samples_numpy_10_default.setup = repo_co_setup_30000_samples_numpy_10_default
time_read_uint32_30000_samples_numpy_10_default.teardown = repo_co_teardown
time_read_uint32_30000_samples_numpy_10_default.processes = 1
time_read_uint32_30000_samples_numpy_10_default.number = 1
time_read_uint32_30000_samples_numpy_10_default.repeat = 1
time_read_uint32_30000_samples_numpy_10_default.warmup_time = 0.000001


def time_read_float32_30000_samples_numpy_10_default():
    aset = co.arraysets['aset_float']
    with aset as cm_aset:
        for i in range(30_000):
            arr = cm_aset[i]
time_read_float32_30000_samples_numpy_10_default.setup_cache = repo_co_setup_cache_30000_samples_numpy_10_default
time_read_float32_30000_samples_numpy_10_default.setup = repo_co_setup_30000_samples_numpy_10_default
time_read_float32_30000_samples_numpy_10_default.teardown = repo_co_teardown
time_read_float32_30000_samples_numpy_10_default.processes = 1
time_read_float32_30000_samples_numpy_10_default.number = 1
time_read_float32_30000_samples_numpy_10_default.repeat = 1
time_read_float32_30000_samples_numpy_10_default.warmup_time = 0.000001


# ---------------- peakmem --------------------------------


def peakmem_read_uint32_30000_samples_hdf5_00_default():
    aset = co.arraysets['aset_int']
    with aset as cm_aset:
        for i in range(30_000):
            arr = cm_aset[i]
peakmem_read_uint32_30000_samples_hdf5_00_default.setup_cache = repo_co_setup_cache_30000_samples_hdf5_00_default
peakmem_read_uint32_30000_samples_hdf5_00_default.setup = repo_co_setup_30000_samples_hdf5_00_default
peakmem_read_uint32_30000_samples_hdf5_00_default.teardown = repo_co_teardown


def peakmem_read_float32_30000_samples_hdf5_00_default():
    aset = co.arraysets['aset_float']
    with aset as cm_aset:
        for i in range(30_000):
            arr = cm_aset[i]
peakmem_read_float32_30000_samples_hdf5_00_default.setup_cache = repo_co_setup_cache_30000_samples_hdf5_00_default
peakmem_read_float32_30000_samples_hdf5_00_default.setup = repo_co_setup_30000_samples_hdf5_00_default
peakmem_read_float32_30000_samples_hdf5_00_default.teardown = repo_co_teardown


def peakmem_read_uint32_30000_samples_numpy_10_default():
    aset = co.arraysets['aset_int']
    with aset as cm_aset:
        for i in range(30_000):
            arr = cm_aset[i]
peakmem_read_uint32_30000_samples_numpy_10_default.setup_cache = repo_co_setup_cache_30000_samples_numpy_10_default
peakmem_read_uint32_30000_samples_numpy_10_default.setup = repo_co_setup_30000_samples_numpy_10_default
peakmem_read_uint32_30000_samples_numpy_10_default.teardown = repo_co_teardown


def peakmem_read_float32_30000_samples_numpy_10_default():
    aset = co.arraysets['aset_float']
    with aset as cm_aset:
        for i in range(30_000):
            arr = cm_aset[i]
peakmem_read_float32_30000_samples_numpy_10_default.setup_cache = repo_co_setup_cache_30000_samples_numpy_10_default
peakmem_read_float32_30000_samples_numpy_10_default.setup = repo_co_setup_30000_samples_numpy_10_default
peakmem_read_float32_30000_samples_numpy_10_default.teardown = repo_co_teardown