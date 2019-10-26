# Write the benchmarking functions here.
# See "Writing benchmarks" in the asv docs for more information.
from tempfile import mkdtemp
from shutil import rmtree
import numpy as np
from hangar import Repository
from hangar.utils import folder_size


# ------------------------- fixture functions ----------------------------------


repo = None
co = None
tmpdir = ''
def repo_co_setup(num_samples):
    global repo
    global co
    global tmpdir
    tmpdir = mkdtemp()
    repo = Repository(path=tmpdir, exists=False)
    repo.init(user_name='tester', user_email='foo@test.bar', remove_old=True)
    co = repo.checkout(write=True)


def repo_co_teardown(num_samples):
    global repo
    global co
    global tmpdir
    co.close()
    repo._env._close_environments()
    rmtree(tmpdir)


def repo_teardown(num_samples):
    global repo
    global tmpdir
    global co
    repo._env._close_environments()
    rmtree(tmpdir)


# ------------------------- adding samples tests ---------------------------------


def time_add_uint32_2D_arrays_hdf5_00_default(num_samples):
    a = np.hamming(100).reshape(100, 1)
    b = np.hamming(100).reshape(1, 100)
    arr = np.round(a*b*1000).astype(np.uint32)
    aset = co.arraysets.init_arrayset('aset', prototype=arr, backend_opts='00')
    with aset as cm_aset:
        for i in range(num_samples):
            arr += 1
            cm_aset[i] = arr
time_add_uint32_2D_arrays_hdf5_00_default.setup = repo_co_setup
time_add_uint32_2D_arrays_hdf5_00_default.teardown = repo_co_teardown
time_add_uint32_2D_arrays_hdf5_00_default.params = [500, 10_000, 30_000]
time_add_uint32_2D_arrays_hdf5_00_default.param_names = ['num_samples']
time_add_uint32_2D_arrays_hdf5_00_default.processes = 1
time_add_uint32_2D_arrays_hdf5_00_default.number = 1
time_add_uint32_2D_arrays_hdf5_00_default.repeat = 1
time_add_uint32_2D_arrays_hdf5_00_default.warmup_time = 0.000001


def peakmem_add_uint32_2D_arrays_hdf5_00_default(num_samples):
    a = np.hamming(100).reshape(100, 1)
    b = np.hamming(100).reshape(1, 100)
    arr = np.round(a*b*1000).astype(np.uint32)
    aset = co.arraysets.init_arrayset('aset', prototype=arr, backend_opts='00')
    with aset as cm_aset:
        for i in range(num_samples):
            arr[:] += 1
            cm_aset[i] = arr
peakmem_add_uint32_2D_arrays_hdf5_00_default.setup = repo_co_setup
peakmem_add_uint32_2D_arrays_hdf5_00_default.teardown = repo_co_teardown
peakmem_add_uint32_2D_arrays_hdf5_00_default.params = [500, 10_000, 30_000]
peakmem_add_uint32_2D_arrays_hdf5_00_default.param_names = ['num_samples']
peakmem_add_uint32_2D_arrays_hdf5_00_default.timeout = 60

def track_repo_nbytes_add_uint32_2D_arrays_hdf5_00_default(num_samples):
    a = np.hamming(100).reshape(100, 1)
    b = np.hamming(100).reshape(1, 100)
    arr = np.round(a*b*1000).astype(np.uint32)
    aset = co.arraysets.init_arrayset('aset', prototype=arr, backend_opts='00')
    with aset as cm_aset:
        for i in range(num_samples):
            arr += 1
            cm_aset[i] = arr
    co.commit('first commit')
    co.close()
    nbytes = folder_size(repo._env.repo_path, recurse=True)
    return nbytes
track_repo_nbytes_add_uint32_2D_arrays_hdf5_00_default.setup = repo_co_setup
track_repo_nbytes_add_uint32_2D_arrays_hdf5_00_default.teardown = repo_teardown
track_repo_nbytes_add_uint32_2D_arrays_hdf5_00_default.params = [500, 10_000, 30_000]
track_repo_nbytes_add_uint32_2D_arrays_hdf5_00_default.param_names = ['num_samples']
track_repo_nbytes_add_uint32_2D_arrays_hdf5_00_default.unit = 'bytes'


def time_add_float32_2D_arrays_hdf5_00_default(num_samples):
    a = np.hamming(100).reshape(100, 1).astype(np.float32)
    b = np.hamming(100).reshape(1, 100).astype(np.float32)
    arr = a*b
    aset = co.arraysets.init_arrayset('aset', prototype=arr, backend_opts='00')
    with aset as cm_aset:
        for i in range(num_samples):
            arr += 1.0
            cm_aset[i] = arr
time_add_float32_2D_arrays_hdf5_00_default.setup = repo_co_setup
time_add_float32_2D_arrays_hdf5_00_default.teardown = repo_co_teardown
time_add_float32_2D_arrays_hdf5_00_default.params = [500, 10_000, 30_000]
time_add_float32_2D_arrays_hdf5_00_default.param_names = ['num_samples']
time_add_float32_2D_arrays_hdf5_00_default.processes = 1
time_add_float32_2D_arrays_hdf5_00_default.number = 1
time_add_float32_2D_arrays_hdf5_00_default.repeat = 1
time_add_float32_2D_arrays_hdf5_00_default.warmup_time = 0.000001


def peakmem_add_float32_2D_arrays_hdf5_00_default(num_samples):
    a = np.hamming(100).reshape(100, 1).astype(np.float32)
    b = np.hamming(100).reshape(1, 100).astype(np.float32)
    arr = a*b
    aset = co.arraysets.init_arrayset('aset', prototype=arr, backend_opts='00')
    with aset as cm_aset:
        for i in range(num_samples):
            arr += 1
            cm_aset[i] = arr
peakmem_add_float32_2D_arrays_hdf5_00_default.setup = repo_co_setup
peakmem_add_float32_2D_arrays_hdf5_00_default.teardown = repo_co_teardown
peakmem_add_float32_2D_arrays_hdf5_00_default.params = [500, 10_000, 30_000]
peakmem_add_float32_2D_arrays_hdf5_00_default.param_names = ['num_samples']


def track_repo_nbytes_add_float32_2D_arrays_hdf5_00_default(num_samples):
    a = np.hamming(100).reshape(100, 1).astype(np.float32)
    b = np.hamming(100).reshape(1, 100).astype(np.float32)
    arr = a*b
    aset = co.arraysets.init_arrayset('aset', prototype=arr, backend_opts='00')
    with aset as cm_aset:
        for i in range(num_samples):
            arr += 1
            cm_aset[i] = arr
    co.commit('first commit')
    co.close()
    nbytes = folder_size(repo._env.repo_path, recurse=True)
    return nbytes
track_repo_nbytes_add_float32_2D_arrays_hdf5_00_default.setup = repo_co_setup
track_repo_nbytes_add_float32_2D_arrays_hdf5_00_default.teardown = repo_teardown
track_repo_nbytes_add_float32_2D_arrays_hdf5_00_default.params = [500, 10_000, 30_000]
track_repo_nbytes_add_float32_2D_arrays_hdf5_00_default.param_names = ['num_samples']
track_repo_nbytes_add_float32_2D_arrays_hdf5_00_default.unit = 'bytes'


# ------------------------ numpy_10 ------------------------------------------


def time_add_uint32_2D_arrays_numpy_10_default(num_samples):
    a = np.hamming(100).reshape(100, 1)
    b = np.hamming(100).reshape(1, 100)
    arr = np.round(a*b*1000).astype(np.uint32)
    aset = co.arraysets.init_arrayset('aset', prototype=arr, backend_opts='10')
    with aset as cm_aset:
        for i in range(num_samples):
            arr += 1
            cm_aset[i] = arr
time_add_uint32_2D_arrays_numpy_10_default.setup = repo_co_setup
time_add_uint32_2D_arrays_numpy_10_default.teardown = repo_co_teardown
time_add_uint32_2D_arrays_numpy_10_default.params = [500, 10_000, 30_000]
time_add_uint32_2D_arrays_numpy_10_default.param_names = ['num_samples']
time_add_uint32_2D_arrays_numpy_10_default.processes = 1
time_add_uint32_2D_arrays_numpy_10_default.number = 1
time_add_uint32_2D_arrays_numpy_10_default.repeat = 1
time_add_uint32_2D_arrays_numpy_10_default.warmup_time = 0.000001


def peakmem_add_uint32_2D_arrays_numpy_10_default(num_samples):
    a = np.hamming(100).reshape(100, 1)
    b = np.hamming(100).reshape(1, 100)
    arr = np.round(a*b*1000).astype(np.uint32)
    aset = co.arraysets.init_arrayset('aset', prototype=arr, backend_opts='10')
    with aset as cm_aset:
        for i in range(num_samples):
            arr[:] += 1
            cm_aset[i] = arr
peakmem_add_uint32_2D_arrays_numpy_10_default.setup = repo_co_setup
peakmem_add_uint32_2D_arrays_numpy_10_default.teardown = repo_co_teardown
peakmem_add_uint32_2D_arrays_numpy_10_default.params = [500, 10_000, 30_000]
peakmem_add_uint32_2D_arrays_numpy_10_default.param_names = ['num_samples']


def track_repo_nbytes_add_uint32_2D_arrays_numpy_10_default(num_samples):
    a = np.hamming(100).reshape(100, 1)
    b = np.hamming(100).reshape(1, 100)
    arr = np.round(a*b*1000).astype(np.uint32)
    aset = co.arraysets.init_arrayset('aset', prototype=arr, backend_opts='10')
    with aset as cm_aset:
        for i in range(num_samples):
            arr += 1
            cm_aset[i] = arr
    co.commit('first commit')
    co.close()
    nbytes = folder_size(repo._env.repo_path, recurse=True)
    return nbytes
track_repo_nbytes_add_uint32_2D_arrays_numpy_10_default.setup = repo_co_setup
track_repo_nbytes_add_uint32_2D_arrays_numpy_10_default.teardown = repo_teardown
track_repo_nbytes_add_uint32_2D_arrays_numpy_10_default.params = [500, 10_000, 30_000]
track_repo_nbytes_add_uint32_2D_arrays_numpy_10_default.param_names = ['num_samples']
track_repo_nbytes_add_uint32_2D_arrays_numpy_10_default.unit = 'bytes'


def time_add_float32_2D_arrays_numpy_10_default(num_samples):
    a = np.hamming(100).reshape(100, 1).astype(np.float32)
    b = np.hamming(100).reshape(1, 100).astype(np.float32)
    arr = a*b
    aset = co.arraysets.init_arrayset('aset', prototype=arr, backend_opts='10')
    with aset as cm_aset:
        for i in range(num_samples):
            arr += 1.0
            cm_aset[i] = arr
time_add_float32_2D_arrays_numpy_10_default.setup = repo_co_setup
time_add_float32_2D_arrays_numpy_10_default.teardown = repo_co_teardown
time_add_float32_2D_arrays_numpy_10_default.params = [500, 10_000, 30_000]
time_add_float32_2D_arrays_numpy_10_default.param_names = ['num_samples']
time_add_float32_2D_arrays_numpy_10_default.processes = 1
time_add_float32_2D_arrays_numpy_10_default.number = 1
time_add_float32_2D_arrays_numpy_10_default.repeat = 1
time_add_float32_2D_arrays_numpy_10_default.warmup_time = 0.000001



def peakmem_add_float32_2D_arrays_numpy_10_default(num_samples):
    a = np.hamming(100).reshape(100, 1).astype(np.float32)
    b = np.hamming(100).reshape(1, 100).astype(np.float32)
    arr = a*b
    aset = co.arraysets.init_arrayset('aset', prototype=arr, backend_opts='10')
    with aset as cm_aset:
        for i in range(num_samples):
            arr += 1
            cm_aset[i] = arr
peakmem_add_float32_2D_arrays_numpy_10_default.setup = repo_co_setup
peakmem_add_float32_2D_arrays_numpy_10_default.teardown = repo_co_teardown
peakmem_add_float32_2D_arrays_numpy_10_default.params = [500, 10_000, 30_000]
peakmem_add_float32_2D_arrays_numpy_10_default.param_names = ['num_samples']


def track_repo_nbytes_add_float32_2D_arrays_numpy_10_default(num_samples):
    a = np.hamming(100).reshape(100, 1).astype(np.float32)
    b = np.hamming(100).reshape(1, 100).astype(np.float32)
    arr = a*b
    arr = np.random.randn(100, 100).astype(np.float32)
    aset = co.arraysets.init_arrayset('aset', prototype=arr, backend_opts='10')
    with aset as cm_aset:
        for i in range(num_samples):
            arr += 1
            cm_aset[i] = arr
    co.commit('first commit')
    co.close()
    nbytes = folder_size(repo._env.repo_path, recurse=True)
    return nbytes
track_repo_nbytes_add_float32_2D_arrays_numpy_10_default.setup = repo_co_setup
track_repo_nbytes_add_float32_2D_arrays_numpy_10_default.teardown = repo_teardown
track_repo_nbytes_add_float32_2D_arrays_numpy_10_default.params = [500, 10_000, 30_000]
track_repo_nbytes_add_float32_2D_arrays_numpy_10_default.param_names = ['num_samples']
track_repo_nbytes_add_float32_2D_arrays_numpy_10_default.unit = 'bytes'