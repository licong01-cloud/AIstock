"""检查HDF5文件结构"""
import h5py
import os

hdf5_dir = r"F:\Dev\AIstock\data\correlation_matrices"
files = sorted([f for f in os.listdir(hdf5_dir) if f.startswith("corr_") and f.endswith(".h5")], reverse=True)

if files:
    latest = os.path.join(hdf5_dir, files[0])
    print(f"文件: {files[0]}")

    with h5py.File(latest, "r") as f:
        print(f"\nHDF5文件包含的键:")
        for key in f.keys():
            print(f"  - {key}: {f[key].shape if hasattr(f[key], 'shape') else 'group'}")
