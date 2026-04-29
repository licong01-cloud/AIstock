from setuptools import setup, find_packages

setup(
    name="aistock_models",
    version="1.0.0",
    description="Custom Qlib model classes for AIstock (LambdaMART, TabPFN)",
    packages=find_packages(),
    python_requires=">=3.10",
    install_requires=[
        "lightgbm>=4.0",
        "tabpfn>=7.0",
        "numpy",
        "pandas",
    ],
)
