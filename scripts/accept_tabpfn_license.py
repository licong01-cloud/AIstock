"""Quick TabPFN license acceptance and model download test."""
import os
os.environ["TABPFN_TOKEN"] = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyIjoiYzU0YTQ5NTctMjAwMi00ODM1LThjOTUtMzA5YmNkZTY5ZGVlIiwiZXhwIjoxODA5MDE0NDQwfQ.udxgLP4VxGGO2wkbZqXU0P6ugqvjZXeffQgjljztBxU"

from tabpfn import TabPFNClassifier
import tabpfn
print(f"TabPFN version: {tabpfn.__version__}")

try:
    clf = TabPFNClassifier(device="cpu", n_estimators=1)
    print("TabPFNClassifier created successfully")
    import numpy as np
    X = np.random.randn(50, 10)
    y = np.random.randint(0, 3, 50)
    clf.fit(X, y)
    proba = clf.predict_proba(X[:5])
    print(f"Prediction shape: {proba.shape}")
    print("TabPFN license accepted and model works")
except Exception as e:
    error_msg = str(e)
    print(f"Error: {error_msg}")
    if "license" in error_msg.lower():
        print(">>> 需要在浏览器中访问 https://ux.priorlabs.ai 接受许可协议")
        print(">>> 登录 → Licenses 标签页 → 接受许可 → 重新运行此脚本")
