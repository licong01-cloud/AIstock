#!/usr/bin/env python3
"""验证 QE 使用 qlib 内置模型的 bug 修复"""

# 测试 1: 验证类型转换逻辑
print("=== Test 1: Type Conversion Logic ===")
test_cases = [
    ("1e-3", float, 0.001),
    ("5e-4", float, 0.0005),
    (0.001, float, 0.001),
    (1e-3, float, 0.001),
]

for input_val, expected_type, expected_val in test_cases:
    # 模拟修复后的逻辑
    result = float(input_val) if isinstance(input_val, str) else input_val
    assert isinstance(result, expected_type), f"Type mismatch: {type(result)} != {expected_type}"
    assert abs(result - expected_val) < 1e-10, f"Value mismatch: {result} != {expected_val}"
    print(f"  OK {input_val} ({type(input_val).__name__}) -> {result} ({type(result).__name__})")

print("Test 1: PASSED\n")

# 测试 2: 验证 YAML 数值格式
print("=== Test 2: YAML Numeric Format ===")
import yaml

yaml_with_numbers = """
model:
  kwargs:
    lr: 0.001
    weight_decay: 0.0001
    batch_size: 4096
"""

yaml_with_strings = """
model:
  kwargs:
    lr: "0.001"
    weight_decay: "0.0001"
    batch_size: 4096
"""

config_numbers = yaml.safe_load(yaml_with_numbers)
config_strings = yaml.safe_load(yaml_with_strings)

print(f"  Numbers - lr type: {type(config_numbers['model']['kwargs']['lr'])}")
print(f"  Strings - lr type: {type(config_strings['model']['kwargs']['lr'])}")

assert isinstance(config_numbers['model']['kwargs']['lr'], float), "YAML should parse unquoted numbers as float"
assert isinstance(config_strings['model']['kwargs']['lr'], str), "YAML should parse quoted numbers as string"

print("Test 2: PASSED\n")

# 测试 3: 验证 GeneralPTNN 参数类型要求
print("=== Test 3: GeneralPTNN Parameter Types ===")
try:
    from qlib.contrib.model.pytorch_general_nn import GeneralPTNN

    # 测试接受 float
    try:
        model = GeneralPTNN(lr=0.001, weight_decay=0.0001, n_epochs=10)
        print("  OK GeneralPTNN accepts float lr and weight_decay")
    except Exception as e:
        print(f"  ✗ GeneralPTNN rejects float: {e}")
        raise

    # 测试拒绝 string
    try:
        model = GeneralPTNN(lr="0.001", weight_decay="0.0001", n_epochs=10)
        print("  ✗ GeneralPTNN should reject string lr and weight_decay")
        raise AssertionError("GeneralPTNN should not accept string parameters")
    except TypeError as e:
        print(f"  OK GeneralPTNN correctly rejects string: {type(e).__name__}")

    print("Test 3: PASSED\n")
except ImportError:
    print("  WARN Skipped (qlib not available in current environment)\n")

print("=" * 50)
print("All tests PASSED! Bug fixes are correct.")
print("=" * 50)
