"""验证v25集成到QE实验框架"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

def verify_v25_integration():
    print('=== 验证v25集成 ===\n')

    # 1. 验证模型文件存在
    print('1. 检查模型文件...')
    early_model = Path('/home/lc999/data/rl_models/v25/v25_early_net_joint_fixed.pt')
    late_model = Path('/home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt')

    if early_model.exists():
        print(f'  ✓ Early model: {early_model}')
    else:
        print(f'  ✗ Early model not found: {early_model}')
        return False

    if late_model.exists():
        print(f'  ✓ Late model: {late_model}')
    else:
        print(f'  ✗ Late model not found: {late_model}')
        return False

    # 2. 验证executor可以导入
    print('\n2. 检查executor导入...')
    try:
        from rl_execution.executor.v25_two_stage_executor import V25TwoStageExecutor
        print('  ✓ V25TwoStageExecutor导入成功')
    except Exception as e:
        print(f'  ✗ V25TwoStageExecutor导入失败: {e}')
        return False

    # 3. 验证backend algo可以导入
    print('\n3. 检查backend algo导入...')
    try:
        from backend.execution_algos.v25_two_stage_algo import V25TwoStageAlgo
        print('  ✓ V25TwoStageAlgo导入成功')
    except Exception as e:
        print(f'  ✗ V25TwoStageAlgo导入失败: {e}')
        return False

    # 4. 验证algo注册
    print('\n4. 检查algo注册...')
    try:
        from backend.execution_algos.registry import ALGO_REGISTRY, get_algo
        if 'V25_TWO_STAGE' in ALGO_REGISTRY:
            print('  ✓ V25_TWO_STAGE已注册')
            print(f'    注册的类: {ALGO_REGISTRY["V25_TWO_STAGE"].__name__}')
        else:
            print(f'  ✗ V25_TWO_STAGE未注册')
            print(f'    可用算法: {list(ALGO_REGISTRY.keys())}')
            return False
    except Exception as e:
        print(f'  ✗ V25_TWO_STAGE注册检查失败: {e}')
        return False

    # 5. 验证模型可以加载
    print('\n5. 检查模型加载...')
    try:
        import torch
        executor = V25TwoStageExecutor(
            early_model_path=str(early_model),
            late_model_path=str(late_model),
            device='cpu'
        )
        if executor.early_model is not None and executor.late_model is not None:
            print('  ✓ 模型加载成功')
            print(f'    Early model params: {sum(p.numel() for p in executor.early_model.parameters()):,}')
            print(f'    Late model params: {sum(p.numel() for p in executor.late_model.parameters()):,}')
        else:
            print('  ✗ 模型加载失败')
            return False
    except Exception as e:
        print(f'  ✗ 模型加载异常: {e}')
        import traceback
        traceback.print_exc()
        return False

    # 6. 验证配置文件
    print('\n6. 检查配置文件...')
    config_file = Path(__file__).parent.parent / 'configs' / 'execution_algos' / 'v25_two_stage.yaml'
    if config_file.exists():
        print(f'  ✓ 配置文件: {config_file}')
    else:
        print(f'  ⚠ 配置文件不存在: {config_file}')

    # 7. 测试algo实例化
    print('\n7. 测试algo实例化...')
    try:
        config = {
            'early_model_path': str(early_model),
            'late_model_path': str(late_model),
            'device': 'cpu'
        }
        algo = get_algo('V25_TWO_STAGE', config)
        print(f'  ✓ Algo实例化成功: {algo.__class__.__name__}')
    except Exception as e:
        print(f'  ✗ Algo实例化失败: {e}')
        import traceback
        traceback.print_exc()
        return False

    print('\n=== 验证完成 ===')
    print('✅ v25已成功集成到QE实验框架')
    print('\n使用方法:')
    print('  在QE实验配置中指定:')
    print('    execution_algo: V25_TWO_STAGE')
    print('    execution_config:')
    print('      early_model_path: /home/lc999/data/rl_models/v25/v25_early_net_joint_fixed.pt')
    print('      late_model_path: /home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt')
    print('      device: cuda')
    print('\n可用的执行算法:')
    for code in sorted(ALGO_REGISTRY.keys()):
        print(f'  - {code}')

    return True

if __name__ == '__main__':
    success = verify_v25_integration()
    sys.exit(0 if success else 1)
