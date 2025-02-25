#!/usr/bin/env python3
import unittest
import os
import sys
import time
import argparse

# 添加项目根目录到路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def run_tests(coverage=False, specific_tests=None):
    """运行测试并可选生成覆盖率报告
    
    Args:
        coverage: 是否生成覆盖率报告
        specific_tests: 要运行的特定测试模块列表
    """
    # 创建测试加载器
    loader = unittest.TestLoader()
    
    # 创建测试套件
    if specific_tests:
        test_suite = unittest.TestSuite()
        tests_dir = os.path.dirname(os.path.abspath(__file__))
        for test_module in specific_tests:
            module_path = os.path.join(tests_dir, test_module)
            if os.path.exists(module_path):
                suite = loader.discover(tests_dir, pattern=test_module)
                test_suite.addTest(suite)
            else:
                print(f"警告: 测试模块 {test_module} 不存在")
    else:
        # 发现测试目录中的所有测试
        tests_dir = os.path.dirname(os.path.abspath(__file__))
        test_suite = loader.discover(tests_dir)
    
    # 创建测试运行器
    runner = unittest.TextTestRunner(verbosity=2)
    
    # 运行测试
    print("=" * 70)
    print("开始运行Telegram监控系统测试")
    print("=" * 70)
    
    # 创建必要的目录
    os.makedirs('./logs', exist_ok=True)
    os.makedirs('./data', exist_ok=True)
    os.makedirs('./media', exist_ok=True)
    
    # 测量开始时间
    start_time = time.time()
    
    # 如果使用覆盖率分析
    if coverage:
        try:
            from coverage import Coverage
            cov = Coverage(source=['src'], omit=['*/__pycache__/*', '*/tests/*'])
            cov.start()
            result = runner.run(test_suite)
            cov.stop()
            
            # 生成报告
            print("\n生成测试覆盖率报告...")
            cov.save()
            
            # 命令行报告
            cov.report()
            
            # 生成HTML报告
            html_dir = './coverage_html'
            os.makedirs(html_dir, exist_ok=True)
            cov.html_report(directory=html_dir)
            print(f"HTML覆盖率报告已生成到: {html_dir}")
            
        except ImportError:
            print("警告: 未安装coverage模块，跳过覆盖率分析")
            result = runner.run(test_suite)
    else:
        # 正常运行测试
        result = runner.run(test_suite)
    
    # 计算耗时
    duration = time.time() - start_time
    
    # 输出摘要
    print("\n")
    print("=" * 70)
    print("测试摘要:")
    print(f"运行测试: {result.testsRun}")
    print(f"失败数: {len(result.failures)}")
    print(f"错误数: {len(result.errors)}")
    print(f"跳过数: {len(result.skipped)}")
    print(f"总耗时: {duration:.2f}秒")
    print("=" * 70)
    
    # 返回测试结果
    return result.wasSuccessful()

if __name__ == '__main__':
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='运行Telegram监控系统测试')
    parser.add_argument('--coverage', action='store_true', help='生成测试覆盖率报告')
    parser.add_argument('tests', nargs='*', help='要运行的特定测试模块')
    args = parser.parse_args()
    
    # 运行测试
    success = run_tests(coverage=args.coverage, specific_tests=args.tests)
    
    # 设置退出代码
    sys.exit(not success) 