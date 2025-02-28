#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
更新数据库脚本
"""

import os
import sys
import logging
from sqlalchemy import create_engine, text, inspect

# 导入数据库模型
from src.database.models import Base, init_db, _check_and_add_columns
import config.settings as config

def main():
    # 初始化数据库
    print("正在初始化数据库...")
    init_db()
    print("数据库初始化完成")
    
    # 检查并添加新列
    print("正在检查并添加新列...")
    _check_and_add_columns()
    print("检查并添加新列完成")
    
    print("数据库更新完成")
    return True

if __name__ == "__main__":
    main() 