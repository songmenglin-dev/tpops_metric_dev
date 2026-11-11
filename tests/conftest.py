# -*- coding: utf-8 -*-
"""pytest 公共夹具。"""
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TOOLS_DIR = os.path.join(REPO_ROOT, 'tools')
for _path in (REPO_ROOT, TOOLS_DIR):
    if _path not in sys.path:
        sys.path.insert(0, _path)

DATA_DIR = os.path.join(REPO_ROOT, 'data')
XLSX_PATH = os.path.join(
    REPO_ROOT, '高斯DB-指标明细（客户要求的应该要提供的指标明细）.xlsx'
)
