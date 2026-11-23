"""回归测试包。

**为什么要有这个包**：``tests/`` 下没有 ``__init__.py``，若本目录也没有，
pytest 会把 ``tests/conftest.py`` 与 ``tests/regression/conftest.py`` 都当成
顶层模块 ``conftest`` 导入 → ``import file mismatch`` 报错。
加上 ``__init__.py`` 后本目录是独立包（模块名 ``regression.conftest``），
与 ``tests/unit`` / ``tests/integration`` 的做法一致。
"""
