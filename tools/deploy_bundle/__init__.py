"""采集端**离线部署包**的构建工具。

拆成三个模块（每个都远小于项目约定的 400 行上限）：

- :mod:`deploy_bundle.files` —— 文件拷贝、sha256、清单；
- :mod:`deploy_bundle.wheels` —— wheel 解析、架构无关性校验、版本锁；
- :mod:`deploy_bundle.bundle` —— 装配整个部署包。

对外入口是 :func:`deploy_bundle.bundle.build_bundle` 与
:func:`deploy_bundle.bundle.make_tarball`。
"""

from .bundle import DeployBundleError, build_bundle, make_tarball, read_version
from .files import copy_tree, sha256
from .wheels import assert_arch_independent, resolve_wheels, write_lock_file

__all__ = [
    'DeployBundleError',
    'assert_arch_independent',
    'build_bundle',
    'copy_tree',
    'make_tarball',
    'read_version',
    'resolve_wheels',
    'sha256',
    'write_lock_file',
]
