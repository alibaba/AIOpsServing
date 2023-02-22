#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from distutils.core import setup

from setuptools import find_packages, setup


def _process_requirements():
    packages = open('requirements.txt').read().strip().split('\n')
    requires = []
    for pkg in packages:
        if pkg.startswith('git+https'):
            return_code = os.system('pip install {}'.format(pkg))
        else:
            requires.append(pkg)
    return requires


setup(
    name="alibaba-aiops-serving",
    version="1.0.2",
    description="deploy and benchmark models for aiops",
    url="",

    install_requires=_process_requirements(),
    license="Apache 2.0",
    packages=find_packages(),
    python_requires=">=3.7.10",
    entry_points={
        'console_scripts': [
            'ziya = api.cli_click:cli'
        ]
    },

)
