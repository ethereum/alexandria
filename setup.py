#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import (
    setup,
    find_packages,
)

extras_require = {
    'test': [
        "hypothesis==5.8.0",
        "pytest==5.4.1",
        "pytest-xdist",
        "pytest-trio==0.5.2",
        "tox==3.14.6",
    ],
    'lint': [
        "flake8==3.7.9",
        "isort>=4.2.15,<5",
        "mypy==0.770",
        "pydocstyle>=3.0.0,<4",
    ],
    'doc': [
        "Sphinx>=1.6.5,<2",
        "sphinx_rtd_theme>=0.1.9",
        "towncrier>=19.2.0, <20",
    ],
    'dev': [
        "bumpversion>=0.5.3,<1",
        "pytest-watch>=4.1.0,<5",
        "wheel",
        "twine",
        "ipython",
    ],
    'factory': [
        "factory-boy==2.12.0",
    ],
}

extras_require['dev'] = (
    extras_require['dev'] +  # noqa: W504
    extras_require['test'] +  # noqa: W504
    extras_require['lint'] +  # noqa: W504
    extras_require['factory'] +  # noqa: W504
    extras_require['doc']
)


with open('./README.md') as readme:
    long_description = readme.read()


setup(
    name='alexandria',
    # *IMPORTANT*: Don't manually change the version here. Use `make bump`, as described in readme
    version='0.1.0-alpha.0',
    description="""alexandria: Client for the Alexandria DHT network""",
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='The Ethereum Foundation',
    author_email='snakecharmers@ethereum.org',
    url='https://github.com/ethereum/alexandria',
    include_package_data=True,
    install_requires=[
        "async-exit-stack==1.0.1",
        "async-generator==1.10",
        "async-service==0.1.0a7",
        "coincurve==10.0.0",
        "cryptography>=2.9,<3",
        "eth-keys==0.2.4",
        "eth-utils>=1,<2",
        "netifaces==0.10.9",
        "plyvel==1.2.0",
        "psutil==5.7.0",
        "pyformance==0.4",
        "ssz==0.2.4",
        "trio==0.13.0",
        "trio-typing==0.3.0",
        "uPnPClient==0.0.8",
        "web3==5.7.0",
    ],
    python_requires='>=3.6, <4',
    extras_require=extras_require,
    py_modules=['alexandria'],
    license="MIT",
    zip_safe=False,
    keywords='ethereum',
    packages=find_packages(exclude=["tests", "tests.*"]),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
    entry_points={
        'console_scripts': [
            'alexandria=alexandria._boot:_boot',
        ],
    },
)
