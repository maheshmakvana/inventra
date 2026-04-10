from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="inventra",
    version="1.0.1",
    author="",
    description="Multi-channel inventory sync for small and mid-market eCommerce — real-time conflict resolution, async sync, fraud-safe audit log",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/inventra-py/inventra",
    packages=find_packages(exclude=["tests*"]),
    python_requires=">=3.8",
    install_requires=[
        "pydantic>=2.0",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Office/Business :: Financial",
        "Intended Audience :: Developers",
    ],
    keywords=[
        "inventory sync", "multi-channel inventory", "shopify inventory",
        "amazon inventory sync", "ecommerce inventory management",
        "inventory conflict resolution", "real-time inventory",
        "stockout prevention", "inventory automation python",
        "multichannel ecommerce sync",
    ],
)
