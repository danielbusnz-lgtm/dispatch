from setuptools import setup, find_packages

setup(
    name="dispatch-queue",
    version="0.1.0",
    packages=find_packages(),
    entry_points={"console_scripts": ["dispatch=dispatch.cli:main"]},
    python_requires=">=3.9",
    description="Lightweight Python task queue backed by SQLite. No Redis required.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Daniel Brooks",
    license="MIT",
)
