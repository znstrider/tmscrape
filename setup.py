import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="tmscrape",
    version="0.0.1",
    author="znstrider",
    author_email="mindfulstrider@gmail.com",
    description="scrapers for transfermarkt.de",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/znstrider/tmscrape",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License"
    ],
    python_requires='>=3.6',
)
