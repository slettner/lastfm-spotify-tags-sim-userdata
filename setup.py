from setuptools import find_packages, setup


def read_requirements(filename: str):
    return [line for line in open(filename).readlines() if not line.startswith("--")]


setup(
    name="lastfm_dataset",
    python_requires=">=3.8",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    include_package_data=True,
    version="0.1.0.dev1",
    description="lastfm_dataset",
    url="https://github.com/slettner/lastfm-spotify-tags-sim-userdata",
    install_requires=read_requirements("requirements.txt"),
    author="Sebastian Lettner",
)
