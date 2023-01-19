# Which Version of the project we will releae/publish
from setuptools import find_packages,setup
from typing import List

requirements_file_name="requirements.txt"
REMOVE_PACKAGE="-e ."

def get_requirements()->List[str]:
    with open(requirements_file_name) as requirement_file:
        requirement_list = requirement_file.readline()
    requirement_list = [requirement_name.replace("\n"," ") for requirement_name in requirement_list]
    if REMOVE_PACKAGE in requirement_list:
        requirement_list.remove(REMOVE_PACKAGE)
    return requirement_list

setup(
    name='Insurance',
      version='1.0',
      description='Insurance Project',
      author='Rais Jalia',
      author_email='raisjaliarj@outlook.com',
      packages=find_packages(),
      install_reqires=get_requirements()
)