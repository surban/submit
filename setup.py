from setuptools import setup, find_packages
setup(
    name = "submit",
    description = "submit SLURM jobs",
    author = "Sebastian Urban",
    author_email = "surban@tum.de",
    version = "0.1",
    packages = ['submit'],
    entry_points = {'console_scripts': [ 'submit = submit.main:run' ] },
)
