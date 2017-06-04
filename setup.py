from setuptools import setup

setup(
    name="RarbgSubscriber",
    version="0.1",
    author="verybada",
    author_email="verybada.lin@gmail.com",
    description=("A tool to subscriber RARBG.to"),
    keywords="rarbg",
    packages=['rarbgsubscriber'],
    install_requires=[
        'rarbgapi'
    ],
    extras_require={
        'travis': ['pep8', 'pylint']
    },
    entry_points={
        'console_scripts': [
            'rarbg=rarbgsubscirber.rarbgdaemon:main'
        ]
    }
)
