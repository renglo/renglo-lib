"""
Tank Library - Core controllers, models, and utilities for Tank applications
"""

from setuptools import setup, find_packages

setup(
    name="tank-lib",
    version="1.0.0",
    description="Core Tank functionality - controllers, models, and utilities",
    author="Tank Team",
    packages=find_packages(),
    python_requires=">=3.12",
    install_requires=[
        "boto3==1.35.38",
        "botocore==1.35.38",
        "Flask==3.1.0",
        "Flask-Cognito==1.21",
        "Flask-Caching==2.1.0",
        "PyJWT==2.10.1",
        "Requests==2.32.3",
        "validate_email==1.3",
        "cryptography==38.0.4",
        "openai==1.65.2",
    ],
    include_package_data=True,
    package_data={
        'tank': [
            'app_chat/blueprints/*.json',
        ],
    },
)

