'''Vioozer Matching engine package.
Adam Matan, Shahar Asher, 2014-12-22'''

from setuptools import setup, find_packages

setup(name='rabbitcoat',
      version='0.1',
      description='a simple wrapper suit for RabbitMQs main functionalities',
      url='https://github.com/picotera/rabbit-coat',
      author='Adam Lev-Libfeld, Koby Bas',
      author_email='adam@tamarlabs.com',
      license='Apache License Version 2.0',
      packages=find_packages(),
      include_package_data=True,
      install_requires=[
          'pika'
      ],
      zip_safe=False)


