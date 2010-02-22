from setuptools import setup, find_packages
 
setup(
    name='django-background-task',
    version=__import__('background_task').__version__,
    description='Database backed asynchronous task queue',
    long_description=open('README.rst').read(),
    author='John Montgomery',
    author_email='john@littlespikeyland.com',
    url='http://github.com/lilspikey/django-background-task',
    download_url='http://github.com/lilspikey/django-background-task/downloads',
    license='BSD',
    packages=find_packages(exclude=['ez_setup']),
    include_package_data=True,
    zip_safe=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)