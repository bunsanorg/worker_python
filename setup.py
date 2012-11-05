from distutils.core import setup

setup(
    name='bunsan::worker',
    version='0.0',
    description = 'bunsan worker python implementation',
    author = 'Aleksey Filippov',
    author_email = 'sarum9in@gmail.com',
    url = 'https://github.com/sarum9in/bunsan_worker_python',
    packages = ['bunsan.worker'],
    package_dir = {'': 'src'}
)
