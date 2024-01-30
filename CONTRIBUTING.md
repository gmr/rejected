# Contributing

To get setup in the environment and run the tests, take the following steps:

```sh
python3 -m venv env
source env/bin/activate
pip install -e '.[testing]'

flake8
coverage run && coverage report
```

Please format your code contributions with the ``yapf`` formatter:

```sh
yapf -i --recursive --style=pep8 rejected
```

## Test Coverage

Pull requests that make changes or additions that are not covered by tests
will likely be closed without review.
