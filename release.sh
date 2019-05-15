#!/bin/bash

if [ $# -eq 0 ]; then
    echo "No version specified, exiting"
    exit 1
fi

# Set version to release
sed -i "s/^__version__ = .*/__version__ = \"$1\"/" dbsink/__init__.py
sed -i "s/version: .*/version: \"$1\"/" conda-recipe/meta.yaml
#sed -i "s/version = .*/version = \"$1\"/" docs/conf.py
#sed -i "s/release = .*/release = \"$1\"/" docs/conf.py
echo $1 > VERSION

# Commit release
git add dbsink/__init__.py
git add conda-recipe/meta.yaml
git add VERSION
#git add docs/conf.py
git commit -m "Release $1"

# Tag
git tag $1

# Push to Git
echo "If everything looks correct, run:
    git push --tags origin master
"

echo "If CI is successfully, upload to PyPI:
    rm -r dist/
    python setup.py sdist bdist_wheel
    twine upload --verbose --repository-url https://upload.pypi.org/legacy/ dist/*
"
