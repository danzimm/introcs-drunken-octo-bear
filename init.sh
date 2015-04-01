#!/usr/bin/env bash

CDIR=`pwd`

if [ ! -e "hmwk.scala" ]; then
  echo "You must be in the base directory of the repo in order to run this"
fi

git submodule update --init --recursive
ln -s datafiles/*.data .
ln -s datafiles/students_*.txt .
ln -s datafiles/categories_*.txt .

