
help:
	@echo "clean - remove all build, test, coverage and Python artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "clean-test - remove test and coverage artifacts"
	@echo "build - package"

all: clean build

default: clean build

clean: clean-build clean-pyc

clean-build:
	rm -fr dist/

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

build: clean
	mkdir ./dist
	cp ./src/main.py ./dist
	cp ./script/Execute* ./dist
	chmod +x ./dist/Execute*
	cd ./src/modules.zip && zip -r ../../dist/modules.zip ./*