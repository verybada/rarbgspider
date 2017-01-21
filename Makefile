all: checkall

checkall: pep8 pylint

pep8:
	pep8 rarbgsubscriber |tee pep8.log

pylint:
	pylint rarbgsubscriber -f parseable |tee pylint.out
