all: checkall

checkall: pep8 pylint

pep8:
	@pep8 rarbgsubscriber > pep8.log

pylint:
	@pylint rarbgsubscriber -f parseable > pylint.out
