#ifndef FOO
#define FOO
#include <stdio.h>
#include <iostream>
#include <cassandra.h>

class Foo
{
	virtual void _query();

public:

	Foo()
	{
	}

	virtual ~Foo()
	{
	}

	void query()
	{
		_query();
	}
};
#endif
