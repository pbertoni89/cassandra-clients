#ifndef EXTFOO
#define EXTFOO
#include "Foo.cpp"

class ExtFoo : public Foo
{
	void _query()
	{
	}

public:
	ExtFoo(): Foo()
	{
	}
};
#endif
