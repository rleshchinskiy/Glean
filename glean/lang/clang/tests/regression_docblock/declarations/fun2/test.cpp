// Copyright (c) Facebook, Inc. and its affiliates.

/**
 * @testname declarations/fun2
 * @type struct Foo
 */
struct Foo {
  /**
   * @testname declarations/fun2
   * @type Foo::Foo
   */
  Foo();
  /**
   * @testname declarations/fun2
   * @type Foo::Foo()
   */
  Foo(const Foo&);
  /**
   * @testname declarations/fun2
   * @type Foo::~Foo
   */
  ~Foo();

  /**
   * @testname declarations/fun2
   * @type operator bool
   */
  operator bool();
  /**
   * @testname declarations/fun2
   * @type operator const unsigned int *
   */
  operator const unsigned int *();
  /**
   * @testname declarations/fun2
   * @type void operator=
   */
  void operator=(Foo&);
  /**
   * @testname declarations/fun2
   * @type void operator+
   */
  void operator+(Foo&);
  /**
   * @testname declarations/fun2
   * @type void operator--
   */
  void operator--();
};

/**
 * @testname declarations/fun2
 * @type bool operator ""_foo
 */
bool operator ""_foo(const char *);
