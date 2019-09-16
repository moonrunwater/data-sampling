#!/usr/bin/python3
# -*- coding: utf-8 -*-


def test_mysql_concat(*args):
    print("MD5(CONCAT(%s))" % ','.join([ "IFNULL(`%s`,'')" % name for name in args]))

def test_pgsql_concat(*args):
    print("md5(concat(" + ",".join([ '"%s"' % name for name in args]) + "))")

def test_slice():
    tup = (0, 1, 2, 3)
    tup = (0, 1)
    print(tup)
    print(tup[:])
    print(tup[0:])
    print(tup[0:-1])
    print(tup[0:-2])

    test_print(tup[0:-2])
    test_print(*tup[0:-2])

def test_print(*args):
    print('args type:', type(args))
    print(args)
    print(*args)

def test_dict_list():
    dct = {
        'id': 1,
        'name': 'huohu',
    }
    print(dct)

    # dct.update(None)
    [1, 2].extend(None)

if __name__ == "__main__":
    test_mysql_concat('artisan_id', 'user_id', 'source_type', 'product_id', 'category')
    test_pgsql_concat("id", "sso_userid", "name", "email", "inserted_at")
    test_slice()
    test_dict_list()