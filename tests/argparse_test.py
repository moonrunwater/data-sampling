#!/usr/bin/python3
# -*- coding: utf-8 -*-

import argparse


# python argparse_test.py nihao 99 -v 2
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("echo", help="echo is here~")
    parser.add_argument("square", type=int, help = "To sqaure the number given")
    parser.add_argument("-v", "--verbosity", required=True, type=int,
                        choices=[0, 1, 2], help="increase output verbosity")

    args = parser.parse_args()

    print(args)
    print(args.echo)
    print(args.square)
    print(args.verbosity)