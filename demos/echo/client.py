#!/usr/bin/env python3
import rpyc_async as rpyc


def echo_once():
    conn = rpyc.connect("localhost", 18861)
    conn.root.echo("Echo")
    conn.close()


if __name__ == "__main__":
    echo_once()
