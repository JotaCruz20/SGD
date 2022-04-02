#!/bin/bash
sqlite3 -batch tcp-hKeys.db ".import '/mnt/d/SGD/Tcp-postgres/customer.tbl' customer"