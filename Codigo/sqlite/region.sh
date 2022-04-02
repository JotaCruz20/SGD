#!/bin/bash
sqlite3 -batch tcp-hKeys.db ".import '/mnt/d/sgd/Tcp-postgres/region.tbl' region"