#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os

# Project path
PRJ_DIR = '/'.join(os.path.abspath(__file__).split('/')[:-2])

# Path where to store data
DATA_DIR = os.path.join(PRJ_DIR, 'data')

# Path with maps
MAPS_DIR = os.path.join(PRJ_DIR, 'maps')

# Path figures
FIGURES_DIR = os.path.join(PRJ_DIR, 'reports/figures')


# Flag used to silence prints
VERBOSE = True

# Number of jobs
N_JOBS = 8
