#!/usr/bin/env python
# coding: utf8

"""
    This is a source separation library with pretrained models.
    The library is based on Tensorflow:

    -   It provides already trained model for performing separation.

"""

# expose modules(scripts) of this package
from airflow.operators.tfserving.audio.source_separation.scripts import run_source_separation
