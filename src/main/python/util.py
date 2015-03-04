import os
import shutil
import re
import logging
import errno
import sys
import subprocess
import time

def timestamp_string():
  return time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())

def mkdir_p(dst):
  try:
    os.makedirs(dst)
  except OSError as exc:
    if exc.errno == errno.EEXIST and os.path.isdir(dst):
      pass
    else:
      raise

def rm_rf(dst):
  try:
    if os.path.exists(dst):
      shutil.rmtree(dst)
  except OSError:
    pass

def create_clean_dir(results_dir):
  if os.path.exists(results_dir):
    print >> sys.stderr, "Results dir %s already exists. Overwriting.." % results_dir
  rm_rf(results_dir)
  mkdir_p(results_dir)

def find(f, seq):
  """Return first item in sequence where f(item) == True."""
  for item in seq:
    if f(item):
      return item

def backtick(cmd, *args, **kwargs):
  return subprocess.Popen(cmd, *args, shell=True, stdout=subprocess.PIPE, **kwargs).stdout.read().strip()

def system(cmd, *args, **kwargs):
  return subprocess.call(cmd, *args, shell=True, **kwargs)
