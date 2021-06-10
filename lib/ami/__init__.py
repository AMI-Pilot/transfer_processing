#!/usr/bin/env python3
"""
This is the ami module which is contains the bits that are used by all
libraries and binaries
"""
import yaml
from pathlib import Path
import sys
import logging.config
import logging
import logging.handlers
import fcntl
from pymongo import MongoClient
import os

class Ami:
    def __init__(self, application=None):
        if application is None:
            application = Path(sys.argv[0]).stem
        self.application = application

        # set up and load configuration stuff
        self.root = Path(sys.path[0], "..").resolve()
        self.config_path = Path(self.root, "etc")
        with open(self.config_path.joinpath("ami.conf")) as f:
            self.config = yaml.safe_load(f)        

        # configure standard logging
        logging.config.dictConfig(self.config['logging'])

        # set up the uncaught exception handler
        sys.excepthook = Ami.uncaught_exception

        # Things that will be filled in later.
        sys.db = None


    @staticmethod
    def uncaught_exception(exc_type, exc_value, exc_traceback):
        "Handle uncaught exceptions in a sane fashion"
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
        logging.critical("Uncaught Exception", exc_info=(exc_type, exc_value, exc_traceback))


    def get_db(self):
        "Get a DB object that is fully configured"
        # intialize a new db connection if we don't have one or
        # the PID is different from the one we already have
        if sys.db is None or sys.db[0] != os.getpid():
            mdb = MongoClient(**self.config['mongodb']['connection'])
            mdb = mdb.get_database(self.config['mongodb']['database'])            
            sys.db = (os.getpid(), mdb)
        return sys.db[1]

    def get_directory(self, name):
        "Get a directory path object from the config file 'directories' section"
        return Path(self.resolve_path(self.config['directories'][name]))


    def resolve_path(self, path):
        "Resolve a path relative to the install"
        return Path(self.root, path)

    def get_config(self):
        "Return the configuration blob for an application"
        return self.config['apps'].get(self.application, {})

    def get_application(self):
        "Get the current application name"
        return self.application



class ConsoleHandler(logging.StreamHandler):
    "Only pass logging messages through if the handle is a tty"
    def __init__(self):
        logging.StreamHandler.__init__(self)

    def emit(self, record):        
        if self.stream.isatty():
            super().emit(record)


class TimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
    "Multi-process locking file handler"
    def __init__(self, *args, **kwargs):
        if not Path(kwargs['filename']).is_absolute():
            # if it isn't absolute, rebase it into the logs directory
            kwargs['filename'] = Path(sys.path[0], '../logs', kwargs['filename']).resolve()
        logging.handlers.TimedRotatingFileHandler.__init__(self, *args, **kwargs)
        
    def emit(self, record):
        # copy the stream for locking/unlocking in case we rolled 
        x = self.stream
        fcntl.lockf(x, fcntl.LOCK_EX)
        super().emit(record)
        if not x.closed:        
            fcntl.lockf(x, fcntl.LOCK_UN)



