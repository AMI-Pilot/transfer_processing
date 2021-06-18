from datetime import datetime
from pymongo.database import Database
import logging
import time
import traceback
from ami import Ami

class Package:
    states = {
        'transferred': False,
        'validating': False,
        'validation_failed': True,
        'shaping': False,
        'accepted': True,
        'local_failed': True,
        'processing': False,
        'processed': True,
        'processing_failed': True,
        'storing': False,
        'sda_soft_failed': False,
        'distributing': False,
        'distributed': True,
        'sda_hard_failed': True,
        'submitting': False,
        'hcp_soft_failed': False,
        'dist_waiting': False,
        'hcp_hard_failed': True,
        'finished': True,
        'dist_soft_failed': False,
        'dist_hard_failed': False,
        'cleaning': False,
        'to_delete': True,
        'deleted': False,
    }

    @staticmethod
    def create(ami, pkgid, state="transferred"):
        "Create the a new package in the database"
        if state not in Package.states:
            raise ValueError("Invalid state")
        
        data = {
            '_version': 1,
            'id': pkgid,
            'timestamp': datetime.now().strftime("%Y%m%d-%H%M%S"),
            'state': state,
            'state_change': time.time(),
            'log': [],
            'app_data': {},
            'sda_location': None,
        }

        res = ami.get_db().packages.insert_one(data)        
        _id = res.inserted_id
        p = Package(ami, _id)
        p.log("info", "Package initialized")
        return p


    def __init__(self, ami:Ami, _id):
        "Reconstitute a package in mongodb into a python object"
        self.ami = ami
        self.db = ami.get_db()
        self.data = self.db.packages.find_one({'_id': _id})        
        if self.data is None:
            raise KeyError("No package with that id")
        
        # Add any database version migrations here
        # * check for version less than migration version
        # * update the document accordingly
        # * self.__init__(db, _id)  to refresh from the DB
        # Repeat for newer versions

        
    def __str__(self):
        return str(self.data)

    __repr__ = __str__


    def get_id(self):
        "Return the package id"
        return self.data['id']

    def set_state(self, state, external=False):
        "Set the package state"
        if state not in Package.states:
            raise ValueError("Invalid state")
        if external and not Package.states[state]:
            raise ValueError("Cannot change to this state externally")
                
        oldstate = self.data['state']
        if oldstate != state:
            self.data['state'] = state
            self.db.packages.update_one({'_id': self.data['_id']}, 
                                        {'$set': {'state': state, 'state_change': time.time()}})
            self.log('info', f"State changed from {oldstate} to {self.data['state']}")

    def get_state(self):
        "Get the package state"
        return self.data['state']

    def get_state_change(self):
        "Get the time when the state was changed"
        return self.data['state_change']


    def get_logs(self):
        "Return the logs for the package"
        return self.data['logs']

    def log(self, severity, message, exception=False):
        "Add to the package log"
        if severity.lower() not in ['info', 'warn', 'error', 'debug']:
            raise ValueError("Invalid log severity")
        # If an exception was passed, push a backtrace into the debug log.
        if exception:
            logging.debug(None, exc_info=True)

        logging.log(logging.getLevelName(severity.upper()), f"{self.get_id()}/{self.get_timestamp()}: {message}")
        msg = {'time': datetime.now().strftime("%Y%m%d-%H%M%S"),
               'severity': severity,
               'message': message}               
        self.db.packages.update_one({'_id': self.data['_id']},
                                    {'$push': {'log': msg}})
        self.data['log'].append(msg)
        

    def reset(self):
        "Reset the object to accepted and clear out data"
        self.db.packages.update_one({'_id': self.data['_id']},
                                    {'$set': {'state': 'accepted', 
                                              'state_changed': time.time(), 
                                              'log': [],
                                              'app_data': {}}})
        self.log('info', "Object has been reset to its initial state")

    def get_timestamp(self):
        "Return the object timestamp"
        return self.data['timestamp']

    def get_dirname(self):
        "Return the directory name used for the package"
        return self.get_id() + "_" + self.get_timestamp()

    def get_app_data(self, key, default=None, appname=None):
        "Get stored application-specific data for this object"
        if appname is None:
            appname = self.ami.get_application()
        return self.data['app_data'].get(appname, {}).get(key, default)

    def set_app_data(self, key, data, appname=None):
        "Set the stored application-specific data for this object"
        if appname is None:
            appname = self.ami.get_application()
        if appname not in self.data['app_data']:            
            self.data['app_data'][appname] = {}
            self.db.packages.update_one({'_id': self.data['_id']},
                                        {'$set': {'app_data.' + appname: {}}})
        

        self.data['app_data'][appname][key] = data

        self.db.packages.update_one({'_id': self.data['_id']},
                                    {'$set': {'app_data.' + appname + "." + key: data}})

                                    
    def get_sda_location(self):
        "Get the root path for the object on SDA"
        return self.data['sda_location']

    def set_sda_location(self, location):
        "Set the root path for the object on SDA"
        self.data['sda_location'] = location
        self.db.packages.update_one({'_id': self.data['_id']},
                                    {'$set': {'sda_location': location}})



