from datetime import datetime
from pymongo.database import Database
import logging

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
    def create(db, pkgid, state="transferred"):
        "Create the a new package in the database"
        if state not in Package.states:
            raise ValueError("Invalid state")
        
        data = {
            'version': 2,
            'id': pkgid,
            'timestamp': datetime.now().strftime("%Y%m%d-%H%M%S"),
            'state': state,
            'log': [],
            'app_data': {}

        }

        res = db.packages.insert_one(data)        
        _id = res.inserted_id
        p = Package(db, _id)
        p.log("info", "Package initialized")
        return p


    def __init__(self, db: Database, _id):
        "Reconstitute a package in mongodb into a python object"
        self.db = db
        self.data = self.db.packages.find_one({'_id': _id})        
        if self.data is None:
            raise KeyError("No package with that id")
        
        # Migrate to version 2
        if self.data['version'] < 2:
            self.db.packages.update_one({'_id': _id}, {'$set': {'version': 2, 'app_data': {}}})
            self.__init__(db, _id)  # get it again from the DB
        
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
                                        {'$set': {'state': state}})
            self.log('info', f"State changed from {oldstate} to {self.data['state']}")

    def get_state(self):
        "Get the package state"
        return self.data['state']

    def get_logs(self):
        "Return the logs for the package"
        return self.data['logs']

    def log(self, severity, message):
        "Add to the package log"
        if severity.lower() not in ['info', 'warn', 'error', 'debug']:
            raise ValueError("Invalid log severity")
        
        logging.log(logging.getLevelName(severity.upper()), f"{self.get_id()}({self.get_timestamp()}): {message}")
        msg = {'time': datetime.now().strftime("%Y%m%d-%H%M%S"),
               'severity': severity,
               'message': message}               
        self.db.packages.update_one({'_id': self.data['_id']},
                                    {'$push': {'log': msg}})
        self.data['log'].append(msg)
        
    def get_timestamp(self):
        "Return the object timestamp"
        return self.data['timestamp']

    def get_dirname(self):
        "Return the directory name used for the package"
        return self.get_id() + "_" + self.get_timestamp()

    def get_app_data(self, appname, key):
        "Get stored application-specific data for this object"
        return self.data['app_data'].get(appname, {}).get(key, None)

    def set_app_data(self, appname, key, data):
        "Set the stored application-specific data for this object"
        if appname not in self.data['app_data']:
            self.data['app_data'][appname] = {}
            self.db.packages.update_one({'_id': self.data['_id']},
                                        {'$set': {'app_data.' + appname: {}}})
        
        self.data['app_data'][appname][key] = data
        self.db.packages.update_one({'_id': self.data['_id']},
                                    {'$set': {'app_data.' + appname + "." + key: data}})

                                    