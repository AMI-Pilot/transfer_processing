import fnmatch
from pymongo import ASCENDING, DESCENDING
from .package import Package

class PackageFactory:
    def __init__(self, ami):
        self.ami = ami
        self.db = ami.get_db()

    def ids(self, pattern="*"):
        "Return package ids that match the pattern"
        regex = fnmatch.translate(pattern)
        res = self.db.packages.find({'id': {'$regex': regex}},
                                    {'id': 1})        
        return res.distinct("id")

    def package_exists(self, pkgid):
        "Check if a package id exists"
        return True if self.package_timestamps(pkgid) else False

    def package_timestamps(self, pkgid):
        "Get the different timestamp values for this package id"
        res = self.db.packages.find({'id': pkgid}, {'timestamp': 1})
        return res.distinct("timestamp")

    def get_package(self, pkgid, timestamp=None):
        " Fetch a package object with the latest timestamp, unless specified"
        query = {'id': pkgid}
        if timestamp:
            query['timestamp'] = timestamp
        else:
            pass
        res = self.db.packages.find(query, {'_id': 1}).sort('timestamp', DESCENDING).limit(1)
        if res:            
            return Package(self.db, res[0]['_id'])
        else:
            raise KeyError("No package with those specs")

    def packages_by_state(self, state):
        "Grab all of the (latest) packages with a given state"
        if state not in Package.states:
            raise ValueError("Invalid state")

        # This is moderately complex:
        # * sort by reverse timestamp (to put the latest version of packages earlier)
        # * group by package id, collecting the original _id and the state from the first (only!)
        # * push the minidocument to the root
        # * filter for the state we're looking for
        res = self.db.packages.aggregate([
            {'$sort': {'timestamp': -1}},
            {'$group': {
                '_id': '$id',
                'doc': {'$first': {'_id': '$_id', 'state': '$state'}},
            }},
            {'$replaceRoot': {'newRoot': '$doc'}},
            {'$match': {'state': state}}
        ])
        return [Package(self.db, x['_id']) for x in res]


