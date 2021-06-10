import fnmatch
from pymongo import ASCENDING, DESCENDING
from .package import Package
import logging

logger = logging.getLogger()

class PackageFactory:
    def __init__(self, ami):
        self.ami = ami
        self.db = ami.get_db()

    def ids(self, pattern="*"):
        "Return package ids that match the pattern"
        regex = fnmatch.translate(pattern)
        res = self.db.packages.find({'id': {'$regex': "^" + regex + "$"}},
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

    def packages_by_state(self, state, all=False):
        "Grab all of the (latest) packages with a given state"
        if state not in Package.states:
            raise ValueError("Invalid state")

        if not all:
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
        else:
            res = self.db.packages.find({'state': state})
        return [Package(self.db, x['_id']) for x in res]


    def find_packages(self, *packagespec):
        """Find packages matching a package specs:
        * If the spec starts with '.' it is a state search, for the latest timestamp
        * If the spec starts with '+' is is a state search for all timestamps
        * Otherwise, it's a package specification and it follows these rules:
        ** If it contains '/' it is a package_id/timestamp pair.  Both the package_id and timestamp can contain wildcards
        ** if it doesn't contain '/', then it's a package_id (with possible wildcards)
        """
        results = set()
        for spec in packagespec:
            try:
                if spec.startswith('.'):
                    # latest with states
                    results.update(self.packages_by_state(spec[1:]))
                elif spec.startswith('+'):
                    # all with state
                    results.update(self.packages_by_state(spec[1:], all=True))
                else:
                    # object spec
                    if '/' in spec:
                        pkg_id, timestamp = spec.split("/", 1)
                        pregex = "^" + fnmatch.translate(pkg_id) + "$"
                        tregex = "^" + fnmatch.translate(timestamp) + "$"
                        res = self.db.packages.find({'id': {'$regex': pregex},
                                                     'timestamp': {'$regex': tregex}})
                        results.update([Package(self.db, x['_id']) for x in res])
                    else:
                        # plain object (use the same method as in packages_by_state)
                        regex = "^" + fnmatch.translate(spec) + "$"
                        res = self.db.packages.aggregate([
                            {'$match': {'id': {'$regex': regex}}},  # find the ids that match
                            {'$sort': {'timestamp': -1}},  # sort so the newest is first
                            {'$group': {
                                '_id': '$id',
                                'doc': {'$first': {'_id': '$_id'}},
                            }}, # group so only the first package id is kept
                            {'$replaceRoot': {'newRoot': '$doc'}} # return only the _id fields                            
                        ])
                        results.update([Package(self.db, x['_id']) for x in res])

            except Exception as e:
                logger.debug(f"Could not find package for {spec}: {e}")
                
        # filter the results so there's only one copy of each package object
        filtered = {}
        for x in results:
            filtered[x.data['_id']] = x
            
        return filtered.values()