#!/usr/bin/env python3

import _preamble
from ami import Ami
from ami.package import Package
from ami.package_factory import PackageFactory
import logging

ami = Ami()

def main():
    
    db = ami.get_db()
    #pkg = Package.create(db, "test_pkg")
    #print(pkg)

    pkgf = PackageFactory(ami)
    print(pkgf.ids())
    print(pkgf.package_exists("asdf"), pkgf.package_exists("test_pkg"))
    print(pkgf.package_timestamps("test_pkg"))
    latest = pkgf.get_package("test_pkg")
    middle = pkgf.get_package("test_pkg", timestamp="20210526-135332")
    #latest.set_state("finished")

    print("finished", pkgf.packages_by_state("finished"))
    print("transferred", pkgf.packages_by_state("transferred"))
    print("processed", pkgf.packages_by_state("processed"))
    logging.debug("Hello world")
    logging.info("goodbye")


if __name__ == "__main__":
    main()