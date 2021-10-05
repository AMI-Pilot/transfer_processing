import os
import stat
import subprocess
import time
import re
from dataclasses import dataclass
from datetime import datetime
import signal
import logging

logger = logging.getLogger()

class HSIError(Exception):
    """An HSI error of some sort"""

    def __init__(self, message):
        super(HSIError, self).__init__()
        self.message = message


class HSICore:
    """
    This is the core of the HSI wrapper.

    It should be fork-safe.
    """

    def __init__(self, initDir, hsiBinary="/usr/local/bin/hsi",
                 keyTab=os.environ['HOME'] + "/.hsi.keytab",
                 userName=os.environ['USER']):
        self.initDir = initDir
        self.hsiBinary = hsiBinary
        if not os.path.exists(hsiBinary):
            raise ValueError(f"HSI binary '{hsiBinary}' doesn't exist")
        self.keyTab = keyTab
        if not os.path.exists(keyTab):
            raise ValueError(f"Keytab '{keyTab}' doesn't exist")
        self.userName = userName
        self.connection = None
        self.pid = os.getpid()


    def ping(self):
        """
        Ping the server to see if it is accepting connections
        """
        result = subprocess.run([self.hsiBinary, "-P", "-A", "keytab", "-k",
                        self.keyTab, "-l", self.userName, "pwd"], capture_output=True, encoding='utf-8')
        return result.returncode == 0 and str(result.stdout).startswith("pwd0")

    def run_command(self, command, cos=None):
        """
        Issue an HSI command and capture the capture_output

        command:  a list representing the command
        cos:  optional class-of-service to use for the command
        """
        if self.pid != os.getpid():
            self.connection = None

        if self.connection is not None:
            rc = self.connection.poll()
            if rc is not None:
                logger.warn(f"HSI process exited unexpectedly with return code {rc}")
                self.connection = None

        if self.connection is None:
            while self.connection is None:
                hsicmd = [self.hsiBinary, "-P",
                          "-A", "keytab", 
                          "-k", str(self.keyTab), 
                          "-l", self.userName]
                logger.debug(f"HSI command: {hsicmd}")
                self.connection = subprocess.Popen(hsicmd,
                                                   stdin=subprocess.PIPE,
                                                   stdout=subprocess.PIPE,
                                                   stderr=subprocess.STDOUT,
                                                   encoding="utf-8")
                logger.debug(self.connection)
                if self.connection.poll() is not None:
                    logger.warn("Cannot connect to HSI.  Will retry in 30 seconds")
                    time.sleep(30)
                    self.connection = None
                else:
                    self.pid = os.getpid()
                    print("pwd;lpwd;glob;idletime -1;id",
                          file=self.connection.stdin)
                    self.connection.stdin.flush()
                    self.hpssRoot = self.connection.stdout.readline().rstrip()[6:]
                    localPwd = self.connection.stdout.readline()
                    self.connection.stdout.readline()
                    self.connection.stdout.readline()
                    self.connection.stdout.readline()
                    sentinel = self.connection.stdout.readline().rstrip()
                    logger.debug(f"HSI local pwd: {localPwd}, sentinel: {sentinel}")
                    self.sentinel = sentinel

        cmd = " ".join(command)
        if cos is not None:
            cmd += f" cos={cos}"
        cmd += "; id"
        logger.debug(f"Running HSI command: {cmd}")
        print(cmd, file=self.connection.stdin)
        self.connection.stdin.flush()
        lines = []
        line = None
        exception = None
        while line != self.sentinel:
            line = self.connection.stdout.readline().rstrip()
            logger.debug(f"Got line from HPSS: {line}")
            # Errors will begin with stars.  Some we care about, Some
            # we don't.  In particular, things relating to files not
            # found, staging levels not populated, etc.  everything
            # else will throw an HSIError
            # TODO:  I'm not really happy with the error handling here.  It also
            # doesn't catch connection failures...
            if line.startswith("***"):
                if exception is None:
                    exception = line
                elif exception is not None and line != self.sentinel:
                    exception += "\n" + line
            if line != self.sentinel:
                lines.append(line)
        if exception is not None:
            # scan for errors we don't care about:
            for e in [r"getFile: no valid checksum for",
                      r"no data at hierarchy level",
                      r"ls:.+HPSS_ENOENT",
                      r"Background stage failed with error -5",
                      "setting nameserver attributes.+HPSS_EACCES",
                      "stage: No such file or directory"]:
                if re.search(e, exception):
                    # we don't care, but we don't want to return anything
                    lines = []
                    break
            else:
                raise HSIError(exception + "CMD: " + cmd)
        return lines

    def clean_path(self, path):
        """
        Remove ., .., and // entries from a path
        """
        newPath = []
        for n in path.split("/"):
            if n in ('.', ''):
                continue
            elif n == "..":
                if len(newPath) > 1:
                    newPath.pop()
            else:
                newPath.append(n)
        return "/" + "/".join(newPath)

    def abs_path(self, path):
        """
        Convert a path to an HPSS absolute path
        """
        return self.hpssRoot + "/" + self.initDir + "/" + self.clean_path(path)

    def _mode2int(self, mode):
        result = 0
        for c in mode[1:]:
            result <<= 1
            if c != '-':
                result += 1
        return result

    @dataclass
    class Stat:
        """
        Hold stat information for an HPSS file
        """
        name: str = None
        type: str = None
        parent: str = None
        mode: int = 0
        nlink: int = 0
        owner: str = None
        group: str = None
        size: int = 0
        level: str = None
        time: float = 0
        storage: list = None
        cos: int = None

        def is_dir(self):
            """ return true if this is a directory """
            return self.type == "dir"

        def is_file(self):
            """ return true if this is a file """
            return self.type == "file"

        def can_read(self):
            """ return true if the owner can read this """
            return self.mode & 0o400 != 0

        def can_write(self):
            """ return true if the owner can write this """
            return self.mode & 0o200 != 0

        def is_migrated(self):
            """
            return true if the 2nd tape level has a copy.  Directories
            will always return true.
            """
            if self.is_dir():
                return True
            
            # pylint: E1136
            return self.storage[1]["bytes"] == self.size and self.storage[2]["bytes"] == self.size

        def tape_info(self, level=1):
            """
            return a list containing the tape, section, and offset.  if
            this is a directory, return None
            """
            if self.is_dir():
                return None
            
            # pylint: E1136
            s = self.storage[level]
            if "tape" in s and "section" in s and "offset" in s:
                return [s["tape"], s["section"], s["offset"]]

            return []

        def on_tape(self):
            """
            return true if there is a tape copy.  Directories always return
            true
            """
            if self.is_dir():
                return True

            # pylint: unsubscriptable-object
            return self.storage[1]["bytes"] == self.size

        def on_disk(self):
            """
            return true if there is a disk copy.  Directories always return
            True
            """
            if self.is_dir():
                return True

            # pylint: unsubscriptable-object
            return self.storage[0]["bytes"] == self.size

    def _parseLS(self, lines):
        """
        Parse an ls listing into a list of stat objects
        """
        result = []
        if len(lines) > 0:
            inStorage = False
            for line in lines:
                #print(f"ParseLS line: {line}")
                if inStorage:
                    file = result[-1]
                    if file.storage is None:
                        file.storage = []
                    if line == "":
                        inStorage = False
                    else:
                        parts = re.search(r"(\d+)\s+\((tape|disk)\)\s+\d+\s+\d+\s+(\d+|\(no data at this level\))",
                                          line)
                        if parts is not None:
                            bytes = parts.group(3)
                            if bytes.startswith("(no data"):
                                bytes = 0
                            else:
                                bytes = int(bytes)
                            file.storage.append({'level': parts.group(1),
                                                 'type': parts.group(2),
                                                 'bytes': bytes})
                            pass
                        else:
                            parts = re.search(r"Pos:\s+(\d+)\+(\d+)\s+PV\s+List:\s+(\S+)", line)
                            if parts is not None:
                                storage = file.storage[-1]
                                storage['tape'] = parts.group(3)
                                storage['section'] = int(parts.group(1))
                                storage['offset'] = int(parts.group(2))
                else:
                    if line[0] == 'S':
                        inStorage = True
                    else:
                        parts = line.split()
                        if parts[0][0] == 'd':
                            dtime = datetime.strptime(" ".join(parts[7:11]), "%b %d %H:%M:%S %Y")
                            result.append(HSICore.Stat(type='file' if parts[0][0] == '-' else 'dir',
                                                       mode=self._mode2int(parts[0][1:]),
                                                       nlink=int(parts[1]),
                                                       owner=parts[2],
                                                       group=parts[3],
                                                       size=int(parts[5]),
                                                       time=dtime.timestamp(),
                                                       name=parts[11].split("/")[-1]))
                        else:
                            dtime = datetime.strptime(" ".join(parts[9:13]), "%b %d %H:%M:%S %Y")
                            result.append(HSICore.Stat(type='file' if parts[0][0] == '-' else 'dir',
                                                       mode=self._mode2int(parts[0][1:]),
                                                       nlink=int(parts[1]),
                                                       owner=parts[2],
                                                       group=parts[3],
                                                       cos=int(parts[4]),
                                                       level=parts[6].lower(),
                                                       size=int(parts[7]),
                                                       time=dtime.timestamp(),
                                                       name=parts[13].split("/")[-1]))
        return result

    def stat(self, path, useMtime=False):
        """
        Get stat information for the given path
        """
        cmd = ["ls", "-aldDNX"]
        if useMtime:
            cmd.append("-Tm")
        cmd.append(self.initDir + self.clean_path(path))
        lines = self.run_command(cmd)
        stats = self._parseLS(lines)
        if not stats:
            return None
        
        return stats[0]

    def exists(self, path):
        """
        Return true if the path exists
        """
        return self.stat(path) is not None

    def readdir(self, path, pattern=None, withDirInfo=False):
        """
        Read a directory.  Returns an empty list if the path doesn't
        exist or if it isn't a directory.  If a pattern is specified, then
        the entries returned will match that regex.  If withDirInfo is true,
        then the list will actually be lists with two fields:  the name and
        a True/False indicating whether or not the name is a directory
        """
        s = self.stat(path)
        if s is None or not s.is_dir():
            return []
        else:
            lines = self.run_command(["ls", "-alNO", self.initDir + self.clean_path(path)])
            results = []
            for line in lines:
                parts = line.split()
                name = parts[-1].split("/")[-1]
                if pattern is not None and not re.match(pattern, name):
                    continue
                if withDirInfo:
                    results.append([name, parts[0][0] == 'd'])
                else:
                    results.append(name)
            return results

    def statdir(self, path, pattern=None):
        """
        Read the contents of a directory, but return stat objects (with
        storage information) instead of just filenames
        """
        s = self.stat(path)
        if s is None or not s.is_dir():
            return []
        else:
            lines = self.run_command(["ls", "-alDNOX", self.initDir + self.clean_path(path)])
            stats = [stat for stat in self._parseLS(lines) if pattern is None or re.match(pattern, stat.name)]
            return stats

    def mkdir(self, path, parents=False):
        """
        Create a new directory, optionally creating the necessary parents
        """
        self.run_command(["mkdir", ("-p" if parents else ""), self.initDir + self.clean_path(path)])

    def rmdir(self, path):
        """
        Remove a directory
        """
        self.run_command(["rmdir", self.initDir + self.clean_path(path)])

    def delete(self, path):
        """
        Remove a file
        """
        self.run_command(["delete", self.initDir + self.clean_path(path)])

    def rename(self, oldName, newName, force=False):
        """
        Rename a file, optionally forcing it
        """
        self.run_command(["mv", ("-f" if force else ""),
                          self.initDir + self.clean_path(oldName),
                          self.initDir + self.clean_path(newName)])

    def chmod(self, mode, path):
        """
        Change the mode of a file.  numeric and symbolic modes are supported
        """
        self.run_command(["chmod", mode, self.initDir + self.clean_path(path)])

    def link(self, source, dest):
        """
        Hard link files
        """
        self.run_command(["ln",
                          self.initDir + self.clean_path(source),
                          self.initDir + self.clean_path(dest)])

    def annotate(self, path, annotation):
        """
        Add annotation to path
        """
        re.sub(r'"', "'", annotation)
        self.run_command(["annotate", "-A", f"\"{annotation}\"",
                          self.initDir + self.clean_path(path)])

    def get_annotation(self, path):
        """
        Get the annotation for a path
        """
        lines = self.run_command(["ls", "-Ad", self.initDir + self.clean_path(path)])
        p = re.compile(r"Annotation:\s+(.+)")
        for line in lines:
            match = p.match(line)
            if match is not None:
                anno = match.group(1).rstrip()
                return anno
        return None

    def du(self, path):
        """
        Get the disk usage (in bytes) for the given path.
        """
        lines = self.run_command(["du", "-n", "-s", self.initDir + self.clean_path(path)])
        parts = lines[2].split()
        return int(parts[0])

    def get(self, rpath, lpath):
        """
        Retrieve a file from HPSS.  If the remote path is a directory, then
        it will be recursively retrieved into the local directory.  If the
        remote path is a file, then if the local path is a directory, it Will
        be placed there, otherwise it will replace the local path file
        """
        rstat = self.stat(rpath)
        if rstat.is_dir():
            if os.path.isdir(lpath):
                self.run_command(["lcd", lpath])
                self.run_command(["get", "-R", "-c", "on", self.initDir + self.clean_path(rpath)])
            else:
                raise ValueError(f"Local path '{lpath}' is not a directory")
        else:
            if os.path.isdir(lpath):
                self.run_command(["lcd", lpath])
                self.run_command(["get", "-c", "on", self.initDir + self.clean_path(rpath)])
            else:
                self.run_command(["get", "-c", "on", lpath, ":", self.initDir + self.clean_path(rpath)])

    def get_pipe(self, rpath, pipename):
        """
        Retrieve a file to the named local fifo
        """
        self.run_command(["get", "-c", "on", pipename, ":", self.initDir + self.clean_path(rpath)])

    def put(self, lpath, rpath, cos=None):
        """
        Put a local file onto HPSS.  If the local file is a directory, it will be pushed
        recursively.
        """
        if os.path.isdir(lpath):
            self.run_command(["put", "-c", "on", "-H", "md5", "-R", lpath, ":",
                              self.initDir + self.clean_path(rpath)], cos=cos)
        else:
            self.run_command(["put", "-c", "on", "-H", "md5", lpath, ":",
                              self.initDir + self.clean_path(rpath)], cos=cos)

    def put_pipe(self, pipename, rpath, cos=None):
        """
        Send a file from a local fifo into HPSS
        """
        if not os.path.exists(pipename) or not stat.S_ISFIFO(os.stat(pipename).st_mode):
            raise ValueError(f"{pipename} is not a fifo")
        self.run_command(["put", "-c", "on", "-H", "md5", f"\"| cat {pipename}\"", ":",
                          self.initDir + self.clean_path(rpath)], cos=cos)

    def stage(self, path):
        """
        Stage a file.  If the path refers to a directory, it will be
        recursively staged.
        """
        s = self.stat(path)
        if s.is_dir():
            self.run_command(["stage", "-w", "-R", self.initDir + self.clean_path(path)])
        else:
            self.run_command(["stage", "-w", self.initDir + self.clean_path(path)])

    def purge(self, path):
        """
        Purge a file.  If the path is a directory, it will be recursively purged
        """
        s = self.stat(path)
        if s.is_dir():
            self.run_command(["purge", "-R", self.initDir + self.clean_path(path)])
        else:
            self.run_command(["purge", self.initDir + self.clean_path(path)])

    def migrate(self, path, force=False):
        """
        Migrate a file.  If the path is a directory, it will be recursively
        migrated
        """
        stat = self.stat(path)
        if stat.is_dir():
            self.run_command(["migrate", "-R", ("-F" if force else ""),
                              self.initDir + self.clean_path(path)])
        else:
            self.run_command(["migrate", ("-F" if force else ""),
                              self.initDir + self.clean_path(path)])

    def get_checksum(self, path):
        """
        Get the checksum for a path, or None if it is a dir or not set.
        """
        stat = self.stat(path)
        if stat.is_dir():
            return None
        else:
            lines = self.run_command(["hashlist", self.initDir + self.clean_path(path)])
            if not lines:
                return None
            else:
                if lines[0].startswith("(none)"):
                    return None
                else:
                    return lines[0][0:32]

    def verify_checksum(self, path):
        """
        Verify a stored checksum matches the one computed from the data.
        Will return True if it matches, False if it doesn't, and None if
        the file doesn't have a checksum or is a directory.
        """
        cksum = self.get_checksum(path)
        if cksum is None:
            return None
        else:
            lines = self.run_command(["hashverify", self.initDir + self.clean_path(path)])
            return lines[0].endswith("OK")

    def create_checksum(self, path):
        """
        Create (or refresh) a checksum for a path.  If the path is a directory,
        the directory will be recursed.
        """
        s = self.stat(path)
        if s.is_dir():
            self.run_command(["hashcreate", "-R", "-H", "md5",
                              self.initDir + self.clean_path(path)])
        else:
            self.run_command(["hashcreate", "-H", "md5",
                              self.initDir + self.clean_path(path)])

    def get_stream(self, rpath):
        """
        Retrieve a file via a file stream.
        """
        s = self.stat(rpath)
        if not s.is_file():
            return None

        # This will start a new instance of HSI with the get command on the
        # command line and it will send the data to stdout.  The stdout
        # stream will get passed back to the client...
        signal.signal(signal.SIGCHLD, signal.SIG_IGN)
        proc = subprocess.Popen([self.hsiBinary, "-q",
                                 "-A", "keytab", "-k",
                                 self.keyTab, "-l",
                                 self.userName,
                                 'get', '-c', 'on', '-', ':', self.initDir + self.clean_path(rpath)],
                                stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.DEVNULL,
                                text=False)
        return proc.stdout
