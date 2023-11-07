import threading, traceback
import os, sys, time, random, cmd, stat
from math import *
import shelve, sqlite3
import re

PATH = "c:/"

if len(sys.argv) > 1:
  PATH = sys.argv[1].strip()
  PATH = PATH[0].lower() + PATH[1:]
  PATH = os.path.normpath(os.path.abspath(PATH))
  
  if not PATH.endswith(os.path.sep):
    PATH += os.path.sep

print("PATH:", PATH)
  
DB_VERSION = 0

_dbkey = PATH.replace("/", "_").replace("\\", "_").replace("-", "_").replace(" ", "_").replace(":", "_")

DB_PATH = "_" + _dbkey + "_space_analyzer.db"
db_lock = threading.Lock()
size = [0]
db = [0]

LAST_PATH = _dbkey + "_space_last_path.txt"

def safepath(path):
  #return escape(path)
  
  p = ''
  for c in path:
    n = ord(c)
    
    if n < 10 or n > 210:
      c = "?"
    
    p += c
  
  return p
  
if os.path.exists(LAST_PATH):
  with open(LAST_PATH, "r") as file:
    s = file.read().strip()
    
    if not os.path.exists(s):
      print("ERROR! last_path does not exist!")
    else:
      print("Resuming from " + safepath(s))
      PATH = s

def scandir(path):
  if 0:
    st = os.stat(path)
    print(st.st_file_attributes)

    for k in dir(stat):
      if not k.startswith("FILE_"):
        continue
        
      v = getattr(stat, k)
      if st.st_file_attributes & v:
        print(k)
      
      if st.st_mode & stat.S_IFLNK:
        print("S_IFLNK")
  
  try:
    ret = list(os.scandir(path))
  except PermissionError:
    sys.stderr.write("Failed to open %s\n" % path)
    #traceback.print_last()
    ret = []
  
  ret.sort(key = lambda item: item.name.lower())
  
  return ret 
  
def resumable_walk(start_path):
  path = start_path
  path = os.path.abspath(os.path.normpath(path))
  
  segments = []
  p = path
  lastp = 173
  
  while p != lastp:
    segments.append(os.path.split(path)[1])
    lastp = p
    p = os.path.split(p)[0]
    
  path = os.path.normpath(os.path.abspath(path))
  segments = path.split(os.path.sep)
  stack = []
  
  #wind all iterators
  path2 = ""
  for i in range(len(segments)):
    path2 = os.path.join(path2, segments[i])
    
    if not path2.endswith(os.path.sep):
      path2 += os.path.sep
    
    diter = iter(scandir(path2))
    
    if i == 0:
      for entry in scandir(path2):
        print(entry.name)
    
    print("")
    
    if i < len(segments) - 1:
      next = segments[i + 1].lower()
      
      while 1:
        try:
          entry = diter.__next__()
        except StopIteration:
          diter = None
          break
        
        if i == 0:
          print(entry.name)
        
        if entry.name.lower() == next:
          break
      
    if not diter:
      #print(stack, list(scandir(path2)))
      print("Walk resumption error", path2)
      #sys.exit()
      
      diter = iter(scandir(path2))
    
    stack.append([path2, diter])
  
  while len(stack) > 0:
    root, diter = stack.pop()
    
    dirs = []
    files = []
    
    entries = []
    while 1:
      try:
        entry = diter.__next__()
      except StopIteration:
        break
      
      entries.append(entry)
      
    entries.reverse()
    
    for entry in entries:
      if "Windows\\WUModels" in entry.path or \
         "@rocket.chat" in entry.path or \
         "rocketchat" in entry.path or \
         "Rocket.Chat" in entry.path:
        #windows symlink bug, not being detected by scandir
        continue
      
      try:
        entry.is_dir()
      except PermissionError:
        print("Cannot access", safepath(entry.path))
        continue
      
      if entry.is_dir():
        if not entry.is_symlink():
          dirs.append(entry.name)
          path3 = entry.path          
          stack.append([path3, iter(scandir(path3))])
      else:
        files.append(entry.name)
      
    yield root, dirs, files

if 0:      
  for root, dirs, files in resumable_walk("c:/dev"):
    print(dirs)
    print("")
  sys.exit(0)

class Global:
  def __init__(self):
    self.verbose = 0

glob = Global()

def escape(k):
  if type(k) == str:
    k2 = ""
    
    for c in k:
      n = ord(c)
      if c == "\"" or c == "'" or n < 14 or n > 220:
        c = "_$_CHR_%i_" % n
      k2 += c
    
    return "\"" + k2 + "\""

pattern = re.compile(r"(.*)_\$_CHR_([0-9]+)(_.*)")
def unescape(k):
  def repl(m):
    n = int(m.group(2))
    
    if n < 220:
      n = chr(n)
    else:
      n =  "?"
    
    return m.group(1) + n + m.group(3)
    
  return re.sub(pattern, repl, k)

if 0:
  k = escape("sdfsfdsf\n")  
  print(unescape(k))
  sys.exit()

class DBFile:
  def __init__(self, is_dir=False, path=""):
    self.is_dir = is_dir
    self.path = path
    self.size = 0
    self.key = ""
    self.db_version = DB_VERSION
  
  def clone(self):
    f = DBFile()
    f.is_dir = self.is_dir
    f.path = self.path
    f.size = self.size 
    f.key = self.key
    f.db_version = self.db_version
    
    return f
    
class DBSqLite:
  def __init__(self, path):
    self.path = path
    self.last_save = 0
    self.cache = {}
    self.write_cache = {}
    
    self.closed = False
    
    create = not os.path.exists(path)
    
    #disable thread exclusion
    #, 5, 0, None, False

    self.con = sqlite3.connect(path, check_same_thread=False)
    cur = self.cur = self.con.cursor()
    
    if create:
      print("Creating database table")
      cur.execute("""CREATE TABLE files
      (key text, is_dir bool, path text, size real, db_version real)""")
      cur.execute("""CREATE INDEX idx_key on files (key)""")
      cur.execute("""CREATE INDEX idx_size on files (size)""")
      
      self.con.commit()
      
  def __enter__(self):
    return self
  
  def __exit__(self, a, b, c):
    self.close()
    
  def flush(self):
    for k in self.write_cache:
      q = "SELECT * FROM files WHERE key=%s" % escape(k)
      insert = True
      
      for row in self.cur.execute(q):
        insert = False
      
      f = self.write_cache[k]
      f.key = k
        
      if insert:
        #print("INSERTING")
        q = """INSERT INTO files (key,is_dir,path,size,db_version)
                         VALUES (%s,%s,%s,%s,%s)""" % \
          (escape(f.key), f.is_dir, escape(f.path), f.size, f.db_version)
      else:
        q = """UPDATE files SET is_dir=%s,path=%s,size=%s,db_version=%s WHERE key=%s""" % \
        (f.is_dir, escape(f.path), f.size, f.db_version, escape(f.key))
        
        #print("UPDATING")
        pass
        
      self.cur.execute(q)
    
    self.con.commit()
    self.cur = self.con.cursor()    
    self.write_cache = {}
    
  def _lookup(self, k):
    q = "SELECT * FROM files WHERE key=" + escape(k)
    for row in self.cur.execute(q):
      f = DBFile()
      f.key = unescape(k)
      #print("ROW", row)
      f.is_dir = row[1]
      f.path = unescape(row[2])
      f.size = row[3]
      f.db_version = row[4]
      
      return f
    return None
  
  def get_top(self, n=55, prefix=None):
    self.flush()
    
    q = "SELECT key FROM files ORDER BY size DESC LIMIT " + str(n)
    
    """
    if prefix is not None:
      pattern = prefix.strip() + "*"
      q = "SELECT key FROM files WHERE path LIKE %s ORDER BY size DESC LIMIT %i" % (escape(pattern), n)
      
      print(q)
    #"""
    
    ret = []
    keys = []
    
    for row in self.cur.execute(q):
      keys.append(row[0])
      
    for k in keys:
      ret.append(self[k])
    
    return ret
    
  def __getitem__(self, k):
    if self.closed:
      return None
      
    if k in self.cache:
      return self.cache[k]
    
    f = self._lookup(k)
    
    if f is not None:
      self.cache[k] = f
      return f
    return None
    
  def __setitem__(self, k, v):
    self.cache[k] = self.write_cache[k] = v
    
    if time.time() - self.last_save > 15:
      self.flush()
      self.last_save = time.time()
  
  def __contains__(self, k):
    if k in self.cache:
      return True
      
    f = self._lookup(k)
    if f is not None:
      self.cache[k] = f
      
    return f is not None
    
  def close(self):
    self.flush()
    self.con.commit()
    self.con.close()
    self.closed = True

def test_db():
  if 1: #with DBSqLite(DB_PATH) as db:
    global db
    
    print("key" in db)
    if "key" in db:
      print(db["key"])
    f = DBFile()
    f.db_version = 2
    f.size = 3
    f.path = "path"
    f.is_dir = True
    
    db["key"] = f
    
    keys = ["path2", "path3", "path4", "path5", "ath"]
    
    for k in keys:
      f = f.clone()
      f.path = k
      f.size = int(random.random()*1000.0)
      db[k] = f
    
    for f in db.get_top():
      print(" ", f.size, f.path)
    
  sys.exit(0)

#test_db()

def formatsize(f):
  if f > 1024*1024*1024:
    f = "%.4fgb" % (f / 1024/1024/1024)
  elif f > 1024*1024:
    f = "%.2fmb" % (f / 1024/1024)
  elif f > 1024*1024:
    f = "%.2fkb" % (f / 1024)
  return f

do_stop = [False]

def job():
  if 1: #with DBSqLite(DB_PATH) as db:
    global db
    print("Staring job")
      
    last_print = time.time()
    
    ci = 0
    last_time2 = time.time()
    
    for root, dirname, files in resumable_walk(PATH):
      for f in files:        
        #save walk resume point
        if time.time() - last_time2 > 0.5:
          with open(LAST_PATH, "w") as file:
            file.write(root)
          last_time2 = time.time()
          
        #give other threads cpu time
        if ci > 655:
          time.sleep(0.001)
          ci = 0
        else:
          ci += 1
          
        doprint = time.time() - last_print > 0.75
        if doprint:
          last_print = time.time()
          
        if do_stop[0]:
          return
                
        try:
          path = os.path.join(root, f)
          path = os.path.normpath(os.path.abspath(path))
        except UnicodeEncodeError:
          #print("invalid path")
          continue
        
        if glob.verbose and time.time() - last_print > 0.1:
          print(safepath(path))
          last_print = time.time()
          
        if doprint:
          pass #print(safepath(path))

        with db_lock:
          entry = db[path]
          
          if entry is None:
            entry = DBFile(False, path)
            entry.db_version = -1
            
          if entry.db_version == DB_VERSION:
            size[0] += entry.size
            continue
          
          try:
            st = os.stat(path)
            isdir = stat.S_ISDIR(st.st_mode)
          except:
            #print("failed to open file", safepath(path))
            continue
            
          if doprint:
            #print(st.st_size, isdir)
            #print(formatsize(size[0]))
            pass
          
          sz = st.st_size
          size[0] += sz
          
          lastparent = None
          parent = os.path.split(path)[0]
          n = 0
          
          entry.size = st.st_size
          entry.db_version = DB_VERSION
          db[path] = entry

          while parent != lastparent and len(parent) > 0:
            n += 1
            if n > 100:
              print("EEK!", parent)
              break
            
            if parent in db:
              entry2 = db[parent]
            else:
              entry2 = DBFile(True, parent)
            
            entry2.size += st.st_size
            db[parent] = entry2

            lastparent = parent
            parent = os.path.split(parent)[0]
    
class ExitSignal (RuntimeError):
  pass
  
class Console (cmd.Cmd):
  intro = "Scanning"
  prompt = "> "
  file = None
  
  def do_v(self, arg):
    'verbose'
    
    with db_lock:
      glob.verbose ^= True
      print("VERBOSE", glob.verbose)
  
  def do_q(self, arg):
    'exit'
    raise ExitSignal
    
  def do_quit(self, arg):
    'exit'
    raise ExitSignal
  
  def do_exit(self, arg):
    'exit'
    raise ExitSignal
 
  def do_s(self, arg):
    'print current size sum'
    print(formatsize(size[0]))
      
  def do_p(self, arg):
    'print size'
    args = arg.split(" ")
    arg = args[0]
    
    maxn = 15
    
    if arg:
      try:
        maxn = int(arg)
      except:
        pass
        
    if len(args) > 1:
      prefix = args[1]
    else:
      prefix = None
    
    with db_lock:
      for f in db.get_top(maxn, prefix):
        print(formatsize(f.size), f.path)
        
      print("\n\n")
      
    print("Size:", formatsize(size[0]))
    
def main():
  global db 
  
  db = DBSqLite(DB_PATH)
  
  thread = threading.Thread(target=job)
  thread.start()

  try:
    Console().cmdloop()
  except ExitSignal:
    print("Exiting")
    do_stop[0] = True

    with db_lock:
      db.close()      
  except KeyboardInterrupt:
    print("Exiting")
    do_stop[0] = True

    with db_lock:
      db.close()
    
if __name__ == "__main__":
  main()

