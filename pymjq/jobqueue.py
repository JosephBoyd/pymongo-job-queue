import pymongo
from pymongo import MongoClient, CursorType, IndexModel
from pymongo.write_concern import WriteConcern
from datetime import datetime
import time


class JobQueue:

    def __init__(self, db, name='jobqueue', silent=False):
        """ Return an instance of a JobQueue.
        Initialization requires one argument, the database,
        since we use one jobqueue collection to cover all
        sites in an installation/database. The second
        argument specifies if to print status while waiting
        for new job, the default value is False"""
        self.db = db
        self.name = name
        self.silent = silent
        if not self._exists():
            print('Creating jobqueue collection.')
            self._create()
        self.q = self.db[name]

    def _create(self, capped=True):
        """ Creates a Capped Collection. """
        # size parameter bytesize
        try:
            self.db.create_collection(self.name,
                                      capped=capped, max=800000,
                                      size=300 * 1024 * 800000,
                                      autoIndexId=True)
            self.db[self.name].create_indexes([IndexModel([('status', pymongo.ASCENDING)]),
                                               IndexModel([('data', pymongo.ASCENDING)]),
                                               IndexModel([('ts.created', pymongo.ASCENDING)]),
                                               IndexModel([('ts.started', pymongo.ASCENDING)]),
                                               IndexModel([('ts.done', pymongo.ASCENDING)])])
                
        except:
            raise Exception('Collection "jobqueue" already created')

    def _exists(self):
        """ Ensures that the jobqueue collection exists in the DB. """
        return self.name in self.db.collection_names()

    def valid(self):
        """ Checks to see if the jobqueue is a capped collection. """
        opts = self.db[self.name].options()
        if opts.get('capped', False):
            return True
        return False

    def next(self):
        """ Runs the next job in the queue. """
        row = self.q.f.find_one_and_update({'status': 'waiting'},
                                           {'$set':
                                            {'status': 'done...',
                                             'ts.started': datetime.now(),
                                             'ts.done': datetime.now()}})
        if row:
            try:
                return row
            except:
                raise Exception('There are no jobs in the queue')

            
    def pub(self, data=None):
        """ Publishes a doc to the work queue. """
        doc = dict(
            ts={'created': datetime.now(),
                'started': datetime.now(),
                'done': datetime.now()},
            status='waiting',
            data=data)
        try:
            self.q.insert(doc, manipulate=False)
        except:
            raise Exception('could not add to queue')
        return True

    
    def upub(self, data=None):
        """ Publishes a doc to the work queue. """
        doc = dict(
            ts={'created': datetime.now(),
                'started': datetime.now(),
                'done': datetime.now()},
            status='waiting',
            data=data)
        try:
            self.q.update_one({'status': 'waiting', 'data': data},
                              {'$set': doc}, upsert=True)
        except:
            raise Exception('could not add to queue')
        return True

    
    def __iter__(self):
        """ Iterates through all docs in the queue
            and waits for new jobs when queue is empty. """
        while 1:
            try:
                row = self.q.find_one_and_update(
                    {'status': 'waiting'},
                    {'$set':
                     {'status': 'working',
                      'ts.started': datetime.now()}})
                if row:
                    print('---')
                    print('Working on job:')
                    yield row
                    self.q.update_one({'_id': row['_id']},
                                      {'$set': {'status': 'done...',
                                                'ts.done': datetime.utcnow()}})
                else:
                    raise Exception('queue empty')
            except:
                time.sleep(5)
                if not self.silent:
                    print('waiting!')

    def queue_count(self):
        """ Returns the number of jobs waiting in the queue. """
        cursor = self.q.find({'status': 'waiting'})
        if cursor:
            return cursor.count()

    def clear_queue(self):
        """ Drops the queue collection. """
        self.q.drop()
