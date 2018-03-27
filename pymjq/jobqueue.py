import pymongo
from pymongo import MongoClient, CursorType
from datetime import datetime
import time


class JobQueue:

    def __init__(self, db, silent=False):
        """ Return an instance of a JobQueue.
        Initialization requires one argument, the database,
        since we use one jobqueue collection to cover all
        sites in an installation/database. The second 
        argument specifies if to print status while waiting
        for new job, the default value is False"""
        self.db = db
        self.silent=silent
        if not self._exists():
            print ('Creating jobqueue collection.')
            self._create()
        self.q = self.db['jobqueue']

    def _create(self, capped=True):
        """ Creates a Capped Collection. """
        # TODO - does the size parameter mean number of docs or bytesize?
        try:
            self.db.create_collection('jobqueue',
                                      capped=capped, max=100000,
                                      size=10 * 1024 * 1024,
                                      autoIndexId=True)
        except:
            raise Exception('Collection "jobqueue" already created')

    def _exists(self):
        """ Ensures that the jobqueue collection exists in the DB. """
        return 'jobqueue' in self.db.collection_names()

    def valid(self):
        """ Checks to see if the jobqueue is a capped collection. """
        opts = self.db['jobqueue'].options()
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

    def __iter__(self):
        """ Iterates through all docs in the queue
            andw aits for new jobs when queue is empty. """
        while 1:
            try:
                row = self.q.find_one_and_update(
                    {'status': 'waiting'},
                    {'$set':
                     {'status': 'working',
                      'ts.started': datetime.now()}})
                print('---')
                print('Working on job:')
                yield row
                self.q.update_one({'_id': row['_id']},
                                  {'$set': {'status': 'done...',
                                            'ts.done': datetime.utcnow()}})
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
