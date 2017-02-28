
from nameko.rpc import rpc
from nameko.timer import timer
from pymongo import IndexModel, ASCENDING, DESCENDING
from nameko_mongodb.database import MongoDatabase

from application.dependencies.opta import OptaDependency


class OptaCollectorService(object):
    name = 'opta_collector'
    
    database = MongoDatabase()
    
    opta = OptaDependency()
    
    def create_f1_indexes(self):
        indexes = [IndexModel([('id', ASCENDING,)])]

        self.database.f1.create_indexes(indexes)
        
    def create_f9_indexes(self):
        indexes = [IndexModel([('match_info.id', ASCENDING,)])]
        
        self.database.f9.create_indexes(indexes)
    
    @rpc
    def add_f1(self, season_id, competition_id):
        
        self.create_f1_indexes()
        
        calendar = self.opta.get_calendar(season_id, competition_id)
        
        self.database.f1.insert_many(calendar)
        
    @rpc
    @timer(900)
    def update_f1_f9(self):
    
        self.create_f9_indexes()
        
        ids = self.database.f1.find({})
    
        
    