'''
Created on 01/11/2015

@author: fer
'''
from tweepy.streaming import StreamListener
from Conexion_BD.Conexion import Conexion
import json

class Descarga(StreamListener):
    
    
    def on_data(self, data):
        try:
            json_object = json.loads(data)
            print json_object
        except ValueError, e:
            print "ERROR"
        
        
        #print b #se le puso para que se pueda escribir en los archivos
        print "------------" 
        return True
    
    def on_error(self, status):
        print status
