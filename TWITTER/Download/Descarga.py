'''
Created on 01/11/2015

@author: fer
'''
from tweepy.streaming import StreamListener
from Conexion_BD.Conexion import Conexion
import json

class Descarga(StreamListener):
    
    
    def on_data(self, data):
        con = Conexion()
        try:
            json_object = json.loads(data)
            con.insertTuit(None,None,'prueba2', json.dumps(json_object))
            json_object.keys()
            #key = json_object.keys()
            #lista = ','.join(self.verifica_Columnas(json_object.get(x),x) for x in key)
            #print lista
        except ValueError, e:
            print "ERROR"
        #print b #se le puso para que se pueda escribir en los archivos
        print "------------" 
        
    def on_error(self, status):
        print status
     
    def verifica_Columnas(self,tipo = None,col= None):
        if type(tipo) == dict :
            return 'HOLAA******'
        else: 
            return col
