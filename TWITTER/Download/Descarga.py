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
            self.download_Tuits(json_object)
            self.download_user(json_object)
            self.download_Coord(json_object)
            self.download_Geo(json_object)
            self.download_Place(json_object)
            self.download_Bounding(json_object)
        except ValueError, e:
            print "ERROR"
        #print b #se le puso para que se pueda escribir en los archivos
        print "------------" 
        
    def on_error(self, status):
        print status
     
    def download_Tuits(self,json_object= None):
        con = Conexion()
        values = []
        lista_keys = []
        key = json_object.keys()
        for col in key:
            if col != 'entities' and col != 'geo' and col != 'user' and col != 'place' and col != 'coordinates'and col != 'extended_entities' and col != 'quoted_status':
                lista_keys.append(col)
        for i in lista_keys:
            values.append(json_object.get(i))
        lista = ','.join(x for x in lista_keys)
            #values.append(self.get_Values(value))
        con.insertTuit(lista, values, 'tuits')
               #print '{0} {1}'.format(col, type(valor)) 
    def download_user(self,json_object= None):
        con = Conexion()
        values = []
        lista_keys = []
        
        key = json_object.keys()
        
        for col in key:
            if col == 'id':
                id = json_object.get(col)
            elif col == 'user':
                usr = json_object.get(col)
                lista_keys = usr.keys()
                for i in lista_keys:
                    values.append(json_object.get(i))
                
                lista_keys.append('id_tuit')
                values.append(id)
                lista = ','.join(x for x in lista_keys)
                
                con.insertTuit(lista, values,'usuarios')
                
    def download_Coord(self,json_object=None):            
       
        con = Conexion()
        values = []
        lista_keys = []
        
        key = json_object.keys()
        
        for col in key:
            if col == 'id':
                id = json_object.get(col)
            elif col == 'coordinates':
                if json_object.get(col) != None:
                    cds = json_object.get(col)
                    key = cds.keys()
                    for i in key:
                        if i == 'type':
                            lista_keys.append(i)
                            values.append(cds.get(i))
                        else:
                            arr = cds.get(i)
                            lista_keys.append('latitud')
                            values.append(arr[1])
                            lista_keys.append('longitud')
                            values.append(arr[0])        
                    #values.append(json_object.get(i))
                    lista_keys.append('id_tuit')
                    values.append(id)
                    lista = ','.join(x for x in lista_keys)
                    con.insertTuit(lista, values,'coordenadas')
    
    def download_Geo(self,json_object=None):            
       
        con = Conexion()
        values = []
        lista_keys = []
        
        key = json_object.keys()
        
        for col in key:
            if col == 'id':
                id = json_object.get(col)
            elif col == 'geo':
                if json_object.get(col) != None:
                    cds = json_object.get(col)
                    key = cds.keys()
                    for i in key:
                        if i == 'type':
                            lista_keys.append(i)
                            values.append(cds.get(i))
                        else:
                            arr = cds.get(i)
                            lista_keys.append('latitud')
                            values.append(arr[1])
                            lista_keys.append('longitud')
                            values.append(arr[0])        
                    #values.append(json_object.get(i))
                    lista_keys.append('id_tuit')
                    values.append(id)
                    lista = ','.join(x for x in lista_keys)
                    con.insertTuit(lista, values,'geo')
                
    def download_Place(self,json_object= None):
        con = Conexion()
        values = []
        lista_keys = []
        
        key = json_object.keys()
        
        for col in key:
            if col == 'id':
                id = json_object.get(col)
            elif col == 'place':
                place = json_object.get(col)
                key = place.keys()
                for i in key:
                    if  i != 'attributes' and i != 'bounding_box':
                        lista_keys.append(i) 
                        values.append(place.get(i))
                
                lista_keys.append('id_tuit')
                values.append(id)
                lista = ','.join(x for x in lista_keys)
                #print lista
                #print values
                con.insertTuit(lista, values,'Place')
                
    def download_Bounding(self,json_object= None):
        con = Conexion()
        values = []
        lista_keys = []
        
        key = json_object.keys()
        
        for col in key:
            if col == 'id':
                id = json_object.get(col)
            elif col == 'place':
                place = json_object.get(col)
                key = place.keys()
                for i in key:
                    if i == 'bounding_box':
                        box = place.get(i)
                        val = box.values()
                        #print len(val[1][0])
                        for x in val[1][0]:
                            val_box = []
                            list_box=[]
                            list_box.append('id')
                            val_box.append(id)
                            list_box.append('type')
                            val_box.append(box.get('type'))
                            list_box.append('longitud')
                            val_box.append(x[0])
                            list_box.append('latitud')
                            val_box.append(x[1])
                            lista_str = ','.join(x for x in list_box)
                            con.insertTuit(lista_str, val_box,'Boundings')
                            lista_str = None 
                            list_box = None
                            val_box = None
                            
                            
                    