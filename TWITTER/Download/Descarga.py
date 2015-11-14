'''
Created on 01/11/2015

@author: fer
'''
from tweepy.streaming import StreamListener
from Conexion_BD.Conexion import Conexion
import json
from _dbus_bindings import Array

class Descarga(StreamListener):
    
    
    def on_data(self, data):
        try:
            json_object = json.loads(data)
            self.download_Tuits(json_object)
            self.download_User(json_object)
            self.download_Coord(json_object)
            self.download_Place(json_object)
            self.download_Bounding(json_object)#'''
            self.download_Entities(json_object)
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
    
    def download_User(self,json_object= None):
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
                    if  str(i).find('profile') >= 0 :
                        s = str(i)
                        #print s
                        lista_keys.remove(s)
                for i in lista_keys:
                    if str(i).find('default') >= 0:
                        s = str(i)
                        #print s
                        lista_keys.remove(s)
                for i in lista_keys:
                    if str(i).find('profile') >= 0:
                        s = str(i)
                        #print s
                        lista_keys.remove(s)
                for i in lista_keys:         
                    values.append(json_object.get(i)) 
                 
                lista_keys.append('id_tuit')
                values.append(id)
                lista = ','.join(x for x in lista_keys)
                #print len(lista)
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
                            
                            
    def download_Entities(self,json_object= None):
        con = Conexion()
        values = []
        lista_keys = []
        
        key = json_object.keys()
        
        for col in key:
            if col == 'id':
                id = json_object.get(col)
            elif col == 'entities':
                ent = json_object.get(col)
                key = ent.keys()
                for i in key:
                    #print '{0}{1}'.format(i,type(ent.get(i)))
                    if 'hashtags' == i:
                        arr =ent.get(i)
                        if len(arr) > 0:
                            for i in range(0,len(arr)):
                                values_has = []
                                list_keys = []
                                
                                list_keys = arr[i].keys()
                                list_keys.pop(0)
                                lista = ','.join(x for x in list_keys)
                                lista = lista +','+'ind_inicial'+','+'ind_final'+','+'id'       
                                values_has = arr[i].values()
                                values_has.append (values_has[0][0])
                                values_has.append (values_has[0][1])
                                values_has.pop(0)
                                values_has.append(id)
                                con.insertTuit(lista, values_has,'hashtags')
                                lista=None
                                values_has = None
                                list_keys = None
                    elif 'user_mentions' == i:
                        arr = ent.get(i)
                        if len(arr) > 0:
                            for i in range(0,len(arr)):
                                keys = arr[i].keys()
                                val_usr = arr[i].values()
                                keys.pop(1)
                                keys.append('id_tuit')
                                keys.append('ind_inicial')
                                keys.append('ind_final')
                                lista = ','.join(x for x in keys)
                                val_usr.append(id)
                                val_usr.append(val_usr[1][0])
                                val_usr.append(val_usr[1][1])
                                val_usr.pop(1)
                                
                                con.insertTuit(lista,val_usr,'User_mentions')
                                lista = None
                                val_usr = None
                                keys = None
                                
                    elif 'symbols' == i:
                        arr = ent.get(i)
                        #print arr
                        
                    
