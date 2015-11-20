'''
Created on 01/11/2015

@author: fer
'''
import time
from tweepy.streaming import StreamListener
from Conexion_BD.Conexion import Conexion
import json
from _dbus_bindings import Array

class Descarga(StreamListener):
    
    
    def on_data(self, data):
        try:
            json_object = json.loads(data)
            id = json_object.get('id')
            self.download_Tuits(json_object)
            '''self.download_User(json_object.get('user'),id)
            
            coord = json_object.get('coordinates')
            if coord != None:
                self.download_Coord(coord,id)
            
            place = json_object.get('place')
            self.download_Place(place,id)
            self.download_Bounding(place.get('bounding_box') , id)
            
            self.download_Entities(json_object.get('entities'),id)#'''
        except ValueError, e:
            print "ERROR"
        #print b #se le puso para que se pueda escribir en los archivos
        print "------------" 
        
    def on_error(self, status):
        print status
     
    def download_Tuits(self,json_object= None):#QUITAR TODOS LOS STR
        con = Conexion()
        values = []
        lista_keys = []
        key = self.quita_Key('str',json_object.keys())
        
        for col in key:#puse los nombres de las columnas que especificamente no queria por que hay veces que no aparecen y tienen nonetype y al almacenar en la base saldra error 
            #print json_object.get('coordinates')
            if type(json_object.get(col)) != list and type(json_object.get(col)) != dict and json_object.get(col) != None:#and col != 'coordinates'and col != 'geo':#col != 'entities' and col != 'geo' and col != 'user' and col != 'place' and col != 'coordinates'and col != 'extended_entities' and col != 'quoted_status':
                #al if le puse diferente de NOne para que toda llave que no tenga un valor asociado no la guardo en la base 
                #la base de datos por default le pone null
                if col == 'created_at':
                    created = str(json_object.get(col))
                    arreglo = created.split(" ")
                    #obtengo la hora con python 
                    fecha = (time.strftime("%y-%m-%d"))#'{0}-{1}-{2}'.format(arreglo[5],arreglo[1],arreglo[2])
                    lista_keys.append ('fecha')
                    values.append(fecha)
                    lista_keys.append('hora')
                    values.append(time.strftime("%X"))
                    
                else:
                    lista_keys.append(col)#construir cadena 
                    values.append(json_object.get(col))
        #lista_keys = self.quita_Key('str', lista_keys)
        #values = self.get_Valores(lista_keys, json_object)    
        lista = ','.join(x for x in lista_keys)
        #print lista 
        #print values
        #con.insertTuit(lista, values, 'tuits')
        
    def download_User(self,json_object= None,id = None):
        con = Conexion()
        values = []
        lista_keys = []
        aux = []
        palabras = ['profile','str']
        
        lista_keys = json_object.keys()
        for i in palabras:
            lista_keys = self.quita_Key(i,lista_keys)
        
        values =  self.get_Valores(lista_keys, json_object)            
        lista_keys.append('id_tuit')
        values.append(id)
        lista = ','.join(x for x in lista_keys)
        con.insertTuit(lista, values,'usuarios')
               
                 
    def download_Coord(self,json_object = None,id = None):            
       
        con = Conexion()
        values = []
        lista_keys = []
        key = json_object.keys()
        for i in key:
            if i == 'type':
                lista_keys.append(i)
                values.append(json_object.get(i))
            else:
                arr = json_object.get(i)
                lista_keys.append('latitud')
                values.append(arr[1])
                lista_keys.append('longitud')
                values.append(arr[0])        
                #values.append(json_object.get(i))
                lista_keys.append('id_tuit')
                values.append(id)
                lista = ','.join(x for x in lista_keys)
        con.insertTuit(lista, values,'coordenadas')
    
    '''def download_Geo(self,json_object=None,id = None):            
       
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
                    con.insertTuit(lista, values,'geo')'''
                
    def download_Place(self,json_object= None,id = None):
        con = Conexion()
        values = []
        lista_keys = []
        key = json_object.keys()
        for i in key:
            if type(json_object.get(i)) != dict and type(json_object.get(i)) != list:
                lista_keys.append(i) 
                values.append(json_object.get(i))
                
        lista_keys.append('id_tuit')
        values.append(id)
        lista = ','.join(x for x in lista_keys)
        con.insertTuit(lista, values,'Place')
                
    def download_Bounding(self,json_object= None,id = None):
        con = Conexion()
        values = []
        lista_keys = []
        val = json_object.values()#saco los valores de las llaves 
        #el valor val[1] es [[[-99.364536, 19.232313], [-99.364536, 19.405081], [-99.246625, 19.405081], [-99.246625, 19.232313]]]
        #el val[1][0] es [[-99.191996, 19.357102], [-99.191996, 19.404124], [-99.130965, 19.404124], [-99.130965, 19.357102]]
        for x in val[1][0]:
            val_box = []
            list_box=[]
            list_box.append('id')
            val_box.append(id)
            list_box.append('type')
            val_box.append(json_object.get('type'))
            list_box.append('longitud')
            val_box.append(x[0])
            list_box.append('latitud')
            val_box.append(x[1])
            lista_str = ','.join(x for x in list_box)
            con.insertTuit(lista_str, val_box,'Boundings')
            lista_str = None 
            list_box = None
            val_box = None
                            
                            
    def download_Entities(self,json_object = None,id = None):
        con = Conexion()
        values = []
        lista_keys = []
        
        key = json_object.keys()
        
        for i in key:
                    #print '{0}{1}'.format(i,type(ent.get(i)))
            if 'hashtags' == i:
                arr =json_object.get(i)
                #print arr
                if len(arr) > 0:
                    self.hash_users(arr, id,i,None)
                    
            elif 'user_mentions' == i:
                arr = json_object.get(i)
                if len(arr) > 0:
                    self.hash_users(arr, id,i,'str')
                    
                                
                
                    #print arr
    def hash_users(self,arr = None,id = None,tabla = None,remove_cad = None):
        con = Conexion()
        for i in range(0,len(arr)):
            values = []
            list_keys = []
            
            list_keys = arr[i].keys()                     
            values = arr[i].values()
            
                                    
            list_keys = self.quita_Key('indices',list_keys)
            
            if remove_cad != None:
                list_keys = self.quita_Key(remove_cad,list_keys)
            
            lista = ','.join(x for x in list_keys)
            lista = lista +','+'ind_inicial'+','+'ind_final'+','+'id'       
            #necesito el valor de inidice que es un arreglo [a,b] despues de sacar sus valores a y b ahora si pop
            for x in range(0,len(values)-1):
                if type(values[x]) == list:
                    values.append(values[x][0])
                    values.append(values[x][1])
                    values.pop(x)
            values.append(id)
            #con.insertTuit(lista, values,tabla)
            lista=None
            values = None
            list_keys = None
                            
                    
    def quita_Key (self,cadena = None,lista_keys = None):
        aux = []
        for j in lista_keys:
            if  str(j).find(cadena) < 0 :
                aux.append(j)
        return aux
    
    def get_Valores (self,lista_keys = None, json_object = None):
        aux = []
        for i in lista_keys:
            aux.append(json_object.get(i))
        return aux
            
        
        
        