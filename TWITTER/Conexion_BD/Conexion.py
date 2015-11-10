'''
Created on 01/11/2015

@author: fer
'''

import psycopg2#sirve para trabajar la conexion de base de datos postgres y python
import sys

class Conexion():
    
    def _conexion (self):
        con = None
        cur = None
        try:
            con = psycopg2.connect(database='BaseTwitter', user='postgres',host='localhost', password = 'f')
            #devuelve un objeto de conexion el host se puede omitir
            #si no tiene password se puede omitir ese parametro tambien
            #cur = con.cursor()

            #print len(datos)
            return con
            #con.commit()
        except psycopg2.DatabaseError, e:
            print 'Error %s' % e
            return con
            #sys.exit(1)
            
        '''finally:
            
            if con:
                con.close()'''
                
                
    def insertTuit(self,__keys=None , values =   None,table_name= None,obj =None):
        cur = None
        con = self._conexion()#este metodo regresa el objeto de conexion
        cur = con.cursor() #devolvera un objeto de cursor , se puede utilizar este objeto para realizar consultas

        #r = ','.join(['%s'] * len(values))#se va a listas %s dependiendo del numero de columnas que tenga (.join sirve para cocatenar cadena so caracteres en este caso mezclados con ",")
        query = ("INSERT INTO prueba2 VALUES (1,'"+obj+"');")
        #query = ("""INSERT INTO """+table_name+""" (""" +__keys +""") VALUES ({0})""".format(r)) #'{0}, {1}, {2}'.format('a', 'b', 'c') >> 'a, b, c'
        #{} si esta vacio lo deja en el orden en que estan en contenido del arreglo values solamente funciona en python 2.7
        #print query
        try:
            cur.execute(query)
            #cur.execute(query,values)
            con.commit()
        except psycopg2.DatabaseError, e:
            print 'Error %s' % e
        con.close()

