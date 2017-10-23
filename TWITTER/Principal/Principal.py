'''
Created on 02/11/2015

@author: fer
'''
from tweepy import OAuthHandler
from tweepy import Stream
from Download.Descarga import Descarga



if __name__ == '__main__':
    
    
    #Estas variables contienen las credenciales para podres ingresar a la API
    access_token = "****"
    access_token_secret = "*****"
    consumer_key = "*******"
    consumer_secret = "*****"
    l = Descarga()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    #stream.filter(locations=[-99.36666666,19.05,-98.95,19.6],languages=['es'])
    
    users = ['166594238,3217128862,121549722,40098528','205339755','58847335']#se le pone el id del usuario
    stream.filter(users)

    
    
