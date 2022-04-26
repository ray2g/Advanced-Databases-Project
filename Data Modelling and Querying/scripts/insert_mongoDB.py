"""
Mongo DB - Data Insertion
ADB Bands Project

@author: Joao Raimundo

"""

# Import Packages
from pymongo import MongoClient
import certifi
from datetime import datetime


#___________________Def Functions___________________________________________________________//

# get bands

def getBand(url):
    # add bands
    name = None
    with open("mongo_csv/bands_mongoDB.txt","r",encoding="utf-8") as file:
        for line in file:
            line = line.split("\t")
            band_url = line[0]
            band_name = line[1].strip('\n')
            if url == band_url:
                name = band_name
                
    # verify is the band associated with the band_url exist for the album's band, 
    # otherwise the key for the bands list/array in the albums dictionary will not 
    # be created, and the band list/array will not be appended to the albums dictionary.       
    return name if name else None



# get genres_______________________________________________________________________________/

def getGenre(url):
    # add genres
    genres = []
    with open("mongo_csv/genres_mongoDB.csv","r",encoding="utf-8") as file:
        # add genres
        for line in file:
            line = line.split(",")
            band_url = line[0].strip('"')
            genre_name = line[1].strip('\"\n')
            if url == band_url:
                genres.append(genre_name)
    
    # verify is the genres associated with the band_url exist for the album's band, 
    # otherwise the key for the genres list/array in the albums dictionary will not 
    # be created, and the genres list/array will not be appended to the albums dictionary. 
    return genres if genres else None



# get artists_______________________________________________________________________________/

def getArtists(url):
    
    # add artists
    artists = []
    
    with open("mongo_csv/artists_mongoDB.csv","r",encoding="utf-8") as file:
        for line in file:
            
            dict_artist = {
                "artist_url": None,
                "artistic_name": list(),
                "is_active": None
                }
            
            line = line.split('\t')
            band_url = line[0]
            artist_url = line[1]
            artistic_name = line[2]
            is_active = line[3].strip('\n')
            
            if url == band_url:
                dict_artist["artist_url"] = artist_url
                dict_artist["is_active"] = is_active
                if artist_url == dict_artist["artist_url"]:
                    dict_artist["artistic_name"].append(artistic_name)
                    
                else:
                    dict_artist["artistic_name"] = [artistic_name]
                    dict_artist["is_active"] = is_active
                artists.append(dict_artist)
    
    # verify is the artists associated with the band_url exist for the album's band, 
    # otherwise the key for the artists dictionary in the albums dictionary will not be created,
    # and the artist dictionary will not be appended to the albums dictionary.      
    return artists if artists else None



#___________________Albums Dictionary_______________________________________________________________//


# import albums data
albums_albums = open("mongo_csv/albums_mongoDB.txt","r",encoding="utf-8")

# open a list that will contain one dictionary for each band
albums = []

# open a set of unique band URLS
album_set = set()


# add albums. Create a dictionary for the albums colection
#i=0
for line in albums_albums:
    #if i<5  :
        line = line.split("\t")
        
        # transform realease date str into datetime
        year = int(line[4].split("-")[0])
        month = int(line[4].split("-")[1])
        day = int(line[4].split("-")[2])
        date_str = line[4]
        date = datetime.strptime(date_str,'%Y-%m-%d')

        band_url = line[0]
        album_name = line[1].strip('\"')
        sales = int(line[2]) 
        running_time = float(line[3])
        release_date = date
        abstract = line[5].strip('\"').strip('\"\n')
    

        # add new band URL to albums set and create a dictionary entry
        dict_album = {
            "band_url": band_url,
            "album_name": album_name,
            "sales": sales,
            "running_time": running_time,
            "release_date": release_date,
            "abstract": abstract,
            }
        
        
        # in case of the inexistance of the bands, genres and artists list/array/dictionary associated with the band_url
        # for an album,the list/array/dictionary will not be appended to the album dictionary, aka document, 
        # and the a key will not be created.
        
        # get bands
        bands = getBand(band_url)
        if bands:
            dict_album["bands"] = bands
            
        # get genre
        genres = getGenre(band_url)
        if genres:
            dict_album["genres"] = genres
            
        # get artists
        artists = getArtists(band_url)
        if artists:
            dict_album["artists"] = artists
        
        # add an album dictionary to albums list, for each band
        albums.append(dict_album)     
        
        #i+=1
        
    #else:
        #break


#______________Connect to MongoDB_________________________________________________________________________________________________________________//

# connect to Mongo Atlas DB (replace <password>)
ca = certifi.where()
cluster = MongoClient("mongodb+srv://bandsDB:<password>@bandscluster.1eit9.mongodb.net/myFirstDatabase?retryWrites=true&w=majority",tlsCAFile=ca)

db = cluster["bandsDB"]
collection = db["albums"]

    
#_____Populate MongoDB with albums dictionaries___________________________//

for album in albums:
    collection.insert_one(album)




