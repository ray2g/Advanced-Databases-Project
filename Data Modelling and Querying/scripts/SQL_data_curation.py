'''
Data transformation 

@author Joao Rato

'''

import pandas
import numpy as np

# import import files

bands = pandas.read_csv("band-band_name_processed.csv", sep=',',
                        header=0, engine='python', encoding='utf8')

bands_genre = pandas.read_csv("band-genre_name_processed.csv", sep=',',
                              header=None, engine='python', encoding='utf8')

albums = pandas.read_csv("band-album_data_processed.csv",
                         sep=',', header=None, engine='python')

former_member = pandas.read_csv("band-former_member-member_name_processed.csv", sep=',',
                                header=None, engine='python', encoding='utf8')

member = pandas.read_csv("band-member-member_name_processed.csv", sep=',',
                         header=None, engine='python', encoding='utf8')


# adding index(id) to each dataframe
albums_index = albums.index.values
albums.insert(0, column="band_id", value=albums_index)

bands_index = bands.index.values
bands.insert(0, column="album_id", value=bands_index)


# adding column names
bands.columns = ["band_id", "band_url", "band_name"]
bands_genre.columns = ["band_url", "genre"]
albums.columns = ["album_id", "band_url", "album_name",
                  "release_date", "abstract", "running_time", "sales"]
former_member.columns = ["band_url", "artist_url", "artistic_name"]
member.columns = ["band_url", "artist_url", "artistic_name"]

# adding isActive column to members related table
former_member['is_active'] = False
member['is_active'] = True

# creating the necessary members dataframes
all_members = [former_member, member]
all_members = pandas.concat(all_members)
all_members_band = all_members.reset_index(drop=True)


all_members_url_only = all_members_band[["artist_url"]]
all_members_url_only = all_members_url_only["artist_url"].unique()
all_members_url_only = pandas.DataFrame(all_members_url_only)
all_members_url_only_index = all_members_url_only.index.values
all_members_url_only.insert(0, column="member_id",
                            value=all_members_url_only_index)
all_members_url_only.columns = ["member_id", "artist_url"]

all_members_name = all_members_band[["artistic_name", "artist_url"]]
all_members_name = pandas.DataFrame(all_members_name)
all_members_name_index = all_members_name.index.values
all_members_name.insert(0, column="name_id", value=all_members_name_index)
all_members_name.columns = ["name_id", "artistic_name", "artist_url"]


# creating genre dataframe
genre = bands_genre["genre"].unique()
genre_dataframe = pandas.DataFrame(genre)
genre_index = genre_dataframe.index.values
genre_dataframe.insert(0, column="genre_id", value=genre_index)
genre_dataframe.columns = ["genre_id", "genre_name"]


# merging dataframes
genre_band_merge_genre = pandas.merge(
    bands_genre, genre_dataframe, left_on='genre', right_on='genre_name', how='left')

genre_merged_merge_bands = pandas.merge(
    bands, genre_band_merge_genre, left_on='band_url', right_on='band_url', how='left')

genre_band_dataframe = genre_merged_merge_bands[[
    'band_id', 'genre_id']].astype('Int64').dropna()


albums_merge_band = pandas.merge(
    bands, albums, left_on='band_url', right_on='band_url', how='left').dropna()


albums_merge_band_mong = albums_merge_band[[
    "album_id", "band_url", "album_name", "sales", "running_time", "release_date", "abstract"]]

albums_merge_band = albums_merge_band[[
    "album_id", "band_id", "album_name", "sales", "running_time", "release_date", "abstract"]]


all_members_url_only_merge_all_members_name = pandas.merge(
    all_members_url_only, all_members_name, left_on='artist_url', right_on='artist_url', how='left')
all_members_url_only_merge_all_members_name = all_members_url_only_merge_all_members_name[[
    "name_id", "member_id", "artistic_name"]]


all_members_band_merge_band = pandas.merge(
    all_members_band, all_members_url_only, left_on='artist_url', right_on='artist_url', how='left')
all_members_band_merge_band_merge_bands = pandas.merge(
    all_members_band_merge_band, bands, left_on='band_url', right_on='band_url', how='left')

all_members_band_merge_band_merge_bands = all_members_band_merge_band_merge_bands[[
    "band_id", "member_id", "is_active"]]
all_members_band_merge_band_merge_bands = all_members_band_merge_band_merge_bands.drop_duplicates(
    ["band_id", "member_id", "is_active"])[["band_id", "member_id", "is_active"]]

# fix date time values
#albums_merge_band['sales'].str.replace(['.',','], '').astype(int)
albums_merge_band['release_date'] = pandas.to_datetime(
    albums_merge_band.release_date)
albums_merge_band['release_date'] = albums_merge_band['release_date'].dt.strftime(
    '%Y/%m/%d')


albums_merge_band_mong['release_date'] = pandas.to_datetime(
    albums_merge_band_mong.release_date)
albums_merge_band_mong['release_date'] = albums_merge_band_mong['release_date'].dt.strftime(
    '%Y/%m/%d')
print(albums_merge_band_mong)
# export data to files
bands.to_csv("D:/BDA project/export_files/bands.csv", sep="\t", index=False)
genre_dataframe.to_csv(
    "D:/BDA project/export_files/genre.csv", sep="\t", index=False)
genre_band_dataframe.to_csv(
    "D:/BDA project/export_files/bands_genre.csv", sep="\t", index=False)
albums_merge_band.to_csv(
    "D:/BDA project/export_files/albums.csv", sep="\t", index=False)
all_members_url_only.to_csv(
    "D:/BDA project/export_files/members.csv", sep="\t", index=False)
all_members_url_only_merge_all_members_name.to_csv(
    "D:/BDA project/export_files/artistic_names.csv", sep="\t", index=False)
all_members_band_merge_band_merge_bands.to_csv(
    "D:/BDA project/export_files/bands_members.csv", sep="\t", index=False)

all_members_band_mongo = all_members_band[["band_url", "artist_url", "artistic_name",
                                           "is_active"]]
all_members_band_mongo.to_csv(
    "D:/BDA project/export_files/mongodb_members.csv", sep="\t", index=False)

albums_merge_band_mong.to_csv(
    "D:/BDA project/export_files/albums.csv", sep=",", index=False)
