# lastfm-spotify-tags-sim-userdata
Version of the Million Songs Dataset that yields 30sec of audio, lastfm tags, lastfm similarity, and echo profile user data per song.
Specifically, the dataset contains around 130k songs and for each songs the following data is available:
- `track_id` from MSD (primary key)
- `song_name` name of the song
- `song_arist` artist of the song
- `lastfm_url` link to the song on LastFM
- `tags` from the lastfm dataset (genre descriptors)
- `similars` from the lastfm dataset (list of `track_id`) with score
- `users` from the Echo Nest dataset with play count
- `preview_url` to the 30sec Spotify preview of the song (mp3)
- `spotify_id` can e.g. be used to query Spotify API for getTrack

## Schema
The sqlite database contains the following tables:

### Tracks
`track_id` | `name` | `artist` | `spotify_preview_url` | `lastfm_url` | `spotify_id`

### Users
`user_id`

### Tags
`track_id` | `tag_name_0`| `tag_name_1`| `tag_name_2`| `tag_name_3` ...

### Similar
`track_id` | `track_id` | `score`

### Track Users
`track_id` | `user_id` | `playcount`




## Data Prerequisites
The following data is required if you want to re-create the dataset.
If you are only interested in the results, no need to download.

### Lastfm Similars
Download the lastfm data from the official [website](http://millionsongdataset.com/lastfm/) both the `TRAIN SET` and the `TEST SET` or 
```bash script
$ wget http://millionsongdataset.com/sites/default/files/lastfm/lastfm_train.zip -P data
$ wget http://millionsongdataset.com/sites/default/files/lastfm/lastfm_test.zip -P data
```
Unzip and put it into `./data`


### Echo Nest Taste Profile Train Triplets
Download the `train_triplets.txt` file from the official [website](http://millionsongdataset.com/tasteprofile/) or 
``` bash script
$ wget http://millionsongdataset.com/sites/default/files/challenge/train_triplets.txt.zip -P data
```
Unzip and put it into `./data`


### Spotify Previews and Tags
The spotify preview and lastfm tags are obtained from [this](https://github.com/renesemela/lastfm-dataset-2020) repository. 
You can just download the sqlite file containing:
```
$ wget https://github.com/renesemela/lastfm-dataset-2020/blob/master/datasets/lastfm_dataset_2020/lastfm_dataset_2020.db -P data
```


### Unique Tracks (unique_tracks.txt)
Translates song ids into track ids and back. Get it from [here](http://millionsongdataset.com/pages/getting-dataset/)
```shell script
$ wget http://millionsongdataset.com/sites/default/files/AdditionalFiles/unique_tracks.txt -P data
```