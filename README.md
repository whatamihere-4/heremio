# Heremio
Watch your Stremio library directly inside of HereSphere
This readme will be updated better in the future I just wanted to get this working ASAP

# Installation
I strongly recommend running this in a Docker container but it can technically be run locally
All streaming and video dec is done through headset so no need for any hw passthrough into docker

First, rename example.env to .env and fill the file with your:
- Stremio credentials
- PornTube manifest URL
- StashDB API key
- Real-Debrid token
Real-Debrid is optional but recommended in case the addon doesn't have a DDL link and can only grab a torrent. This feature kind of blows right now anyways tho so don't worry about doing this part too much.

## Docker
```
docker compose up -d
````
This should literally just do everything for you.

# Notes
## Thumbnails
By default HereSphere downloads thumbnails straight from their source (Stremio/StashDB). Some headsets can't render webp images, so you can proxy thumbnails through Heremio instead. All of these go in `.env`:

| Variable | Default | What it does |
| --- | --- | --- |
| `THUMB_PROXY` | `False` | Serve thumbnails through `/thumb/<id>` instead of the original URL. Fetches run in parallel and the library is prefetched on startup/refresh. Required for everything below. |
| `THUMB_WORKERS` | `8` | Number of parallel fetches when prefetching thumbnails. |
| `THUMB_CONVERT_WEBP` | `True` | Convert webp thumbnails to jpeg (png/jpg pass through untouched). Only applies with `THUMB_PROXY`. Without the proxy, webp thumbnails will not render on headsets that lack webp support. |
| `THUMB_CACHE_DIR` | *(empty)* | Cache proxied thumbnails on disk. Empty means no disk cache. In Docker use `/app/cache/thumbs` so it lands in the mounted `./cache` folder. |
| `THUMB_CACHE_TTL_DAYS` | `0` | Delete cached thumbnails that haven't been accessed in this many days. `0` keeps them forever. Only applies with `THUMB_CACHE_DIR`. |

## Matching Videos
Because of how PornTube serves VR videos, we need to query StashDB to get proper titles, tags, etc
At the moment, I have pretty broad parsing logic applied, but it's not perfect since a lot of videos just have random title formats and the titles, tags, performers, studios, and dates are just randomly sorted in the string
I prefer to have more false negatives that I can go and manually match than false positives that I have to go and undo
If you visit http://localhost:9000/library you can view matched videos, as well as match videos that were not found on stashdb according to the script
The script will only find a match if the parsed title is 90% similar to the title from stashdb so this should prevent most false positives
I think there is a problem with stashdb querying because sometimes the string we search for on the API will find nothing, but searching it on the website search bar will find exactly what we need, but we can work that out later. Important thing is that this is mostly working.

## First run
On first run it will take a while to match everything manually so it does do some automatic title/tag parsing just so things work out of the box, but sa evyerhting gets matched it becomes more accurate. Stashdb sholud only ever need to be queried once per video anyways.