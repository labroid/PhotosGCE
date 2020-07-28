"""
if first run
    Get list of zip files and put in persistent queue  
For each zip file in queue  
    download to disk  
    unzip to disk
    for each member  
        if smaller than 100 MB  
            get file stats
            compute MD5sum
            extract tags
            compute MD5image
        else  
            extract to disk  
            extract file stats  
        save parent zip file name, filepath, MD5sum, file stats, tags, MD5image
close fs
"""

import fs
import hashlib
from PIL import Image, UnidentifiedImageError, ImageFile
from exifread import process_file
from fs.copy import copy_fs
from fs.zipfs import ZipFS
import json
from loguru import logger

logger.add("stats_from_zips.log", rotation="1 MB")

UNWANTED_TAGS = ["MakerNote", "JPEGThumbnail"]
ZIP_QUEUE = "zip_queue"
ZIP_SOURCE = "~/Drive/Takeout"
# ZIP_SOURCE = "~"
RESULTS_FILE = "./results.json"
CHUNK_SIZE = int(100_000_000)

ImageFile.LOAD_TRUNCATED_IMAGES = True

# Future update: Make this a job queue
# # Only run this once to initialize ZIP file queue
# q = pq.SQLiteAckQueue(ZIP_QUEUE)
# with fs.open_fs(ZIP_SOURCE) as zipdir:
#     for z in zipdir.glob('*.zip'):
#         q.put(z.path)

#
# # Run this if not initializing ZIP file queue
# q = pq.SQLiteAckQueue(ZIP_QUEUE, auto_commit=True)

localdir = fs.open_fs(".")

logger.info(f"Getting zipfiles from {ZIP_SOURCE}")
with fs.open_fs(ZIP_SOURCE) as zipdir:
    ziplist = [
        ZipFS(fs.path.combine(zipdir.getsyspath("/"), z))
        for z in (zipdir.walk.files(filter=["*.zip"], max_depth=1))
    ]
for zipfs in ziplist:
    with fs.open_fs("mem://") as cache:
        logger.info(f"Extracting from {zipfs}")
        copy_fs(zipfs, cache)
        for unzipped in cache.walk.files():
            logger.info(f"Processing file {unzipped}")
            result = {"zipfile": zipfs._file, "path": unzipped.lstrip("/")}
            info = cache.getinfo(unzipped, namespaces=["details"])
            result["name"] = info.name
            result["size"] = info.size

            with cache.open(unzipped, mode="rb") as fp:
                md5 = hashlib.md5()
                while True:
                    data = fp.read(CHUNK_SIZE)
                    if not data:
                        break
                    md5.update(data)
                result["md5"] = md5.hexdigest()

                fp.seek(0)
                try:
                    im = Image.open(fp)
                    result["md5image"] = hashlib.md5(im.tobytes()).hexdigest()
                except UnidentifiedImageError:
                    result["md5image"] = ""

                fp.seek(0)
                try:
                    exif = process_file(fp)
                    result["exif"] = {
                        k: str(v) for k, v in exif.items() if k not in UNWANTED_TAGS
                    }
                except Exception as e:
                    result["exif"] = {"Exception": e}

            localdir.appendtext(RESULTS_FILE, json.dumps(result))

logger.info("Done")
