#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Download Functionality of YC
"""

# Built-in modules
from asyncio import run_coroutine_threadsafe
import hashlib
from os import getenv, listdir, makedirs
from os.path import abspath, dirname, exists, join
from shutil import copy2
from tempfile import TemporaryDirectory

# Local modules
from yc_colours import RESET, Foreground
from yc_logging import NO_COLOR, YTDLPLogger, logger
from yc_magic import run_with_live_output
from yc_spotify import SpotifyURLProcessor
from yc_utils import (
    cap_width_and_height,
    create_data_folder_if_not_present,
    get_audio_name,
    get_hq_audio_name,
    get_video_name,
    is_audio_already_downloaded,
    is_hq_audio_already_downloaded,
    is_video_already_downloaded,
    remove_ansi_escape_codes,
    remove_whitespace,
)

# optional pip modules
try:
    from orjson import dumps
except ModuleNotFoundError:
    from json import dumps

# pip modules
from sanic import Websocket
from yt_dlp import YoutubeDL

# pylint settings
# pylint: disable=pointless-string-statement
# pylint: disable=fixme
# pylint: disable=too-many-locals
# pylint: disable=too-many-arguments
# pylint: disable=too-many-branches

DATA_FOLDER = join(dirname(abspath(__file__)), "data")
FFMPEG_PATH = getenv("FFMPEG_PATH", "ffmpeg")
SANJUUNI_PATH = getenv("SANJUUNI_PATH", "sanjuuni")
DISABLE_OPENCL = bool(getenv("DISABLE_OPENCL"))


def download_video(
    temp_dir: str, media_id: str, resp: Websocket, loop, width: int, height: int
):
    """
    Converts the downloaded video to 32vid
    """
    input_files = listdir(temp_dir)
    if not input_files:
        logger.warning("No downloaded video file found in temp_dir.")
        run_coroutine_threadsafe(
            resp.send(dumps({"action": "error", "message": "Faild to convert video!"})),
            loop,
        )
        return

    input_file = join(temp_dir, input_files[0])
    target_filepath = join(DATA_FOLDER, get_video_name(media_id, width, height))

    # Compute input hash
    hasher = hashlib.sha256()
    with open(input_file, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hasher.update(chunk)
    input_hash = hasher.hexdigest()

    cache_dir = join(DATA_FOLDER, "sanjuuni_cache")
    makedirs(cache_dir, exist_ok=True)
    cache_filename = f"{input_hash}_{width}x{height}_{'no_opencl' if DISABLE_OPENCL else 'opencl'}.32vid"
    cache_filepath = join(cache_dir, cache_filename)

    if exists(cache_filepath):
        logger.info("Using cached Sanjuuni result for input %s", input_hash)
        run_coroutine_threadsafe(
            resp.send(
                dumps({"action": "status", "message": "Using cached video conversion ..."})
            ),
            loop,
        )
        copy2(cache_filepath, target_filepath)
        return

    run_coroutine_threadsafe(
        resp.send(
            dumps({"action": "status", "message": "Converting video to 32vid ..."})
        ),
        loop,
    )

    if NO_COLOR:
        prefix = "[Sanjuuni]"
    else:
        prefix = f"{Foreground.BRIGHT_YELLOW}[Sanjuuni]{RESET} "

    def handler(line):
        logger.debug("%s%s", prefix, line)
        run_coroutine_threadsafe(
            resp.send(dumps({"action": "status", "message": line})), loop
        )

    returncode = run_with_live_output(
        [
            SANJUUNI_PATH,
            "--width=" + str(width),
            "--height=" + str(height),
            "-i",
            input_file,
            "--raw",
            "-o",
            target_filepath,
            "--disable-opencl" if DISABLE_OPENCL else "",
        ],
        handler,
    )

    if returncode == 0:
        if exists(target_filepath):
            copy2(target_filepath, cache_filepath)
            logger.info("Cached Sanjuuni result to %s", cache_filename)
    else:
        logger.warning("Sanjuuni exited with %s", returncode)
        run_coroutine_threadsafe(
            resp.send(dumps({"action": "error", "message": "Faild to convert video!"})),
            loop,
        )


def download_audio(temp_dir: str, media_id: str, resp: Websocket, loop):
    """
    Converts the downloaded audio to dfpwm
    """
    run_coroutine_threadsafe(
        resp.send(
            dumps({"action": "status", "message": "Converting audio to dfpwm ..."})
        ),
        loop,
    )

    if NO_COLOR:
        prefix = "[FFmpeg]"
    else:
        prefix = f"{Foreground.BRIGHT_GREEN}[FFmpeg]{RESET} "

    def handler(line):
        logger.debug("%s%s", prefix, line)
        # TODO: send message to resp

    returncode = run_with_live_output(
        [
            FFMPEG_PATH,
            "-i",
            join(temp_dir, listdir(temp_dir)[0]),
            "-f",
            "dfpwm",
            "-ar",
            "48000",
            "-ac",
            "1",
            join(DATA_FOLDER, get_audio_name(media_id)),
        ],
        handler,
    )

    if returncode != 0:
        logger.warning("FFmpeg exited with %s", returncode)
        run_coroutine_threadsafe(
            resp.send(dumps({"action": "error", "message": "Faild to convert audio!"})),
            loop,
        )


def download_hq_audio(temp_dir: str, media_id: str, resp: Websocket, loop):
    """
    Converts the downloaded audio to MP3 for CC:HQ Speakers.
    """
    run_coroutine_threadsafe(
        resp.send(dumps({"action": "status", "message": "Converting audio to mp3 ..."})),
        loop,
    )

    if NO_COLOR:
        prefix = "[FFmpeg]"
    else:
        prefix = f"{Foreground.BRIGHT_GREEN}[FFmpeg]{RESET} "

    def handler(line):
        logger.debug("%s%s", prefix, line)

    returncode = run_with_live_output(
        [
            FFMPEG_PATH,
            "-i",
            join(temp_dir, listdir(temp_dir)[0]),
            "-vn",
            "-codec:a",
            "libmp3lame",
            "-b:a",
            getenv("HQ_AUDIO_BITRATE", "192k"),
            join(DATA_FOLDER, get_hq_audio_name(media_id)),
        ],
        handler,
    )

    if returncode != 0:
        logger.warning("FFmpeg exited with %s", returncode)
        run_coroutine_threadsafe(
            resp.send(dumps({"action": "error", "message": "Faild to convert hq audio!"})),
            loop,
        )


def download(
    url: str,
    resp: Websocket,
    loop,
    width: int,
    height: int,
    hq_audio: bool,
    spotify_url_processor: SpotifyURLProcessor,
) -> (dict[str, any], list):
    """
    Downloads and converts the media from the give URL
    """

    is_video = width is not None and height is not None

    # cap height and width
    if width and height:
        width, height = cap_width_and_height(width, height)

    def my_hook(info):
        """https://github.com/yt-dlp/yt-dlp#adding-logger-and-progress-hook"""
        if info.get("status") == "downloading":
            run_coroutine_threadsafe(
                resp.send(
                    dumps(
                        {
                            "action": "status",
                            "message": remove_ansi_escape_codes(
                                f"download {remove_whitespace(info.get('_percent_str'))} "
                                f"ETA {info.get('_eta_str')}"
                            ),
                        }
                    )
                ),
                loop,
            )

    # FIXME: Cleanup on Exception
    with TemporaryDirectory(prefix="youcube-") as temp_dir:
        yt_dl_options = {
            "format": "wv+wa" if is_video else "wa",
            "outtmpl": join(temp_dir, "%(id)s.%(ext)s"),
            "default_search": "auto",
            "restrictfilenames": True,
            "extract_flat": "in_playlist",
            "progress_hooks": [my_hook],
            "logger": YTDLPLogger(),
        }

        yt_dl = YoutubeDL(yt_dl_options)

        run_coroutine_threadsafe(
            resp.send(
                dumps(
                    {"action": "status", "message": "Getting resource information ..."}
                )
            ),
            loop,
        )

        playlist_videos = []

        if spotify_url_processor:
            # Spotify FIXME: The first media key is sometimes duplicated
            processed_url = spotify_url_processor.auto(url)
            if processed_url:
                if isinstance(processed_url, list):
                    url = spotify_url_processor.auto(processed_url[0])
                    processed_url.pop(0)
                    playlist_videos = processed_url
                else:
                    url = processed_url

        data = yt_dl.extract_info(url, download=False)

        if data.get("extractor") == "generic":
            data["id"] = "g" + data.get("webpage_url_domain") + data.get("id")

        """
        If the data is a playlist, we need to get the first video and return it,
        also, we need to grep all video in the playlist to provide support.
        """
        if data.get("_type") == "playlist":
            for video in data.get("entries"):
                playlist_videos.append(video.get("id"))

            playlist_videos.pop(0)

            data = data["entries"][0]

        """
        If the video is extract from a playlist,
        the video is extracted flat,
        so we need to get missing information by running the extractor again.
        """
        if data.get("extractor") == "youtube" and (
            data.get("view_count") is None or data.get("like_count") is None
        ):
            data = yt_dl.extract_info(data.get("id"), download=False)

        media_id = data.get("id")

        if data.get("is_live"):
            return {"action": "error", "message": "Livestreams are not supported"}

        create_data_folder_if_not_present()

        audio_downloaded = is_audio_already_downloaded(media_id)
        hq_audio_downloaded = is_hq_audio_already_downloaded(media_id)
        video_downloaded = is_video_already_downloaded(media_id, width, height)

        if (
            not audio_downloaded
            or (hq_audio and not hq_audio_downloaded)
            or (not video_downloaded and is_video)
        ):
            run_coroutine_threadsafe(
                resp.send(
                    dumps({"action": "status", "message": "Downloading resource ..."})
                ),
                loop,
            )

            yt_dl.process_ie_result(data, download=True)

        # TODO: Thread audio & video download

        if not audio_downloaded:
            download_audio(temp_dir, media_id, resp, loop)

        if hq_audio and not hq_audio_downloaded:
            download_hq_audio(temp_dir, media_id, resp, loop)

        if not video_downloaded and is_video:
            download_video(temp_dir, media_id, resp, loop, width, height)

    out = {
        "action": "media",
        "id": media_id,
        # "fulltitle": data.get("fulltitle"),
        "title": data.get("title"),
        "like_count": data.get("like_count"),
        "view_count": data.get("view_count"),
        # "upload_date": data.get("upload_date"),
        # "tags": data.get("tags"),
        # "description": data.get("description"),
        # "categories": data.get("categories"),
        # "channel_name": data.get("channel"),
        # "channel_id": data.get("channel_id")
    }

    # Only return playlist_videos if there are videos in playlist_videos
    if len(playlist_videos) > 0:
        out["playlist_videos"] = playlist_videos

    files = []
    files.append(get_audio_name(media_id))
    if hq_audio:
        files.append(get_hq_audio_name(media_id))
    if is_video:
        files.append(get_video_name(media_id, width, height))

    return out, files
