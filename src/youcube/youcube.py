#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
YouCube Server
"""

# built-in modules
from asyncio import get_running_loop
from base64 import b64encode
from datetime import datetime
from multiprocessing import Manager
from os import getenv, remove
from os.path import exists, join, getsize
from shutil import which
from time import sleep
from typing import Any, List, Tuple, Type, Union

# optional pip module
try:
    from orjson import JSONDecodeError, dumps
    from orjson import loads as load_json
except ModuleNotFoundError:
    from json import dumps
    from json import loads as load_json
    from json.decoder import JSONDecodeError

try:
    from types import UnionType
except ImportError:
    UnionType = Union[int, str]


# pip modules
from sanic import Request, Sanic, Websocket
from sanic.compat import open_async
from sanic.exceptions import SanicException
from sanic.handlers import ErrorHandler
from sanic.response import file as file_response
from sanic.response import file_stream
from sanic.response import raw, text, json as json_response
from spotipy import MemoryCacheHandler, SpotifyClientCredentials
from spotipy.client import Spotify

# local modules
from yc_colours import RESET, Foreground
from yc_download import DATA_FOLDER, FFMPEG_PATH, SANJUUNI_PATH, download
from yc_logging import NO_COLOR, setup_logging
from yc_magic import run_function_in_thread_from_async_function
from yc_spotify import SpotifyURLProcessor
from yc_utils import cap_width_and_height, get_audio_name, get_video_name, is_save
from yc_utils import get_hq_audio_name

VERSION = "0.0.0-poc.1.0.2"
API_VERSION = "0.0.0-poc.1.0.0"  # https://commandcracker.github.io/YouCube/

# one dfpwm chunk is 16 bits
CHUNK_SIZE = 16

"""
CHUNKS_AT_ONCE should not be too big, [CHUNK_SIZE * 1024]
because then the CC Computer cant decode the string fast enough!
Also, it should not be too small because then the client
would need to send thousands of WS messages
and that would also slow everything down! [CHUNK_SIZE * 1]
"""
CHUNKS_AT_ONCE = CHUNK_SIZE * 256


FRAMES_AT_ONCE = 10

# pylint settings
# pylint: disable=pointless-string-statement
# pylint: disable=fixme
# pylint: disable=multiple-statements

"""
Ubuntu nvida support fix and maby alpine support ?
us async base64 ?
use HTTP (and Streaming)
Add uvloop support https://github.com/CC-YouCube/server/issues/6
"""

"""
1 dfpwm chunk = 16
MAX_DOWNLOAD = 16 * 1024 * 1024 = 16777216
WEBSOCKET_MESSAGE = 128 * 1024 = 131072
(MAX_DOWNLOAD = 128 * WEBSOCKET_MESSAGE)

the speaker can accept a maximum of 128 x 1024 samples 16KiB

playAudio
This accepts a list of audio samples as amplitudes between -128 and 127.
These are stored in an internal buffer and played back at 48kHz.
If this buffer is full, this function will return false.
"""

"""Related CC-Tweaked issues
Streaming HTTP response https://github.com/cc-tweaked/CC-Tweaked/issues/1181
Speaker Networks        https://github.com/cc-tweaked/CC-Tweaked/issues/1488
Pocket computers do not have many usecases without network access
https://github.com/cc-tweaked/CC-Tweaked/issues/1406
Speaker limit to 8      https://github.com/cc-tweaked/CC-Tweaked/issues/1313
Some way to notify player through pocket computer with modem
https://github.com/cc-tweaked/CC-Tweaked/issues/1148
Memory limits for computers https://github.com/cc-tweaked/CC-Tweaked/issues/1580
"""

"""TODO: Add those:
AudioDevices:
 - Speaker Note (Sound)  https://tweaked.cc/peripheral/speaker.html
 - Notblock              https://www.youtube.com/watch?v=XY5UvTxD9dA
 - Create Steam whistles https://www.youtube.com/watch?v=dgZ4F7U19do
                         https://github.com/danielathome19/MIDIToComputerCraft/tree/master

Video Formats:
 - 32vid binary https://github.com/MCJack123/sanjuuni
 - qtv          https://github.com/Axisok/qtccv

Audio Formats:
 - DFPWM ffmpeg fallback ? https://github.com/asiekierka/pixmess/blob/master/scraps/aucmp.py
 - PCM
 - NBS  https://github.com/Xella37/NBS-Tunes-CC
 - MIDI https://github.com/OpenPrograms/Sangar-Programs/blob/master/midi.lua
 - XM   https://github.com/MCJack123/tracc

Audio u. Video preview / thumbnail:
 - NFP  https://tweaked.cc/library/cc.image.nft.html
 - bimg https://github.com/SkyTheCodeMaster/bimg
 - as 1 qtv frame
 - as 1 32vid frame
"""

logger = setup_logging()
# TODO: change sanic logging format


def external_base_url(request: Request) -> str:
    """Returns the public HTTP base URL for this request."""
    forwarded_proto = request.headers.get("x-forwarded-proto")
    scheme = forwarded_proto or ("https" if request.scheme == "wss" else request.scheme)
    if scheme == "ws":
        scheme = "http"
    elif scheme == "wss":
        scheme = "https"

    host = request.headers.get("x-forwarded-host") or request.headers.get("host")
    return f"{scheme}://{host}"


async def get_vid(vid_file: str, tracker: int, frames_count: int = FRAMES_AT_ONCE) -> List[str]:
    """Returns given line of 32vid file"""
    async with await open_async(file=vid_file, mode="rb") as file:
        await file.seek(tracker)
        lines = []
        for _unused in range(frames_count):
            line = await file.readline()
            if not line:
                break
            if line.endswith(b"\r\n"):
                line = line[:-2]
            elif line.endswith(b"\n"):
                line = line[:-1]
            lines.append(line.decode("utf-8", errors="ignore"))

    return lines


async def getchunk(media_file: str, chunkindex: int) -> bytes:
    """Returns a chunk of the given media file"""
    async with await open_async(file=media_file, mode="rb") as file:
        await file.seek(chunkindex * CHUNKS_AT_ONCE)
        return await file.read(CHUNKS_AT_ONCE)


# pylint: enable=redefined-outer-name


def assert_resp(
    __obj_name: str,
    __obj: Any,
    __class_or_tuple: Union[
        Type, UnionType, Tuple[Union[Type, UnionType, Tuple[Any, ...]], ...]
    ],
) -> Union[dict, None]:
    """
    "assert" / isinstance that returns a dict that can be send as a ws response
    """
    if not isinstance(__obj, __class_or_tuple):
        return {
            "action": "error",
            "message": f"{__obj_name} must be a {__class_or_tuple.__name__}",
        }
    return None


# pylint: disable=duplicate-code
spotify_client_id = getenv("SPOTIPY_CLIENT_ID")
spotify_client_secret = getenv("SPOTIPY_CLIENT_SECRET")
# pylint: disable-next=invalid-name
spotipy = None

if spotify_client_id and spotify_client_secret:
    spotipy = Spotify(
        auth_manager=SpotifyClientCredentials(
            client_id=spotify_client_id,
            client_secret=spotify_client_secret,
            cache_handler=MemoryCacheHandler(),
        )
    )

# pylint: disable-next=invalid-name
spotify_url_processor = None
if spotipy:
    spotify_url_processor = SpotifyURLProcessor(spotipy)

# pylint: enable=duplicate-code


class Actions:
    """
    Default set of actions
    Every action needs to be called with a message and needs to return a dict response
    """

    # pylint: disable=missing-function-docstring

    @staticmethod
    async def request_media(message: dict, resp: Websocket, request: Request):
        loop = get_running_loop()
        # get "url"
        url = message.get("url")
        if error := assert_resp("url", url, str):
            return error
        # TODO: assert_resp width and height
        out, files = await run_function_in_thread_from_async_function(
            download,
            url,
            resp,
            loop,
            message.get("width"),
            message.get("height"),
            bool(message.get("hq_audio")),
            spotify_url_processor,
        )
        if message.get("hq_audio") and out.get("action") == "media":
            out["audio_format"] = "mp3"
            out["audio_url"] = f"{external_base_url(request)}/mp3/{out.get('id')}"
        for file in files:
            request.app.shared_ctx.data[file] = datetime.now()
        return out

    @staticmethod
    async def get_chunk(message: dict, _unused, request: Request):
        # get "chunkindex"
        chunkindex = message.get("chunkindex")
        if error := assert_resp("chunkindex", chunkindex, int):
            return error

        # get "id"
        media_id = message.get("id")
        if error := assert_resp("media_id", media_id, str):
            return error

        if is_save(media_id):
            file_name = get_audio_name(message.get("id"))
            file = join(DATA_FOLDER, file_name)

            request.app.shared_ctx.data[file_name] = datetime.now()
            chunk = await getchunk(file, chunkindex)

            return {"action": "chunk", "chunk": b64encode(chunk).decode("ascii")}
        logger.warning("User tried to use special Characters")
        return {"action": "error", "message": "You dare not use special Characters"}

    @staticmethod
    async def get_vid(message: dict, _unused, request: Request):
        # get "line"
        tracker = message.get("tracker")
        if error := assert_resp("tracker", tracker, int):
            return error

        # get "id"
        media_id = message.get("id")
        if error := assert_resp("id", media_id, str):
            return error

        # get "width"
        width = message.get("width")
        if error := assert_resp("width", width, int):
            return error

        # get "height"
        height = message.get("height")
        if error := assert_resp("height", height, int):
            return error

        # cap height and width
        width, height = cap_width_and_height(width, height)

        if is_save(media_id):
            file_name = get_video_name(message.get("id"), width, height)
            file = join(DATA_FOLDER, file_name)

            request.app.shared_ctx.data[file_name] = datetime.now()

            # Dynamic FRAMES_AT_ONCE calculation to keep message size under ~50KB
            char_width, char_height = width // 2, height // 3
            frame_size = char_width * char_height * 1.35
            frames_to_read = max(1, min(FRAMES_AT_ONCE, int(50000 // max(1.0, frame_size))))

            return {"action": "vid", "lines": await get_vid(file, tracker, frames_to_read)}

        return {"action": "error", "message": "You dare not use special Characters"}

    @staticmethod
    async def handshake(*_unused):
        return {
            "action": "handshake",
            "server": {"version": VERSION},
            "api": {"version": API_VERSION},
            "capabilities": {
                "video": ["32vid", "ccdirectgpu"],
                "audio": ["dfpwm", "mp3", "cchq-speakers"],
            },
        }

    # pylint: enable=missing-function-docstring


class CustomErrorHandler(ErrorHandler):
    """Error handler for sanic"""

    def default(self, request: Request, exception: Union[SanicException, Exception]):
        """handles errors that have no error handlers assigned"""

        if isinstance(exception, SanicException) and exception.status_code == 426:
            # TODO: Respond with nice html that tells the user how to install YC
            return text(
                "You cannot access a YouCube server directly. "
                "You need the YouCube client. "
                "See https://youcube.madefor.cc/guides/client/installation/"
            )

        return super().default(request, exception)


app = Sanic("youcube")
app.error_handler = CustomErrorHandler()
# FIXME: The Client is not Responsing to Websocket pings
app.config.WEBSOCKET_PING_INTERVAL = 0
# FIXME: Add UVLOOP support for alpine pypy
if getenv("SANIC_NO_UVLOOP"):
    app.config.USE_UVLOOP = False

actions = {}

# add all actions from default action set
for method in dir(Actions):
    if not method.startswith("__"):
        actions[method] = getattr(Actions, method)


DATA_CACHE_CLEANUP_INTERVAL = int(getenv("DATA_CACHE_CLEANUP_INTERVAL", "300"))
DATA_CACHE_CLEANUP_AFTER = int(getenv("DATA_CACHE_CLEANUP_AFTER", "3600"))


def data_cache_cleaner(data: dict):
    """
    Checks for outdated cache entries every DATA_CACHE_CLEANUP_INTERVAL (default 300) Seconds and
    deletes them if they have not been used for DATA_CACHE_CLEANUP_AFTER (default 3600) Seconds.
    """
    try:
        while True:
            sleep(DATA_CACHE_CLEANUP_INTERVAL)
            for file_name, last_used in data.items():
                if (
                    datetime.now() - last_used
                ).total_seconds() > DATA_CACHE_CLEANUP_AFTER:
                    file_path = join(DATA_FOLDER, file_name)
                    if exists(file_path):
                        remove(file_path)
                        logger.debug('Deleted "%s"', file_name)
                    data.pop(file_name)

    except KeyboardInterrupt:
        pass


# pylint: disable=redefined-outer-name
@app.main_process_ready
async def ready(app: Sanic, _):
    """See https://sanic.dev/en/guide/basics/listeners.html"""
    if DATA_CACHE_CLEANUP_INTERVAL > 0 and DATA_CACHE_CLEANUP_AFTER > 0:
        app.manager.manage(
            "Data-Cache-Cleaner", data_cache_cleaner, {"data": app.shared_ctx.data}
        )


@app.main_process_start
async def main_start(app: Sanic):
    """See https://sanic.dev/en/guide/basics/listeners.html"""
    app.shared_ctx.data = Manager().dict()

    if which(FFMPEG_PATH) is None:
        logger.warning("FFmpeg not found.")

    if which(SANJUUNI_PATH) is None:
        logger.warning("Sanjuuni not found.")

    if spotipy:
        logger.info("Spotipy Enabled")
    else:
        logger.info("Spotipy Disabled")


@app.route("/dfpwm/<media_id:str>/<chunkindex:int>")
async def stream_dfpwm(_request: Request, media_id: str, chunkindex: int):
    """WIP HTTP mode"""
    return raw(await getchunk(join(DATA_FOLDER, get_audio_name(media_id)), chunkindex))


@app.route("/mp3/<media_id:str>")
async def stream_mp3(_request: Request, media_id: str):
    """Streams MP3 audio for CC:HQ Speakers."""
    if not is_save(media_id):
        return text("Invalid media id", status=400)
    return await file_response(
        join(DATA_FOLDER, get_hq_audio_name(media_id)),
        mime_type="audio/mpeg",
    )


@app.route("/32vid/<media_id:str>/<width:int>/<height:int>/<tracker:int>")  # , stream=True
async def stream_32vid(
    _request: Request, media_id: str, width: int, height: int, tracker: int
):
    """WIP HTTP mode"""
    return raw(
        "\n".join(
            await get_vid(join(DATA_FOLDER, get_video_name(media_id, width, height)), tracker)
        )
    )


line_offsets_cache = {}

async def get_line_offsets(file_path: str) -> List[int]:
    if file_path in line_offsets_cache:
        return line_offsets_cache[file_path]
    
    offsets = []
    async with await open_async(file_path, mode="rb") as f:
        offset = 0
        while True:
            offsets.append(offset)
            line = await f.readline()
            if not line:
                offsets.pop() # Remove the offset of EOF
                break
            offset += len(line)
            
    line_offsets_cache[file_path] = offsets
    return offsets


HLS_VIDEO_FRAMES_PER_SEGMENT = 10
HLS_AUDIO_CHUNKS_PER_SEGMENT = 10

@app.route("/hls/request")
async def hls_request(request: Request):
    import math
    url = request.args.get("url")
    width_str = request.args.get("width")
    height_str = request.args.get("height")
    hq_audio = request.args.get("hq_audio") == "true"

    if not url:
        return json_response({"error": "Missing url parameter"}, status=400)
    
    try:
        width = int(width_str) if width_str else 656
        height = int(height_str) if height_str else 324
    except ValueError:
        return json_response({"error": "Invalid width or height"}, status=400)

    # Use larger caps for HLS mode (up to 1920x1080)
    width, height = cap_width_and_height(width, height, max_width=1920, max_height=1080)

    loop = get_running_loop()
    
    class DummyWS:
        async def send(self, *args, **kwargs):
            pass
    
    dummy_ws = DummyWS()
    
    out, files = await run_function_in_thread_from_async_function(
        download,
        url,
        dummy_ws,
        loop,
        width,
        height,
        hq_audio,
        spotify_url_processor,
    )
    
    for file in files:
        request.app.shared_ctx.data[file] = datetime.now()
        
    if out.get("action") == "error":
        return json_response(out, status=500)
        
    video_file_name = get_video_name(out["id"], width, height)
    video_file = join(DATA_FOLDER, video_file_name)
    
    try:
        offsets = await get_line_offsets(video_file)
        video_segments_count = math.ceil(len(offsets) / HLS_VIDEO_FRAMES_PER_SEGMENT)
    except Exception:
        video_segments_count = 0

    audio_file_name = get_audio_name(out["id"])
    audio_file = join(DATA_FOLDER, audio_file_name)
    try:
        audio_size = getsize(audio_file)
        audio_segments_count = math.ceil(audio_size / (HLS_AUDIO_CHUNKS_PER_SEGMENT * CHUNKS_AT_ONCE))
    except Exception:
        audio_segments_count = 0

    if hq_audio and out.get("action") == "media":
        out["audio_format"] = "mp3"
        out["audio_url"] = f"{external_base_url(request)}/mp3/{out.get('id')}"

    out["video_segments_count"] = video_segments_count
    out["audio_segments_count"] = audio_segments_count
    return json_response(out)


@app.route("/hls/audio/<media_id:str>/<segment_index:int>")
async def hls_audio(request: Request, media_id: str, segment_index: int):
    channel = request.args.get("channel", "mono")
    
    if channel == "left":
        file_name = get_audio_name(f"{media_id}_left")
    elif channel == "right":
        file_name = get_audio_name(f"{media_id}_right")
    else:
        file_name = get_audio_name(media_id)
        
    file = join(DATA_FOLDER, file_name)
    
    if not is_save(media_id) or not exists(file):
        return text("", status=404)
        
    segment_size = HLS_AUDIO_CHUNKS_PER_SEGMENT * CHUNKS_AT_ONCE
    async with await open_async(file=file, mode="rb") as f:
        await f.seek(segment_index * segment_size)
        chunk = await f.read(segment_size)
        return raw(chunk, content_type="application/octet-stream")


@app.route("/hls/video/<media_id:str>/<width:int>x<height:int>/<segment_index:int>")
async def hls_video(request: Request, media_id: str, width: int, height: int, segment_index: int):
    width, height = cap_width_and_height(width, height, max_width=1920, max_height=1080)
    file_name = get_video_name(media_id, width, height)
    file = join(DATA_FOLDER, file_name)
    
    if not is_save(media_id) or not exists(file):
        return text("", status=404)
        
    try:
        offsets = await get_line_offsets(file)
        start_frame = segment_index * HLS_VIDEO_FRAMES_PER_SEGMENT
        if start_frame < 0 or start_frame >= len(offsets):
            return text("", status=404)
            
        offset = offsets[start_frame]
        async with await open_async(file=file, mode="rb") as f:
            await f.seek(offset)
            lines = []
            for _ in range(HLS_VIDEO_FRAMES_PER_SEGMENT):
                line = await f.readline()
                if not line:
                    break
                if line.endswith(b"\r\n"):
                    line = line[:-2]
                elif line.endswith(b"\n"):
                    line = line[:-1]
                lines.append(line.decode("utf-8", errors="ignore"))
            return text("\n".join(lines))
    except Exception as e:
        logger.error("HLS Video error: %s", e)
        return text("", status=500)


@app.route("/dfpwm/<id:str>")
async def stream_dfpwm_file(request: Request, id: str):
    file_name = get_audio_name(id)
    file = join(DATA_FOLDER, file_name)
    if not is_save(id) or not exists(file):
        return text("Not found", status=404)
    return await file_response(
        file,
        mime_type="application/octet-stream",
    )


@app.route("/32vid/<id:str>/<width:int>/<height:int>")
async def stream_32vid_file(request: Request, id: str, width: int, height: int):
    width, height = cap_width_and_height(width, height, max_width=1920, max_height=1080)
    file_name = get_video_name(id, width, height)
    file = join(DATA_FOLDER, file_name)
    if not is_save(id) or not exists(file):
        return text("Not found", status=404)
    return await file_stream(
        file,
        chunk_size=4096,
        mime_type="text/plain",
    )
# pylint: enable=redefined-outer-name


@app.websocket("/")
# pylint: disable-next=invalid-name
async def wshandler(request: Request, ws: Websocket):
    """Handels web-socket requests"""
    if NO_COLOR:
        prefix = f"[{request.client_ip}] "
    else:
        prefix = f"{Foreground.BLUE}[{request.client_ip}]{RESET} "

    logger.info("%sConnected!", prefix)

    logger.debug("%sMy headers are: %s", prefix, request.headers)

    while True:
        message = await ws.recv()
        logger.debug("%sMessage: %s", prefix, message)

        try:
            message: dict = load_json(message)
        except JSONDecodeError:
            logger.debug("%sFaild to parse Json", prefix)
            await ws.send(dumps({"action": "error", "message": "Faild to parse Json"}))

        if message.get("action") in actions:
            response = await actions[message.get("action")](message, ws, request)
            await ws.send(dumps(response))


def main() -> None:
    """
    Run all needed services
    """
    port = int(getenv("PORT", "5000"))
    host = getenv("HOST", "127.0.0.1")
    fast = not getenv("NO_FAST")

    app.run(host=host, port=port, fast=fast, access_log=True)


if __name__ == "__main__":
    main()
