#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Utils for string manipulation, data management etc.
"""

# Built-in modules
from os import mkdir
from os.path import abspath, dirname, exists, join
from re import RegexFlag
from re import compile as re_compile
from typing import Tuple


def remove_whitespace(string: str) -> str:
    """
    Removes all Spaces / Whitespace from a string
    """
    return string.replace(" ", "")


# Only compile "ansi_escape_codes" once
ansi_escape_codes = re_compile(
    r"""
    \x1B  # ESC
    (?:   # 7-bit C1 Fe (except CSI)
        [@-Z\\-_]
    |     # or [ for CSI, followed by a control sequence
        \[
        [0-?]*  # Parameter bytes
        [ -/]*  # Intermediate bytes
        [@-~]   # Final byte
    )
""",
    RegexFlag.VERBOSE,
)


def remove_ansi_escape_codes(text: str) -> str:
    """
    Remove all Ansi Escape codes
    (7-bit C1 ANSI sequences)
    """
    return ansi_escape_codes.sub("", text)


def cap_width(width: int) -> int:
    """Caps the width"""
    return min(width, 656)


def cap_height(height: int) -> int:
    """Caps the height"""
    return min(height, 324)


def cap_width_and_height(width: int, height: int, max_width: int = 656, max_height: int = 324) -> Tuple[int, int]:
    """Caps the width and height"""
    return min(width, max_width), min(height, max_height)


VIDEO_FORMAT = "32vid"
AUDIO_FORMAT = "dfpwm"
HQ_AUDIO_FORMAT = "mp3"
DATA_FOLDER = join(dirname(abspath(__file__)), "data")


def get_video_name(media_id: str, width: int, height: int) -> str:
    """Returns the file name of the requested video"""
    return f"{media_id}({width}x{height}).{VIDEO_FORMAT}"


def get_audio_name(media_id: str) -> str:
    """Returns the file name of the requested audio"""
    return f"{media_id}.{AUDIO_FORMAT}"


def get_hq_audio_name(media_id: str) -> str:
    """Returns the file name of the high-quality audio file"""
    return f"{media_id}.{HQ_AUDIO_FORMAT}"


def get_video_path(media_id: str, width: int, height: int) -> str:
    """Returns the relative path to the requested video"""
    return join(DATA_FOLDER, get_video_name(media_id, width, height))


def get_audio_path(media_id: str) -> str:
    """Returns the relative path to the requested audio"""
    return join(DATA_FOLDER, get_audio_name(media_id))


def get_hq_audio_path(media_id: str) -> str:
    """Returns the relative path to the high-quality audio file"""
    return join(DATA_FOLDER, get_hq_audio_name(media_id))


def create_data_folder_if_not_present():
    """Creates the data folder if it does not exist"""
    if not exists(DATA_FOLDER):
        mkdir(DATA_FOLDER)


def is_audio_already_downloaded(media_id: str) -> bool:
    """Returns True if the given audio is already downloaded"""
    return (
        exists(get_audio_path(media_id))
        and exists(get_audio_path(f"{media_id}_left"))
        and exists(get_audio_path(f"{media_id}_right"))
    )


def is_hq_audio_already_downloaded(media_id: str) -> bool:
    """Returns True if the given high-quality audio is already downloaded"""
    return exists(get_hq_audio_path(media_id))


def is_video_already_downloaded(media_id: str, width: int, height: int) -> bool:
    """Returns True if the given video is already downloaded"""
    return exists(get_video_path(media_id, width, height))


# Only compile "allowed_characters" once
allowed_characters = re_compile("^[a-zA-Z0-9-._]*$")


def is_save(string: str) -> bool:
    """Returns True if the given string does not contain special characters"""
    return bool(allowed_characters.match(string))
