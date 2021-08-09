import asyncio
import json
import logging
import pandas as pd
import requests


def get_streams(cr, page_size, viewers_threshold):
    """
    Get streams which have viewers more than viewers_threshold via Twitch API.

    The streams data is used to create a dictionary in the form of {streamer login name: streamer display name}
    
    Credentials must be defined and imported as a dictionary. Maximum page_size is 100.
    """

    streamers = {}
    cursor = ''
    page = 1

    logging.info('Getting stream that has more than %d viewers, each page consists of %d streams', viewers_threshold, page_size)
    while True:
        url = f'https://api.twitch.tv/helix/streams?first={page_size}&language=th&after={cursor}'

        # Result are the streams sorted by number of current viewers in descending order.
        logging.info('Getting streams: Page %d', page)
        r = requests.get(url, headers = {'Authorization': 'Bearer ' + cr['access_token'], 'Client-Id': cr['client_id']})
        result = r.json()

        # Get the lowest number of current viewers in stream in current page.
        page_min_viewers = result['data'][-1]['viewer_count']

        for stream in result['data']:
            if (stream['viewer_count'] > viewers_threshold and stream['user_login'] not in streamers):

                # Use streamer's login name as a key.
                login = stream['user_login']

                # Use streamer's display name as a value.
                # If streamer's display name is not in english, use login name as a value instead.
                if stream['user_name'].upper() != stream['user_name'].lower():
                    streamers[login] = stream['user_name']
                else:
                    streamers[login] = stream['user_login']

        # Stop getting more result if number of current viewers in the page reach the threshold.
        if (page_min_viewers < viewers_threshold):
            logging.info('Completed getting streams, added %d streamers to dictionary', len(streamers))
            break

        cursor = result['pagination']['cursor']
        page += 1

    return streamers


async def get_viewers_from_channel(session, channel_login, channel_name):
    """
    Get viewers from tmi.twitch.tv by using login name (channel_login) of a streamer.
    """

    url = f'https://tmi.twitch.tv/group/user/{channel_login}/chatters'

    logging.debug('Getting viewers from channel: %s', channel_name)
    async with session.get(url) as response:
        result = await response.json()
        try:
            viewers = result['chatters']['vips'] + result['chatters']['viewers']
        except:
            viewers = []
    logging.debug('Completed getting viewers from channel: %s', channel_name)

    return channel_name, viewers


async def get_viewers(session, streamers):
    """
    Get viewers from each streamer's channel and add the data to dictionary asynchronously.
    """

    data = {}
    coroutine = [get_viewers_from_channel(session, user_login, streamers[user_login]) for user_login in streamers]

    # Get viewers and store the data in form of {streamer: list of viewers}
    logging.info('Getting viewers from each channel')
    for coru in asyncio.as_completed(coroutine):
        streamer_name, viewers = await coru
        data[streamer_name] = viewers
    logging.info('Completed getting viewers')

    return data


def update_data(file_name, recent_data):
    """
    Add new key and value in recent data to exist data (file_name).
    """

    try:
        logging.info('Importing data to be merged')
        with open(f'../data/{file_name}.json') as read_file:
            exist_data = json.load(read_file)

        count_merge = 0
        count_add = 0

        logging.info('Merging data, exist data have %d streamers', len(exist_data))
        for streamer in recent_data:
            if streamer in exist_data:
                exist_data[streamer] = list(set(recent_data[streamer]) | set(exist_data[streamer]))
                count_merge += 1
            else:
                exist_data[streamer] = recent_data[streamer]
                count_add += 1
        logging.info('Completed: Merged data: %d | Added data: %d, Total streamers: %d', count_merge, count_add, len(exist_data))

        return exist_data

    except FileNotFoundError:
        logging.info('File not found')

        return recent_data


def export_data(file_name, data, export_type):
    """
    Export data in the format as specified in export_type.

    The export_type can be json, csv, or both.
    """

    def export_json(file_name, data):
        with open(f'../data/{file_name}.json', 'w') as out_file:
            json.dump(data, out_file)

    def export_csv(file_name, data):
        for key in data:
            data[key] = pd.Series(data[key])

        df = pd.DataFrame(data)

        df.to_csv(f'../data/{file_name}.csv', index=False)
    
    logging.info('Exporting data')
    if export_type == 'json': export_json(file_name, data)
    elif export_type == 'csv': export_csv(file_name, data)
    elif export_type == 'both':
        export_json(file_name, data)
        export_csv(file_name, data)
    logging.info('Exported')