import aiohttp
import asyncio
import json
import logging

import collecting


async def main():

    logging.basicConfig(filename='log_file.log',
                        format='%(asctime)s:%(levelname)s:%(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.INFO)

    # Need to specify your Twitch developer client id, client secret, and access token
    # in order to make an API call in get_streams function.
    with open('../data/credentials.json') as credentials:
        cr = json.load(credentials)

    streamers = collecting.get_streams(cr, page_size=100, viewers_threshold=20)

    async with aiohttp.ClientSession() as session:
        recent_data = await collecting.get_viewers(session, streamers)

    data = collecting.update_data('twitch_data', recent_data)

    collecting.export_data('twitch_data', data, export_type='both')


if __name__ == '__main__':
    asyncio.run(main())