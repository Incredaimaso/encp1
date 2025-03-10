import aiohttp
from typing import Optional
import os
import re

class AniListAPI:
    def __init__(self):
        self.api_url = "https://graphql.anilist.co"
        self.query = '''
        query ($search: String) {
          Media (search: $search, type: ANIME) {
            id
            title {
              romaji
              english
            }
            coverImage {
              large
            }
          }
        }
        '''
        
    async def search_anime(self, title: str) -> Optional[dict]:
        try:
            # Clean title for better search
            search_title = re.sub(r'\[.*?\]', '', title)
            search_title = re.sub(r'[-_.]', ' ', search_title).strip()
            
            variables = {'search': search_title}
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.api_url,
                    json={'query': self.query, 'variables': variables}
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get('data', {}).get('Media', None)
            return None
        except Exception as e:
            print(f"AniList API error: {e}")
            return None

    async def get_thumbnail(self, title: str, save_path: str) -> Optional[str]:
        try:
            anime_data = await self.search_anime(title)
            if not anime_data or not anime_data.get('coverImage', {}).get('large'):
                return None
                
            thumbnail_url = anime_data['coverImage']['large']
            thumb_path = os.path.join(save_path, f"thumb_{hash(title)}.jpg")
            
            async with aiohttp.ClientSession() as session:
                async with session.get(thumbnail_url) as response:
                    if response.status == 200:
                        with open(thumb_path, 'wb') as f:
                            f.write(await response.read())
                        return thumb_path
            return None
        except Exception as e:
            print(f"Thumbnail download error: {e}")
            return None
