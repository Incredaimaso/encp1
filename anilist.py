import aiohttp
import asyncio
import logging
import os
import re
import json
from typing import Optional, Dict, Any, Union
from enum import Enum
from dataclasses import dataclass
import time
from urllib.parse import quote_plus

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("anilist_client.log")
    ]
)

logger = logging.getLogger("anilist_client")


class AniListException(Exception):
    """Base exception for AniList API operations."""
    pass


class AniListNetworkError(AniListException):
    """Raised when network communication fails."""
    pass


class AniListRateLimitError(AniListException):
    """Raised when API rate limits are exceeded."""
    pass


class AniListAPIError(AniListException):
    """Raised when the API returns an error response."""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"API Error ({status_code}): {message}")


class AniListResourceNotFound(AniListException):
    """Raised when the requested resource is not found."""
    pass


class MediaType(Enum):
    """Types of media in AniList."""
    ANIME = "ANIME"
    MANGA = "MANGA"


@dataclass
class Config:
    """Configuration for AniList client."""
    api_url: str = "https://graphql.anilist.co"
    max_retries: int = 3
    retry_delay: float = 1.0
    timeout: float = 10.0
    user_agent: str = "AniListClient/1.0"
    cache_dir: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")


class AniListAPI:
    """Client for interacting with the AniList GraphQL API with robust error handling."""

    def __init__(self, config: Optional[Config] = None):
        """
        Initialize the AniList API client.
        
        Args:
            config: Optional configuration object. If not provided, default values are used.
        """
        self.config = config or Config()
        self._ensure_cache_dir_exists()
        self._session = None
        self._queries = {
            "search_media": '''
            query ($search: String, $type: MediaType) {
              Media (search: $search, type: $type) {
                id
                title {
                  romaji
                  english
                  native
                }
                coverImage {
                  large
                  medium
                }
                description
                episodes
                status
                averageScore
                genres
                startDate {
                  year
                  month
                  day
                }
              }
            }
            '''
        }
        logger.info(f"AniListAPI initialized with endpoint: {self.config.api_url}")

    def _ensure_cache_dir_exists(self) -> None:
        """Ensure cache directory exists."""
        try:
            os.makedirs(self.config.cache_dir, exist_ok=True)
            logger.debug(f"Cache directory confirmed at: {self.config.cache_dir}")
        except OSError as e:
            logger.error(f"Failed to create cache directory: {e}")
            raise AniListException(f"Failed to create cache directory: {e}")

    async def _get_session(self) -> aiohttp.ClientSession:
        """
        Get or create an aiohttp client session.
        
        Returns:
            An aiohttp ClientSession object.
        """
        if self._session is None or self._session.closed:
            headers = {
                "User-Agent": self.config.user_agent,
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            self._session = aiohttp.ClientSession(headers=headers)
            logger.debug("Created new aiohttp session")
        return self._session

    async def _close_session(self) -> None:
        """Close the aiohttp session if it exists."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            logger.debug("Closed aiohttp session")

    def _clean_title(self, title: str) -> str:
        """
        Clean anime title for better search results.
        
        Args:
            title: The original title string
            
        Returns:
            Cleaned title string
        """
        # Remove content within brackets, replace separators with spaces
        cleaned = re.sub(r'\[.*?\]|\(.*?\)', '', title)
        cleaned = re.sub(r'[-_.]', ' ', cleaned)
        # Remove extra whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        logger.debug(f"Cleaned title: '{title}' -> '{cleaned}'")
        return cleaned

    def _get_cache_path(self, key: str) -> str:
        """
        Get file path for cached content.
        
        Args:
            key: Cache key
            
        Returns:
            Path to cache file
        """
        safe_key = quote_plus(key)
        return os.path.join(self.config.cache_dir, f"{safe_key}.json")

    def _cache_exists(self, key: str) -> bool:
        """
        Check if cache exists for a key.
        
        Args:
            key: Cache key
            
        Returns:
            True if cache exists and is valid
        """
        cache_path = self._get_cache_path(key)
        if not os.path.exists(cache_path):
            return False
            
        # Check if cache is not older than 24 hours
        cache_age = time.time() - os.path.getmtime(cache_path)
        return cache_age < 86400  # 24 hours in seconds

    def _get_from_cache(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve data from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached data or None if not found
        """
        if not self._cache_exists(key):
            return None
            
        cache_path = self._get_cache_path(key)
        try:
            with open(cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logger.debug(f"Retrieved from cache: {key}")
                return data
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Cache retrieval failed for {key}: {e}")
            return None

    def _save_to_cache(self, key: str, data: Dict[str, Any]) -> None:
        """
        Save data to cache.
        
        Args:
            key: Cache key
            data: Data to cache
        """
        cache_path = self._get_cache_path(key)
        try:
            with open(cache_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                logger.debug(f"Saved to cache: {key}")
        except IOError as e:
            logger.warning(f"Failed to save cache for {key}: {e}")

    async def _execute_query(
        self, 
        query: str, 
        variables: Dict[str, Any], 
        use_cache: bool = True
    ) -> Dict[str, Any]:
        """
        Execute a GraphQL query with retries and error handling.
        
        Args:
            query: GraphQL query string
            variables: Query variables
            use_cache: Whether to use cache
            
        Returns:
            Query result
            
        Raises:
            AniListNetworkError: When network communication fails
            AniListRateLimitError: When rate limited
            AniListAPIError: When API returns an error
        """
        cache_key = f"query_{hash(query)}_{hash(json.dumps(variables, sort_keys=True))}"
        
        # Try cache first if enabled
        if use_cache:
            cached = self._get_from_cache(cache_key)
            if cached:
                return cached
        
        session = await self._get_session()
        payload = {'query': query, 'variables': variables}
        
        for attempt in range(1, self.config.max_retries + 1):
            try:
                async with session.post(
                    self.config.api_url,
                    json=payload,
                    timeout=self.config.timeout
                ) as response:
                    response_data = await response.json()
                    
                    # Handle HTTP errors
                    if response.status != 200:
                        error_msg = response_data.get('message', 'Unknown error')
                        
                        if response.status == 429:
                            logger.warning(f"Rate limit exceeded. Attempt {attempt}/{self.config.max_retries}")
                            if attempt < self.config.max_retries:
                                await asyncio.sleep(self.config.retry_delay * attempt)
                                continue
                            raise AniListRateLimitError(f"Rate limit exceeded: {error_msg}")
                        
                        raise AniListAPIError(response.status, error_msg)
                    
                    # Check for GraphQL errors
                    if 'errors' in response_data:
                        error_msg = response_data['errors'][0].get('message', 'Unknown GraphQL error')
                        raise AniListAPIError(response.status, error_msg)
                    
                    # Success case
                    if use_cache:
                        self._save_to_cache(cache_key, response_data)
                    
                    return response_data
                    
            except asyncio.TimeoutError:
                logger.warning(f"Request timed out. Attempt {attempt}/{self.config.max_retries}")
                if attempt < self.config.max_retries:
                    await asyncio.sleep(self.config.retry_delay * attempt)
                    continue
                raise AniListNetworkError("Request timed out after multiple attempts")
                
            except aiohttp.ClientError as e:
                logger.warning(f"Network error: {e}. Attempt {attempt}/{self.config.max_retries}")
                if attempt < self.config.max_retries:
                    await asyncio.sleep(self.config.retry_delay * attempt)
                    continue
                raise AniListNetworkError(f"Network error: {str(e)}")
                
        # This should not be reached due to the exceptions above, but just in case
        raise AniListNetworkError("Failed to execute query after all retry attempts")

    async def search_anime(self, title: str, use_cache: bool = True) -> Optional[Dict[str, Any]]:
        """
        Search for anime by title.
        
        Args:
            title: Anime title to search for
            use_cache: Whether to use cached results
            
        Returns:
            Anime data or None if not found
            
        Raises:
            AniListException and subclasses for various error scenarios
        """
        try:
            clean_title = self._clean_title(title)
            logger.info(f"Searching for anime: '{clean_title}'")
            
            variables = {
                'search': clean_title,
                'type': MediaType.ANIME.value
            }
            
            result = await self._execute_query(
                self._queries['search_media'], 
                variables,
                use_cache
            )
            
            media = result.get('data', {}).get('Media')
            if not media:
                logger.info(f"No results found for anime: '{clean_title}'")
                return None
                
            logger.info(f"Found anime: {media.get('title', {}).get('english') or media.get('title', {}).get('romaji')}")
            return media
            
        except (AniListAPIError, AniListNetworkError, AniListRateLimitError) as e:
            logger.error(f"Error searching for anime '{title}': {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in search_anime: {e}", exc_info=True)
            raise AniListException(f"Unexpected error: {str(e)}")

    async def get_thumbnail(
        self, 
        title: str, 
        save_path: Optional[str] = None, 
        use_cache: bool = True
    ) -> Optional[str]:
        """
        Get anime thumbnail and save it locally.
        
        Args:
            title: Anime title
            save_path: Directory to save thumbnail (defaults to cache dir)
            use_cache: Whether to use cached results
            
        Returns:
            Path to saved thumbnail or None if not found/error
            
        Raises:
            AniListException and subclasses for various error scenarios
        """
        try:
            if save_path is None:
                save_path = self.config.cache_dir
                
            # Ensure save directory exists
            os.makedirs(save_path, exist_ok=True)
            
            # Generate thumbnail filename and path
            safe_title = quote_plus(title)
            thumb_path = os.path.join(save_path, f"thumb_{safe_title}.jpg")
            
            # Check if thumbnail already exists
            if os.path.exists(thumb_path) and use_cache:
                logger.info(f"Using cached thumbnail for '{title}'")
                return thumb_path
                
            # Search for anime data
            anime_data = await self.search_anime(title, use_cache)
            if not anime_data:
                logger.warning(f"No anime data found for '{title}', cannot get thumbnail")
                return None
                
            # Get thumbnail URL
            thumbnail_url = anime_data.get('coverImage', {}).get('large')
            if not thumbnail_url:
                logger.warning(f"No thumbnail URL for anime '{title}'")
                return None
                
            logger.info(f"Downloading thumbnail for '{title}' from {thumbnail_url}")
            
            # Download thumbnail
            session = await self._get_session()
            
            for attempt in range(1, self.config.max_retries + 1):
                try:
                    async with session.get(thumbnail_url, timeout=self.config.timeout) as response:
                        if response.status != 200:
                            if attempt < self.config.max_retries:
                                await asyncio.sleep(self.config.retry_delay * attempt)
                                continue
                            logger.error(f"Failed to download thumbnail: HTTP {response.status}")
                            return None
                            
                        # Write to file
                        with open(thumb_path, 'wb') as f:
                            f.write(await response.read())
                            
                        logger.info(f"Saved thumbnail to {thumb_path}")
                        return thumb_path
                        
                except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                    logger.warning(f"Network error downloading thumbnail: {e}. Attempt {attempt}/{self.config.max_retries}")
                    if attempt < self.config.max_retries:
                        await asyncio.sleep(self.config.retry_delay * attempt)
                        continue
                    logger.error(f"Failed to download thumbnail after {self.config.max_retries} attempts: {e}")
                    return None
                    
            return None
            
        except AniListException as e:
            # Let AniList exceptions propagate
            raise
        except Exception as e:
            logger.error(f"Unexpected error in get_thumbnail: {e}", exc_info=True)
            raise AniListException(f"Unexpected error getting thumbnail: {str(e)}")

    async def close(self) -> None:
        """Close resources used by the API client."""
        await self._close_session()
        logger.info("AniListAPI client closed")


# Context manager support
async def get_anilist_client(config: Optional[Config] = None) -> AniListAPI:
    """
    Factory function to create and properly manage AniListAPI client resources.
    
    Args:
        config: Optional configuration
        
    Returns:
        Configured AniListAPI client
    """
    return AniListAPI(config)


async def __aenter__(self) -> "AniListAPI":
    """Context manager entry."""
    return self


async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
    """Context manager exit with proper cleanup."""
    await self.close()


# Add context manager methods to AniListAPI
AniListAPI.__aenter__ = __aenter__
AniListAPI.__aexit__ = __aexit__