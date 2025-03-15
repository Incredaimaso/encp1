import re
import os
import logging
from typing import Dict, Optional, List, Tuple, Any
from dataclasses import dataclass
import json
from pathlib import Path
import traceback


@dataclass
class VideoMetadata:
    """Structured container for parsed video metadata."""
    title: str
    season: Optional[int] = None
    episode: Optional[int] = None
    quality: str = "Unknown"
    dual_audio: bool = False
    eng_sub: bool = False
    year: Optional[int] = None
    group: Optional[str] = None
    
    def __post_init__(self) -> None:
        """Convert string values to proper types after initialization."""
        if isinstance(self.season, str) and self.season:
            try:
                self.season = int(self.season)
            except ValueError:
                self.season = None
                
        if isinstance(self.episode, str) and self.episode:
            try:
                self.episode = int(self.episode)
            except ValueError:
                self.episode = None
                
        if isinstance(self.year, str) and self.year:
            try:
                self.year = int(self.year)
            except ValueError:
                self.year = None


class ParsingError(Exception):
    """Custom exception for handling parsing failures."""
    pass


class VideoRenamer:
    """
    A robust utility for parsing and renaming video files with consistent naming patterns.
    
    Features:
    - Configurable parsing patterns
    - Caching for improved performance
    - Comprehensive error handling
    - Detailed logging
    """
    
    DEFAULT_CONFIG = {
        "ignored_terms": [
            "x264", "x265", "HEVC", "WEB-DL", "BluRay", "CRX", 
            "AMZN", "NF", "DSNP", "HULU", "HDR", "10bit", "AAC",
            "REPACK", "PROPER"
        ],
        "quality_patterns": {
            "4K": r"2160p|4K|UHD",
            "1080p": r"1080p|1920x1080|FHD|1080",
            "720p": r"720p|1280x720|HD|720",
            "480p": r"480p|854x480|SD|480",
            "360p": r"360p|640x360|360"
        },
        "title_patterns": [
            r"^(.*?)(?:\[S\d+|S\d+|E\d+|\(\d{4}\)|\[\d{4}\])",  # Match until season/episode/year
            r"^(.*?)(?:\d{1,2}x\d{2})",  # Format like "Show Name 1x01"
            r"^(.*?)(?:\d{1,3}v\d)"      # Format like "Show Name 101v2"
        ],
        "season_episode_patterns": [
            r"\[S(\d{1,2})-E(\d{1,3})\]",  # [S01-E01]
            r"S(\d{1,2})E(\d{1,3})",       # S01E01
            r"(\d{1,2})x(\d{2})",          # 1x01
            r"(?:^|\D)(\d)(\d{2})(?:\D|$)" # 101 (season 1, episode 01)
        ],
        "episode_only_patterns": [
            r"\[E(\d{1,3})\]",              # [E01]
            r"Episode\.?(\d{1,3})",         # Episode.01 or Episode01
            r"Ep\.?(\d{1,3})",              # Ep.01 or Ep01
            r"(?:^|\D)E(\d{1,3})(?:\D|$)"   # E01
        ],
        "audio_patterns": [
            r"\[(Dual[ ._-]?Audio|Multi[ ._-]?Audio)\]",
            r"(Dual[ ._-]?Audio|Multi[ ._-]?Audio)"
        ],
        "subtitle_patterns": [
            r"\[(Eng[ ._-]?Sub|Multi[ ._-]?Sub|MultiSub)\]",
            r"(Eng[ ._-]?Sub|Multi[ ._-]?Sub|MultiSub)"
        ],
        "year_pattern": r"(?:\(|\[)?(19\d{2}|20\d{2})(?:\)|\])?",
        "group_pattern": r"\[([\w\.-]+)\]$",  # Release group usually at the end
        "output_format": "{season_ep} {title}{year} {audio} {subs} [{quality}]{group}.{ext}",
        "default_extension": "mkv"
    }
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the VideoRenamer with optional custom configuration.
        
        Args:
            config_path: Path to a JSON configuration file (optional)
        """
        # Set up logging
        self.logger = self._setup_logger()
        
        # Load configuration
        self.config = self._load_config(config_path)
        self.logger.debug(f"Configuration loaded with {len(self.config['ignored_terms'])} ignored terms")
        
        # Initialize cache
        self.name_cache = {}
        
        # Compile regex patterns for efficiency
        self._compile_patterns()
        
    def _setup_logger(self) -> logging.Logger:
        """Configure and return a logger instance."""
        logger = logging.getLogger("VideoRenamer")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """
        Load configuration from file or use defaults.
        
        Args:
            config_path: Path to configuration JSON file
            
        Returns:
            Dictionary containing configuration settings
        """
        config = self.DEFAULT_CONFIG.copy()
        
        if config_path:
            try:
                with open(config_path, 'r') as f:
                    user_config = json.load(f)
                    
                # Merge user config with defaults
                for key, value in user_config.items():
                    if key in config:
                        if isinstance(value, dict) and isinstance(config[key], dict):
                            config[key].update(value)
                        else:
                            config[key] = value
                            
                self.logger.info(f"Custom configuration loaded from {config_path}")
            except (json.JSONDecodeError, FileNotFoundError) as e:
                self.logger.error(f"Error loading configuration: {e}")
                self.logger.info("Using default configuration")
        
        return config
    
    def _compile_patterns(self) -> None:
        """Pre-compile regex patterns for better performance."""
        # Compile patterns
        self.re_quality = {quality: re.compile(pattern, re.IGNORECASE) 
                           for quality, pattern in self.config["quality_patterns"].items()}
        
        self.re_title_patterns = [re.compile(pattern, re.IGNORECASE) 
                                 for pattern in self.config["title_patterns"]]
        
        self.re_season_episode = [re.compile(pattern, re.IGNORECASE) 
                                  for pattern in self.config["season_episode_patterns"]]
        
        self.re_episode_only = [re.compile(pattern, re.IGNORECASE) 
                                for pattern in self.config["episode_only_patterns"]]
        
        self.re_audio = [re.compile(pattern, re.IGNORECASE) 
                         for pattern in self.config["audio_patterns"]]
        
        self.re_subtitle = [re.compile(pattern, re.IGNORECASE) 
                            for pattern in self.config["subtitle_patterns"]]
        
        self.re_year = re.compile(self.config["year_pattern"], re.IGNORECASE)
        self.re_group = re.compile(self.config["group_pattern"], re.IGNORECASE)
    
    def set_log_level(self, level: int) -> None:
        """
        Set the logging level.
        
        Args:
            level: A logging level constant (e.g., logging.DEBUG)
        """
        self.logger.setLevel(level)
        self.logger.debug(f"Log level set to {level}")
    
    def _detect_quality(self, filename: str) -> str:
        """
        Detect video quality from filename.
        
        Args:
            filename: Original filename
            
        Returns:
            String representation of quality (e.g., '1080p')
        """
        filename_upper = filename.upper()
        
        for quality, pattern in self.re_quality.items():
            if pattern.search(filename_upper):
                return quality
                
        return "480p"  # Default quality
    
    def _extract_title(self, filename: str) -> str:
        """
        Extract title from filename using multiple patterns.
        
        Args:
            filename: Cleaned filename
            
        Returns:
            Extracted title
        """
        # Try each title pattern
        for pattern in self.re_title_patterns:
            match = pattern.search(filename)
            if match and match.group(1):
                return match.group(1).strip()
        
        # Fallback: take everything before the first bracket or parenthesis
        title = re.split(r'\[|\(', filename)[0].strip()
        return title
    
    def _extract_season_episode(self, filename: str) -> Tuple[Optional[int], Optional[int]]:
        """
        Extract season and episode numbers.
        
        Args:
            filename: Original filename
            
        Returns:
            Tuple of (season, episode)
        """
        # Try season+episode patterns
        for pattern in self.re_season_episode:
            match = pattern.search(filename)
            if match:
                try:
                    season = int(match.group(1))
                    episode = int(match.group(2))
                    return season, episode
                except (ValueError, IndexError):
                    continue
        
        # Try episode-only patterns
        for pattern in self.re_episode_only:
            match = pattern.search(filename)
            if match:
                try:
                    episode = int(match.group(1))
                    return 1, episode  # Default to season 1
                except (ValueError, IndexError):
                    continue
        
        return None, None
    
    def _extract_year(self, filename: str) -> Optional[int]:
        """
        Extract year from filename.
        
        Args:
            filename: Original filename
            
        Returns:
            Year as integer or None
        """
        match = self.re_year.search(filename)
        if match:
            try:
                return int(match.group(1))
            except (ValueError, IndexError):
                return None
        return None
    
    def _extract_release_group(self, filename: str) -> Optional[str]:
        """
        Extract release group from filename.
        
        Args:
            filename: Original filename
            
        Returns:
            Release group string or None
        """
        match = self.re_group.search(filename)
        if match:
            return match.group(1)
        return None
    
    def _clean_filename(self, filename: str) -> str:
        """
        Remove technical terms and file extensions.
        
        Args:
            filename: Original filename
            
        Returns:
            Cleaned filename
        """
        # Remove extensions
        clean = os.path.splitext(filename)[0]
        
        # Remove ignored terms
        for term in self.config["ignored_terms"]:
            clean = re.sub(rf'\[?{re.escape(term)}\]?', '', clean, flags=re.IGNORECASE)
        
        # Clean up extra spaces and brackets
        clean = re.sub(r'\s+', ' ', clean)
        clean = re.sub(r'\[\s*\]', '', clean)
        
        return clean.strip()
    
    def parse_name(self, filename: str) -> VideoMetadata:
        """
        Parse a filename into structured metadata.
        
        Args:
            filename: Original filename
            
        Returns:
            VideoMetadata object with parsed information
            
        Raises:
            ParsingError: If parsing fails and no fallback succeeds
        """
        # Check cache first
        if filename in self.name_cache:
            self.logger.debug(f"Cache hit for {filename}")
            return self.name_cache[filename]
        
        self.logger.debug(f"Parsing: {filename}")
        try:
            # Clean the filename
            clean_name = self._clean_filename(filename)
            
            # Extract all components
            season, episode = self._extract_season_episode(clean_name)
            title = self._extract_title(clean_name)
            quality = self._detect_quality(filename)
            year = self._extract_year(clean_name)
            group = self._extract_release_group(filename)
            
            # Check for audio and subtitle tags
            has_dual_audio = any(pattern.search(filename) for pattern in self.re_audio)
            has_eng_sub = any(pattern.search(filename) for pattern in self.re_subtitle)
            
            # Create metadata object
            metadata = VideoMetadata(
                title=title,
                season=season,
                episode=episode,
                quality=quality,
                dual_audio=has_dual_audio,
                eng_sub=has_eng_sub,
                year=year,
                group=group
            )
            
            # Validate minimum required fields
            if not title or not episode:
                raise ParsingError("Failed to extract required fields")
            
            # Cache the result
            self.name_cache[filename] = metadata
            return metadata
            
        except Exception as e:
            self.logger.warning(f"Primary parsing failed for {filename}: {e}")
            self.logger.debug(traceback.format_exc())
            
            # Try fallback method
            try:
                return self._fallback_parse(filename)
            except Exception as fallback_error:
                self.logger.error(f"Fallback parsing also failed: {fallback_error}")
                raise ParsingError(f"Could not parse filename: {filename}") from fallback_error
    
    def _fallback_parse(self, filename: str) -> VideoMetadata:
        """
        Basic fallback parsing method when advanced parsing fails.
        
        Args:
            filename: Original filename
            
        Returns:
            VideoMetadata with basic information
            
        Raises:
            ParsingError: If even basic parsing fails
        """
        self.logger.debug(f"Using fallback parsing for {filename}")
        
        # Extract the filename without extension
        base_name = os.path.splitext(filename)[0]
        
        # Basic episode extraction
        ep_match = re.search(r'E?(\d{1,2})', base_name, re.IGNORECASE)
        episode = int(ep_match.group(1)) if ep_match else None
        
        # Basic title extraction - everything before a number
        title_match = re.match(r'^(.*?)(?:\d|$)', base_name)
        title = title_match.group(1).strip() if title_match else base_name
        
        if not title or not episode:
            raise ParsingError("Fallback parsing failed to extract basic information")
        
        # Create basic metadata
        metadata = VideoMetadata(
            title=title,
            season=1,  # Default to season 1
            episode=episode,
            quality=self._detect_quality(filename)
        )
        
        # Cache the result
        self.name_cache[filename] = metadata
        return metadata
    
    def generate_filename(self, original_name: str) -> str:
        """
        Generate a standardized filename based on extracted metadata.
        
        Args:
            original_name: Original filename
            
        Returns:
            Standardized filename
            
        Raises:
            ParsingError: If filename cannot be generated
        """
        try:
            # Extract original extension or use default
            _, extension = os.path.splitext(original_name)
            extension = extension[1:] if extension else self.config["default_extension"]
            
            # Parse the filename
            metadata = self.parse_name(original_name)
            
            # Build components
            if metadata.season is not None and metadata.episode is not None:
                season_ep = f"[S{metadata.season:02d}-E{metadata.episode:02d}]"
            elif metadata.episode is not None:
                season_ep = f"[E{metadata.episode:02d}]"
            else:
                season_ep = ""
            
            title = metadata.title
            
            year = f" ({metadata.year})" if metadata.year else ""
            
            audio = " [Dual Audio]" if metadata.dual_audio else ""
            subs = " [Eng Sub]" if metadata.eng_sub else ""
            
            quality = metadata.quality
            
            group = f" [{metadata.group}]" if metadata.group else ""
            
            # Format the filename according to template
            format_dict = {
                "season_ep": season_ep,
                "title": title,
                "year": year,
                "audio": audio,
                "subs": subs,
                "quality": quality,
                "group": group,
                "ext": extension
            }
            
            # Generate using the output format template
            new_filename = self.config["output_format"].format(**format_dict)
            
            # Clean up any double spaces or empty brackets
            new_filename = re.sub(r'\s+', ' ', new_filename)
            new_filename = re.sub(r'\[\s*\]', '', new_filename)
            
            return new_filename
            
        except Exception as e:
            self.logger.error(f"Error generating filename for {original_name}: {e}")
            raise ParsingError(f"Failed to generate filename: {str(e)}") from e
    
    def rename_file(self, file_path: str, dry_run: bool = False) -> Tuple[str, str]:
        """
        Rename a video file using the standardized format.
        
        Args:
            file_path: Path to the file
            dry_run: If True, doesn't actually rename the file
            
        Returns:
            Tuple of (original_path, new_path)
            
        Raises:
            FileNotFoundError: If the file doesn't exist
            PermissionError: If the file can't be accessed
            ParsingError: If the filename can't be parsed
        """
        file_path = os.path.abspath(file_path)
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        try:
            # Get directory and filename
            directory = os.path.dirname(file_path)
            filename = os.path.basename(file_path)
            
            # Generate new filename
            new_filename = self.generate_filename(filename)
            new_path = os.path.join(directory, new_filename)
            
            self.logger.info(f"Renaming: {filename} -> {new_filename}")
            
            # Rename the file if not a dry run
            if not dry_run:
                if os.path.exists(new_path) and new_path != file_path:
                    raise FileExistsError(f"Target file already exists: {new_path}")
                os.rename(file_path, new_path)
            
            return file_path, new_path
            
        except (OSError, PermissionError) as e:
            self.logger.error(f"OS error renaming {file_path}: {e}")
            raise
        except ParsingError as e:
            self.logger.error(f"Parsing error for {file_path}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error renaming {file_path}: {e}")
            self.logger.debug(traceback.format_exc())
            raise ParsingError(f"Failed to rename file: {str(e)}") from e
    
    def batch_rename(self, directory: str, extensions: List[str] = None, 
                     recursive: bool = False, dry_run: bool = False) -> Dict[str, str]:
        """
        Batch rename video files in a directory.
        
        Args:
            directory: Directory path
            extensions: List of file extensions to process (e.g., ['mp4', 'mkv'])
            recursive: Whether to process subdirectories
            dry_run: If True, doesn't actually rename files
            
        Returns:
            Dictionary mapping original paths to new paths
            
        Raises:
            FileNotFoundError: If the directory doesn't exist
        """
        if not os.path.isdir(directory):
            raise FileNotFoundError(f"Directory not found: {directory}")
        
        if extensions is None:
            extensions = ['mp4', 'mkv', 'avi', 'wmv']
        
        results = {}
        errors = {}
        
        # Normalize extensions
        extensions = [ext.lower().lstrip('.') for ext in extensions]
        
        self.logger.info(f"Starting batch rename in {directory} (recursive={recursive}, dry_run={dry_run})")
        
        # Collect all files
        files_to_process = []
        if recursive:
            for root, _, files in os.walk(directory):
                for file in files:
                    if any(file.lower().endswith(f'.{ext}') for ext in extensions):
                        files_to_process.append(os.path.join(root, file))
        else:
            for file in os.listdir(directory):
                if any(file.lower().endswith(f'.{ext}') for ext in extensions):
                    files_to_process.append(os.path.join(directory, file))
        
        self.logger.info(f"Found {len(files_to_process)} files to process")
        
        # Process each file
        for file_path in files_to_process:
            try:
                original, new = self.rename_file(file_path, dry_run)
                results[original] = new
            except Exception as e:
                self.logger.error(f"Error processing {file_path}: {e}")
                errors[file_path] = str(e)
        
        # Report summary
        success_count = len(results)
        error_count = len(errors)
        self.logger.info(f"Batch rename complete: {success_count} successes, {error_count} errors")
        
        if error_count > 0:
            self.logger.warning("Files with errors:")
            for path, error in errors.items():
                self.logger.warning(f"  {os.path.basename(path)}: {error}")
        
        return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Rename video files with consistent naming scheme.")
    parser.add_argument("path", help="File or directory to process")
    parser.add_argument("--config", help="Path to config file")
    parser.add_argument("--recursive", "-r", action="store_true", help="Process directories recursively")
    parser.add_argument("--dry-run", "-d", action="store_true", help="Don't actually rename files")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    try:
        renamer = VideoRenamer(config_path=args.config)
        
        if args.verbose:
            renamer.set_log_level(logging.DEBUG)
        
        if os.path.isfile(args.path):
            original, new = renamer.rename_file(args.path, dry_run=args.dry_run)
            print(f"{'Would rename' if args.dry_run else 'Renamed'}:")
            print(f"  From: {original}")
            print(f"  To:   {new}")
        elif os.path.isdir(args.path):
            results = renamer.batch_rename(
                args.path, 
                recursive=args.recursive,
                dry_run=args.dry_run
            )
            print(f"{'Would process' if args.dry_run else 'Processed'} {len(results)} files")
        else:
            print(f"Error: Path not found: {args.path}")
            exit(1)
            
    except Exception as e:
        print(f"Error: {e}")
        if args.verbose:
            traceback.print_exc()
        exit(1)