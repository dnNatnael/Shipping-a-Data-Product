"""
Pydantic schemas for FastAPI request/response validation
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field

# Base response schema
class BaseResponse(BaseModel):
    """Base response schema"""
    success: bool = True
    message: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.now)

# Product schemas
class ProductMention(BaseModel):
    """Product mention schema"""
    term: str = Field(..., description="Product or term mentioned")
    mention_count: int = Field(..., ge=0, description="Number of mentions")
    total_views: int = Field(..., ge=0, description="Total views for messages containing this term")
    avg_views: float = Field(..., ge=0, description="Average views per message")
    channels: List[str] = Field(default_factory=list, description="Channels where this term was mentioned")

class TopProductsResponse(BaseResponse):
    """Response for top products endpoint"""
    data: List[ProductMention] = Field(..., description="List of top mentioned products")
    total_analyzed: int = Field(..., ge=0, description="Total messages analyzed")
    query_params: Dict[str, Any] = Field(default_factory=dict, description="Query parameters used")

# Channel activity schemas
class DailyActivity(BaseModel):
    """Daily activity data"""
    date: str = Field(..., description="Date in YYYY-MM-DD format")
    message_count: int = Field(..., ge=0, description="Number of messages posted")
    total_views: int = Field(..., ge=0, description="Total views for messages on this date")
    avg_views: float = Field(..., ge=0, description="Average views per message")
    messages_with_images: int = Field(..., ge=0, description="Number of messages with images")

class ChannelStats(BaseModel):
    """Channel statistics"""
    channel_name: str = Field(..., description="Name of the channel")
    channel_type: Optional[str] = Field(None, description="Type of channel (pharmaceutical, cosmetics, etc.)")
    total_messages: int = Field(..., ge=0, description="Total messages in channel")
    avg_daily_posts: float = Field(..., ge=0, description="Average posts per day")
    total_views: int = Field(..., ge=0, description="Total views across all messages")
    avg_views_per_post: float = Field(..., ge=0, description="Average views per post")
    image_percentage: float = Field(..., ge=0, le=100, description="Percentage of posts with images")
    first_post_date: Optional[str] = Field(None, description="Date of first post")
    last_post_date: Optional[str] = Field(None, description="Date of last post")

class ChannelActivityResponse(BaseResponse):
    """Response for channel activity endpoint"""
    channel_info: ChannelStats = Field(..., description="Channel statistics")
    daily_activity: List[DailyActivity] = Field(..., description="Daily activity data")
    top_terms: List[ProductMention] = Field(default_factory=list, description="Top mentioned terms in this channel")

# Message search schemas
class MessageResult(BaseModel):
    """Single message result"""
    message_id: int = Field(..., description="Unique message identifier")
    channel_name: str = Field(..., description="Channel name")
    message_date: str = Field(..., description="Message date and time")
    message_text: str = Field(..., description="Message text content")
    view_count: int = Field(..., ge=0, description="Number of views")
    forward_count: int = Field(..., ge=0, description="Number of forwards")
    has_image: bool = Field(..., description="Whether message contains an image")
    message_length: int = Field(..., ge=0, description="Length of message in characters")

class MessageSearchResponse(BaseResponse):
    """Response for message search endpoint"""
    messages: List[MessageResult] = Field(..., description="List of matching messages")
    total_found: int = Field(..., ge=0, description="Total number of messages found")
    query_params: Dict[str, Any] = Field(default_factory=dict, description="Search parameters used")

# Visual content schemas
class ChannelVisualStats(BaseModel):
    """Visual content statistics for a channel"""
    channel_name: str = Field(..., description="Channel name")
    total_messages: int = Field(..., ge=0, description="Total messages")
    messages_with_images: int = Field(..., ge=0, description="Messages with images")
    image_percentage: float = Field(..., ge=0, le=100, description="Percentage of messages with images")
    promotional_posts: int = Field(..., ge=0, description="Number of promotional posts")
    product_display_posts: int = Field(..., ge=0, description="Number of product display posts")
    lifestyle_posts: int = Field(..., ge=0, description="Number of lifestyle posts")
    avg_confidence: float = Field(..., ge=0, le=1, description="Average detection confidence")

class VisualContentSummary(BaseModel):
    """Summary of visual content across all channels"""
    total_images_analyzed: int = Field(..., ge=0, description="Total images analyzed")
    avg_confidence_score: float = Field(..., ge=0, le=1, description="Average confidence across all detections")
    top_detected_objects: List[Dict[str, Any]] = Field(default_factory=list, description="Most frequently detected objects")

class VisualContentResponse(BaseResponse):
    """Response for visual content statistics endpoint"""
    channel_stats: List[ChannelVisualStats] = Field(..., description="Visual stats by channel")
    summary: VisualContentSummary = Field(..., description="Overall summary")
    category_distribution: Dict[str, int] = Field(default_factory=dict, description="Distribution of image categories")

# Error response schema
class ErrorResponse(BaseResponse):
    """Error response schema"""
    success: bool = False
    error_code: str = Field(..., description="Error code")
    error_detail: Optional[str] = Field(None, description="Detailed error message")

# Query parameter schemas
class TopProductsParams(BaseModel):
    """Parameters for top products endpoint"""
    limit: int = Field(default=10, ge=1, le=100, description="Maximum number of results to return")
    min_mentions: int = Field(default=1, ge=1, description="Minimum number of mentions required")
    date_from: Optional[str] = Field(None, description="Start date in YYYY-MM-DD format")
    date_to: Optional[str] = Field(None, description="End date in YYYY-MM-DD format")

class ChannelActivityParams(BaseModel):
    """Parameters for channel activity endpoint"""
    days: int = Field(default=30, ge=1, le=365, description="Number of days of activity to analyze")
    include_top_terms: bool = Field(default=True, description="Whether to include top mentioned terms")

class MessageSearchParams(BaseModel):
    """Parameters for message search endpoint"""
    query: str = Field(..., min_length=1, description="Search query string")
    limit: int = Field(default=20, ge=1, le=100, description="Maximum number of results to return")
    channel: Optional[str] = Field(None, description="Filter by specific channel")
    date_from: Optional[str] = Field(None, description="Start date in YYYY-MM-DD format")
    date_to: Optional[str] = Field(None, description="End date in YYYY-MM-DD format")

class VisualContentParams(BaseModel):
    """Parameters for visual content endpoint"""
    include_details: bool = Field(default=True, description="Whether to include detailed detection stats")
    min_confidence: float = Field(default=0.1, ge=0, le=1, description="Minimum confidence threshold for analysis")