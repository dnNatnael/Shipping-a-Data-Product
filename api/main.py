"""
FastAPI application for Ethiopian Medical Business Data Analytics
"""

from fastapi import FastAPI, HTTPException, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional, Dict, Any
import logging
from datetime import datetime

from database import execute_query_to_dataframe, test_connection
from schemas import (
    TopProductsResponse, ProductMention,
    ChannelActivityResponse, ChannelStats, DailyActivity,
    MessageSearchResponse, MessageResult,
    VisualContentResponse, ChannelVisualStats, VisualContentSummary,
    ErrorResponse
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Ethiopian Medical Business Analytics API",
    description="REST API for analyzing Ethiopian medical business data from Telegram channels",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Health check endpoint
@app.get("/health", tags=["Health"])
async def health_check():
    """Check if the API and database are working"""
    try:
        db_status = test_connection()
        return {
            "status": "healthy" if db_status else "unhealthy",
            "database": "connected" if db_status else "disconnected",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unavailable")

@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Ethiopian Medical Business Analytics API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }

# Endpoint 1: Top Products
@app.get("/api/reports/top-products", response_model=TopProductsResponse, tags=["Reports"])
async def get_top_products(
    limit: int = Query(default=10, ge=1, le=100, description="Maximum number of results to return"),
    min_mentions: int = Query(default=1, ge=1, description="Minimum number of mentions required"),
    date_from: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
    date_to: Optional[str] = Query(None, description="End date in YYYY-MM-DD format")
):
    """
    Returns the most frequently mentioned terms/products across all channels.
    
    This endpoint analyzes message text to identify trending medical products,
    drugs, and health-related terms mentioned in Telegram channels.
    """
    try:
        # Build WHERE clause for date filtering
        date_conditions = []
        params = {"min_mentions": min_mentions, "limit": limit}
        
        if date_from:
            date_conditions.append("fm.message_date >= %(date_from)s")
            params["date_from"] = date_from
        
        if date_to:
            date_conditions.append("fm.message_date <= %(date_to)s")
            params["date_to"] = date_to
        
        date_clause = "AND " + " AND ".join(date_conditions) if date_conditions else ""
        
        # SQL query to find top mentioned terms
        query = f"""
        WITH message_terms AS (
            SELECT 
                fm.message_id,
                fm.channel_name,
                fm.message_date,
                fm.view_count,
                fm.message_text,
                -- Extract potential medical/product terms (simplified approach)
                regexp_split_to_table(lower(fm.message_text), '[\s.,;:!?()]+') as term
            FROM analytics.fct_messages fm
            WHERE fm.message_text IS NOT NULL 
            AND length(fm.message_text) > 0
            {date_clause}
        ),
        filtered_terms AS (
            SELECT 
                term,
                message_id,
                channel_name,
                view_count,
                -- Filter for likely medical/product terms
                CASE 
                    WHEN term ~ any(ARRAY['^(amoxicillin|paracetamol|ibuprofen|aspirin|penicillin|vitamin|calcium|iron|zinc|medicine|drug|tablet|capsule|syrup|cream|ointment|injection|vaccine|antibiotic|pain|fever|cold|cough|headache|blood|pressure|diabetes|sugar|insulin|pills|medication|pharmacy|hospital|doctor|nurse|treatment|cure|heal|health|medical|clinical|prescription|dose|mg|ml|gram|box|bottle|pack|strip)$')
                    THEN 1
                    WHEN length(term) >= 3 AND term ~ any(ARRAY['[a-z]+[0-9]+', '[0-9]+[a-z]+'])  -- Alphanumeric terms
                    THEN 1
                    WHEN length(term) >= 4 AND term ~ any(ARRAY['^(anti|bio|medi|pharma|thera|surg|dent|opti|cardio|neuro|gastro|derma|pedi|ortho)$'])
                    THEN 1
                    ELSE 0
                END as is_medical_term
            FROM message_terms
            WHERE length(term) >= 3
            AND term NOT IN (SELECT unnest(ARRAY['the', 'and', 'for', 'are', 'with', 'you', 'that', 'this', 'from', 'they', 'have', 'been', 'has', 'had', 'what', 'will', 'your', 'can', 'said', 'each', 'which', 'their', 'time', 'will', 'about', 'if', 'up', 'out', 'many', 'then', 'them', 'these', 'so', 'some', 'her', 'would', 'make', 'like', 'into', 'him', 'two', 'more', 'very', 'after', 'back', 'call', 'through', 'just', 'also', 'even', 'most', 'such', 'too', 'much', 'well', 'were', 'me', 'first', 'may', 'when', 'where', 'how', 'old', 'did', 'come', 'his', 'there', 'www', 'com', 'http', 'https', 'tme', 'telegram']))
        ),
        term_stats AS (
            SELECT 
                term,
                COUNT(*) as mention_count,
                SUM(view_count) as total_views,
                AVG(view_count) as avg_views,
                array_agg(DISTINCT channel_name) as channels
            FROM filtered_terms
            WHERE is_medical_term = 1
            GROUP BY term
            HAVING COUNT(*) >= %(min_mentions)s
        )
        SELECT 
            term,
            mention_count,
            total_views,
            avg_views,
            channels
        FROM term_stats
        ORDER BY mention_count DESC, total_views DESC
        LIMIT %(limit)s
        """
        
        results = execute_query_to_dataframe(query, params)
        
        # Convert to response format
        products = []
        for result in results:
            products.append(ProductMention(
                term=result['term'],
                mention_count=result['mention_count'],
                total_views=result['total_views'],
                avg_views=round(result['avg_views'], 2),
                channels=result['channels']
            ))
        
        # Get total analyzed count
        total_query = f"""
        SELECT COUNT(*) as total_messages
        FROM analytics.fct_messages fm
        WHERE fm.message_text IS NOT NULL 
        AND length(fm.message_text) > 0
        {date_clause}
        """
        
        total_result = execute_query_to_dataframe(total_query, params)
        total_analyzed = total_result[0]['total_messages'] if total_result else 0
        
        return TopProductsResponse(
            data=products,
            total_analyzed=total_analyzed,
            query_params={
                "limit": limit,
                "min_mentions": min_mentions,
                "date_from": date_from,
                "date_to": date_to
            }
        )
        
    except Exception as e:
        logger.error(f"Error in get_top_products: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve top products data")

# Endpoint 2: Channel Activity
@app.get("/api/channels/{channel_name}/activity", response_model=ChannelActivityResponse, tags=["Channels"])
async def get_channel_activity(
    channel_name: str = Path(..., description="Name of the channel to analyze"),
    days: int = Query(default=30, ge=1, le=365, description="Number of days of activity to analyze"),
    include_top_terms: bool = Query(default=True, description="Whether to include top mentioned terms")
):
    """
    Returns posting activity and trends for a specific channel.
    
    This endpoint provides comprehensive analytics about a channel's posting patterns,
    engagement metrics, and trending topics over a specified time period.
    """
    try:
        params = {
            "channel_name": channel_name.lower(),
            "days": days
        }
        
        # Get channel statistics
        channel_query = """
        SELECT 
            dc.channel_name,
            dc.channel_type,
            dc.total_posts,
            dc.avg_views,
            dc.image_percentage,
            dc.first_post_date,
            dc.last_post_date,
            ROUND(dc.total_posts::decimal / NULLIF(dc.total_posts, 0) / 
                   GREATEST(EXTRACT(EPOCH FROM (dc.last_post_date - dc.first_post_date)) / 86400, 1), 2) as avg_daily_posts
        FROM analytics.dim_channels dc
        WHERE dc.channel_name = %(channel_name)s
        """
        
        channel_results = execute_query_to_dataframe(channel_query, params)
        
        if not channel_results:
            raise HTTPException(status_code=404, detail=f"Channel '{channel_name}' not found")
        
        channel_data = channel_results[0]
        
        # Get daily activity
        activity_query = """
        SELECT 
            dd.full_date as date,
            COUNT(*) as message_count,
            SUM(fm.view_count) as total_views,
            ROUND(AVG(fm.view_count), 2) as avg_views,
            COUNT(CASE WHEN fm.has_image = true THEN 1 END) as messages_with_images
        FROM analytics.fct_messages fm
        JOIN analytics.dim_dates dd ON fm.date_key = dd.date_key
        WHERE fm.channel_key = (SELECT channel_key FROM analytics.dim_channels WHERE channel_name = %(channel_name)s)
        AND dd.full_date >= CURRENT_DATE - INTERVAL '%(days)s days'
        GROUP BY dd.full_date
        ORDER BY dd.full_date DESC
        """
        
        activity_results = execute_query_to_dataframe(activity_query, params)
        
        daily_activity = []
        for result in activity_results:
            daily_activity.append(DailyActivity(
                date=result['date'].strftime('%Y-%m-%d'),
                message_count=result['message_count'],
                total_views=result['total_views'],
                avg_views=result['avg_views'],
                messages_with_images=result['messages_with_images']
            ))
        
        # Get top terms for this channel (if requested)
        top_terms = []
        if include_top_terms:
            terms_query = """
            WITH channel_terms AS (
                SELECT 
                    regexp_split_to_table(lower(fm.message_text), '[\s.,;:!?()]+') as term,
                    fm.view_count,
                    fm.channel_name
                FROM analytics.fct_messages fm
                WHERE fm.channel_key = (SELECT channel_key FROM analytics.dim_channels WHERE channel_name = %(channel_name)s)
                AND fm.message_text IS NOT NULL 
                AND length(fm.message_text) > 0
                AND fm.message_date >= CURRENT_DATE - INTERVAL '%(days)s days'
            ),
            filtered_terms AS (
                SELECT term, view_count, channel_name
                FROM channel_terms
                WHERE length(term) >= 3
                AND term NOT IN (SELECT unnest(ARRAY['the', 'and', 'for', 'are', 'with', 'you', 'that', 'this', 'from', 'they', 'have', 'been', 'has', 'had', 'what', 'will', 'your', 'can', 'said', 'each', 'which', 'their', 'time', 'will', 'about', 'if', 'up', 'out', 'many', 'then', 'them', 'these', 'so', 'some', 'her', 'would', 'make', 'like', 'into', 'him', 'two', 'more', 'very', 'after', 'back', 'call', 'through', 'just', 'also', 'even', 'most', 'such', 'too', 'much', 'well', 'were', 'me', 'first', 'may', 'when', 'where', 'how', 'old', 'did', 'come', 'his', 'there', 'www', 'com', 'http', 'https', 'tme', 'telegram']))
            ),
            term_stats AS (
                SELECT 
                    term,
                    COUNT(*) as mention_count,
                    SUM(view_count) as total_views,
                    AVG(view_count) as avg_views,
                    array_agg(DISTINCT channel_name) as channels
                FROM filtered_terms
                GROUP BY term
                HAVING COUNT(*) >= 2
            )
            SELECT term, mention_count, total_views, avg_views, channels
            FROM term_stats
            ORDER BY mention_count DESC, total_views DESC
            LIMIT 10
            """
            
            terms_results = execute_query_to_dataframe(terms_query, params)
            
            for result in terms_results:
                top_terms.append(ProductMention(
                    term=result['term'],
                    mention_count=result['mention_count'],
                    total_views=result['total_views'],
                    avg_views=round(result['avg_views'], 2),
                    channels=result['channels']
                ))
        
        return ChannelActivityResponse(
            channel_info=ChannelStats(
                channel_name=channel_data['channel_name'],
                channel_type=channel_data['channel_type'],
                total_messages=channel_data['total_posts'],
                avg_daily_posts=channel_data['avg_daily_posts'],
                total_views=channel_data['total_views'] or 0,
                avg_views_per_post=channel_data['avg_views'] or 0.0,
                image_percentage=channel_data['image_percentage'] or 0.0,
                first_post_date=str(channel_data['first_post_date']) if channel_data['first_post_date'] else None,
                last_post_date=str(channel_data['last_post_date']) if channel_data['last_post_date'] else None
            ),
            daily_activity=daily_activity,
            top_terms=top_terms
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_channel_activity: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve channel activity data")

# Endpoint 3: Message Search
@app.get("/api/search/messages", response_model=MessageSearchResponse, tags=["Search"])
async def search_messages(
    query: str = Query(..., min_length=1, description="Search query string"),
    limit: int = Query(default=20, ge=1, le=100, description="Maximum number of results to return"),
    channel: Optional[str] = Query(None, description="Filter by specific channel"),
    date_from: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
    date_to: Optional[str] = Query(None, description="End date in YYYY-MM-DD format")
):
    """
    Searches for messages containing a specific keyword.
    
    This endpoint performs full-text search across all message content
    with optional filtering by channel and date range.
    """
    try:
        # Build query parameters
        params = {
            "search_query": f"%{query.lower()}%",
            "limit": limit
        }
        
        # Build WHERE conditions
        conditions = ["LOWER(fm.message_text) LIKE %(search_query)s"]
        
        if channel:
            conditions.append("dc.channel_name = %(channel)s")
            params["channel"] = channel.lower()
        
        if date_from:
            conditions.append("fm.message_date >= %(date_from)s")
            params["date_from"] = date_from
        
        if date_to:
            conditions.append("fm.message_date <= %(date_to)s")
            params["date_to"] = date_to
        
        where_clause = " AND ".join(conditions)
        
        # Search query
        search_query = f"""
        SELECT 
            fm.message_id,
            dc.channel_name,
            fm.message_date,
            fm.message_text,
            fm.view_count,
            fm.forward_count,
            fm.has_image,
            fm.message_length
        FROM analytics.fct_messages fm
        JOIN analytics.dim_channels dc ON fm.channel_key = dc.channel_key
        WHERE {where_clause}
        ORDER BY fm.message_date DESC, fm.view_count DESC
        LIMIT %(limit)s
        """
        
        results = execute_query_to_dataframe(search_query, params)
        
        # Convert to response format
        messages = []
        for result in results:
            messages.append(MessageResult(
                message_id=result['message_id'],
                channel_name=result['channel_name'],
                message_date=result['message_date'].isoformat() if result['message_date'] else "",
                message_text=result['message_text'] or "",
                view_count=result['view_count'],
                forward_count=result['forward_count'],
                has_image=result['has_image'],
                message_length=result['message_length']
            ))
        
        # Get total count
        count_query = f"""
        SELECT COUNT(*) as total_count
        FROM analytics.fct_messages fm
        JOIN analytics.dim_channels dc ON fm.channel_key = dc.channel_key
        WHERE {where_clause}
        """
        
        count_result = execute_query_to_dataframe(count_query, params)
        total_found = count_result[0]['total_count'] if count_result else 0
        
        return MessageSearchResponse(
            messages=messages,
            total_found=total_found,
            query_params={
                "query": query,
                "limit": limit,
                "channel": channel,
                "date_from": date_from,
                "date_to": date_to
            }
        )
        
    except Exception as e:
        logger.error(f"Error in search_messages: {e}")
        raise HTTPException(status_code=500, detail="Failed to search messages")

# Endpoint 4: Visual Content Stats
@app.get("/api/reports/visual-content", response_model=VisualContentResponse, tags=["Reports"])
async def get_visual_content_stats(
    include_details: bool = Query(default=True, description="Whether to include detailed detection stats"),
    min_confidence: float = Query(default=0.1, ge=0, le=1, description="Minimum confidence threshold for analysis")
):
    """
    Returns statistics about image usage across channels.
    
    This endpoint provides comprehensive analytics about visual content,
    including YOLO object detection results and engagement patterns.
    """
    try:
        params = {"min_confidence": min_confidence}
        
        # Get channel visual statistics
        channel_query = """
        SELECT 
            dc.channel_name,
            dc.total_posts,
            COUNT(img.message_id) as messages_with_images,
            ROUND(COUNT(img.message_id) * 100.0 / NULLIF(dc.total_posts, 0), 2) as image_percentage,
            COUNT(CASE WHEN img.calculated_category = 'promotional' THEN 1 END) as promotional_posts,
            COUNT(CASE WHEN img.calculated_category = 'product_display' THEN 1 END) as product_display_posts,
            COUNT(CASE WHEN img.calculated_category = 'lifestyle' THEN 1 END) as lifestyle_posts,
            COALESCE(AVG(img.max_confidence), 0) as avg_confidence
        FROM analytics.dim_channels dc
        LEFT JOIN analytics.fct_image_detections img ON dc.channel_key = img.channel_key
            AND img.max_confidence >= %(min_confidence)s
        GROUP BY dc.channel_name, dc.total_posts
        HAVING COUNT(img.message_id) > 0
        ORDER BY messages_with_images DESC
        """
        
        channel_results = execute_query_to_dataframe(channel_query, params)
        
        channel_stats = []
        for result in channel_results:
            channel_stats.append(ChannelVisualStats(
                channel_name=result['channel_name'],
                total_messages=result['total_posts'],
                messages_with_images=result['messages_with_images'],
                image_percentage=result['image_percentage'],
                promotional_posts=result['promotional_posts'],
                product_display_posts=result['product_display_posts'],
                lifestyle_posts=result['lifestyle_posts'],
                avg_confidence=round(result['avg_confidence'], 4)
            ))
        
        # Get overall summary
        summary_query = """
        SELECT 
            COUNT(*) as total_images_analyzed,
            COALESCE(AVG(max_confidence), 0) as avg_confidence_score
        FROM analytics.fct_image_detections
        WHERE max_confidence >= %(min_confidence)s
        """
        
        summary_results = execute_query_to_dataframe(summary_query, params)
        summary_data = summary_results[0] if summary_results else {}
        
        # Get category distribution
        category_query = """
        SELECT 
            calculated_category,
            COUNT(*) as count
        FROM analytics.fct_image_detections
        WHERE max_confidence >= %(min_confidence)s
        GROUP BY calculated_category
        ORDER BY count DESC
        """
        
        category_results = execute_query_to_dataframe(category_query, params)
        category_distribution = {result['calculated_category']: result['count'] for result in category_results}
        
        # Get top detected objects (if details requested)
        top_objects = []
        if include_details:
            objects_query = """
            SELECT 
                top_class,
                COUNT(*) as detection_count,
                AVG(top_confidence) as avg_confidence
            FROM analytics.fct_image_detections
            WHERE top_class IS NOT NULL 
            AND max_confidence >= %(min_confidence)s
            GROUP BY top_class
            ORDER BY detection_count DESC
            LIMIT 10
            """
            
            objects_results = execute_query_to_dataframe(objects_query, params)
            top_objects = [
                {
                    "object": result['top_class'],
                    "count": result['detection_count'],
                    "avg_confidence": round(result['avg_confidence'], 4)
                }
                for result in objects_results
            ]
        
        return VisualContentResponse(
            channel_stats=channel_stats,
            summary=VisualContentSummary(
                total_images_analyzed=summary_data.get('total_images_analyzed', 0),
                avg_confidence_score=round(summary_data.get('avg_confidence_score', 0), 4),
                top_detected_objects=top_objects
            ),
            category_distribution=category_distribution
        )
        
    except Exception as e:
        logger.error(f"Error in get_visual_content_stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve visual content statistics")

# Error handler
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    logger.error(f"Global exception: {exc}")
    return ErrorResponse(
        error_code="INTERNAL_SERVER_ERROR",
        error_detail=str(exc)
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)