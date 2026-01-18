#!/usr/bin/env python3
"""
Analyze image content patterns and generate insights
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/image_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ImagePatternAnalyzer:
    def __init__(self):
        """Initialize image pattern analyzer"""
        self.db_host = os.getenv('DB_HOST', 'localhost')
        self.db_port = os.getenv('DB_PORT', '5432')
        self.db_name = os.getenv('DB_NAME', 'medical_warehouse')
        self.db_user = os.getenv('DB_USER', 'postgres')
        self.db_password = os.getenv('DB_PASSWORD', 'password')
        
        self.results_path = Path('results')
        self.results_path.mkdir(exist_ok=True)
        
        # Create database connection
        self.connection_string = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        self.engine = create_engine(self.connection_string)
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute SQL query and return results"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query)).fetchall()
                columns = result[0]._fields if result else []
                df = pd.DataFrame([dict(row._mapping) for row in result], columns=columns)
            return df
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return pd.DataFrame()
    
    def analyze_promotional_vs_product_performance(self) -> Dict[str, Any]:
        """Analyze if promotional posts get more views than product_display posts"""
        
        query = """
        with image_engagement as (
            select 
                img.calculated_category,
                fm.view_count,
                fm.forward_count,
                fm.message_length,
                dc.channel_name
            from analytics.fct_image_detections img
            join analytics.fct_messages fm on img.message_id = fm.message_id
            join analytics.dim_channels dc on img.channel_key = dc.channel_key
            where img.calculated_category in ('promotional', 'product_display', 'lifestyle', 'other')
        ),
        category_stats as (
            select 
                calculated_category,
                count(*) as post_count,
                avg(view_count) as avg_views,
                median(view_count) as median_views,
                avg(forward_count) as avg_forwards,
                median(forward_count) as median_forwards,
                stddev(view_count) as views_stddev
            from image_engagement
            group by calculated_category
        )
        select * from category_stats order by avg_views desc;
        """
        
        df = self.execute_query(query)
        
        if df.empty:
            return {}
        
        # Convert to dictionary for JSON serialization
        result = {
            'analysis_type': 'promotional_vs_product_performance',
            'timestamp': datetime.now().isoformat(),
            'data': df.to_dict('records'),
            'key_insights': []
        }
        
        # Generate insights
        if len(df) > 1:
            promotional_views = df[df['calculated_category'] == 'promotional']['avg_views'].iloc[0] if 'promotional' in df['calculated_category'].values else 0
            product_views = df[df['calculated_category'] == 'product_display']['avg_views'].iloc[0] if 'product_display' in df['calculated_category'].values else 0
            
            if promotional_views > product_views:
                result['key_insights'].append(f"Promotional posts get {promotional_views/product_views:.1f}x more views than product-only posts")
            else:
                result['key_insights'].append(f"Product-only posts get {product_views/promotional_views:.1f}x more views than promotional posts")
        
        return result
    
    def analyze_channel_visual_content(self) -> Dict[str, Any]:
        """Analyze which channels use more visual content"""
        
        query = """
        with channel_visual_content as (
            select 
                dc.channel_name,
                dc.channel_type,
                count(*) as total_messages,
                count(img.message_id) as messages_with_images,
                count(case when img.calculated_category is not null then 1 end) as images_analyzed,
                round(count(img.message_id) * 100.0 / count(*), 2) as image_percentage
            from analytics.dim_channels dc
            left join analytics.fct_messages fm on dc.channel_key = fm.channel_key
            left join analytics.fct_image_detections img on fm.message_id = img.message_id
            group by dc.channel_name, dc.channel_type
        )
        select * from channel_visual_content order by image_percentage desc;
        """
        
        df = self.execute_query(query)
        
        if df.empty:
            return {}
        
        result = {
            'analysis_type': 'channel_visual_content',
            'timestamp': datetime.now().isoformat(),
            'data': df.to_dict('records'),
            'key_insights': []
        }
        
        # Generate insights
        if len(df) > 0:
            top_channel = df.iloc[0]
            result['key_insights'].append(f"{top_channel['channel_name']} has the highest visual content rate at {top_channel['image_percentage']}%")
            
            avg_visual_rate = df['image_percentage'].mean()
            high_visual_channels = df[df['image_percentage'] > avg_visual_rate]
            result['key_insights'].append(f"{len(high_visual_channels)} channels have above-average visual content usage")
        
        return result
    
    def analyze_detection_limitations(self) -> Dict[str, Any]:
        """Analyze limitations of using pre-trained models"""
        
        query = """
        with detection_quality as (
            select 
                img.confidence_level,
                img.detection_density,
                img.total_detections,
                img.max_confidence,
                img.avg_confidence,
                fm.view_count,
                dc.channel_name
            from analytics.fct_image_detections img
            join analytics.fct_messages fm on img.message_id = fm.message_id
            join analytics.dim_channels dc on img.channel_key = dc.channel_key
        ),
        quality_stats as (
            select 
                confidence_level,
                detection_density,
                count(*) as image_count,
                round(avg(total_detections), 2) as avg_detections,
                round(avg(max_confidence), 4) as avg_max_confidence,
                round(avg(avg_confidence), 4) as avg_confidence
            from detection_quality
            group by confidence_level, detection_density
        )
        select * from quality_stats order by confidence_level, detection_density;
        """
        
        df = self.execute_query(query)
        
        if df.empty:
            return {}
        
        result = {
            'analysis_type': 'detection_limitations',
            'timestamp': datetime.now().isoformat(),
            'data': df.to_dict('records'),
            'key_insights': [],
            'limitations': []
        }
        
        # Generate insights about limitations
        low_confidence_images = df[df['confidence_level'] == 'low_confidence']['image_count'].sum() if 'low_confidence' in df['confidence_level'].values else 0
        no_detection_images = df[df['detection_density'] == 'no_objects']['image_count'].sum() if 'no_objects' in df['detection_density'].values else 0
        total_images = df['image_count'].sum()
        
        if total_images > 0:
            low_conf_rate = (low_confidence_images / total_images) * 100
            no_detection_rate = (no_detection_images / total_images) * 100
            
            result['key_insights'].append(f"{low_conf_rate:.1f}% of images have low confidence detections")
            result['key_insights'].append(f"{no_detection_rate:.1f}% of images have no detected objects")
        
        # Document limitations
        result['limitations'] = [
            "Pre-trained YOLOv8 trained on COCO dataset may not recognize Ethiopian medical products",
            "Generic object classes (bottle, cup) may not accurately represent medical items",
            "Cultural context in medical imagery may affect detection accuracy",
            "Confidence scores may be lower for domain-specific images",
            "Model may miss small text or packaging details important for medical products"
        ]
        
        return result
    
    def generate_comprehensive_analysis(self) -> Dict[str, Any]:
        """Generate comprehensive image content analysis"""
        logger.info("Starting comprehensive image content analysis")
        
        analyses = {
            'promotional_performance': self.analyze_promotional_vs_product_performance(),
            'channel_visual_content': self.analyze_channel_visual_content(),
            'detection_limitations': self.analyze_detection_limitations()
        }
        
        # Generate summary
        summary = {
            'analysis_timestamp': datetime.now().isoformat(),
            'total_analyses': len([a for a in analyses.values() if a]),
            'key_findings': []
        }
        
        # Collect all key insights
        for analysis_name, analysis_data in analyses.items():
            if analysis_data and 'key_insights' in analysis_data:
                for insight in analysis_data['key_insights']:
                    summary['key_findings'].append({
                        'analysis': analysis_name,
                        'insight': insight
                    })
        
        result = {
            'summary': summary,
            'analyses': analyses
        }
        
        # Save results
        results_file = self.results_path / 'image_pattern_analysis.json'
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Analysis results saved to {results_file}")
        
        return result
    
    def create_summary_report(self) -> str:
        """Create a text summary report"""
        analysis = self.generate_comprehensive_analysis()
        
        report = []
        report.append("=" * 80)
        report.append("IMAGE CONTENT ANALYSIS REPORT")
        report.append("Ethiopian Medical Business Data Platform")
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("=" * 80)
        report.append("")
        
        # Key findings
        report.append("KEY FINDINGS")
        report.append("-" * 40)
        
        if analysis['summary']['key_findings']:
            for finding in analysis['summary']['key_findings']:
                report.append(f"• {finding['insight']}")
        else:
            report.append("No key findings available.")
        
        report.append("")
        
        # Promotional vs Product Performance
        if analysis['analyses'].get('promotional_performance'):
            report.append("PROMOTIONAL VS PRODUCT DISPLAY PERFORMANCE")
            report.append("-" * 50)
            
            perf_data = analysis['analyses']['promotional_performance']['data']
            for record in perf_data:
                report.append(f"{record['calculated_category'].title()}:")
                report.append(f"  Posts: {record['post_count']}")
                report.append(f"  Avg Views: {record['avg_views']:.1f}")
                report.append(f"  Avg Forwards: {record['avg_forwards']:.1f}")
                report.append("")
        
        # Channel Visual Content
        if analysis['analyses'].get('channel_visual_content'):
            report.append("CHANNEL VISUAL CONTENT USAGE")
            report.append("-" * 35)
            
            channel_data = analysis['analyses']['channel_visual_content']['data']
            for record in channel_data:
                report.append(f"{record['channel_name']}:")
                report.append(f"  Total Messages: {record['total_messages']}")
                report.append(f"  Images: {record['messages_with_images']}")
                report.append(f"  Visual Rate: {record['image_percentage']}%")
                report.append("")
        
        # Model Limitations
        if analysis['analyses'].get('detection_limitations'):
            report.append("PRE-TRAINED MODEL LIMITATIONS")
            report.append("-" * 35)
            
            limitations = analysis['analyses']['detection_limitations']['limitations']
            for limitation in limitations:
                report.append(f"• {limitation}")
            report.append("")
        
        report.append("=" * 80)
        report.append("End of Report")
        report.append("=" * 80)
        
        report_text = "\n".join(report)
        
        # Save report
        report_file = self.results_path / 'image_analysis_report.txt'
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_text)
        
        logger.info(f"Summary report saved to {report_file}")
        
        return report_text

def main():
    """Main function"""
    analyzer = ImagePatternAnalyzer()
    
    try:
        # Generate comprehensive analysis
        analysis = analyzer.generate_comprehensive_analysis()
        
        # Create summary report
        report = analyzer.create_summary_report()
        
        print("\n=== Image Content Analysis Complete ===")
        print(f"Key findings: {len(analysis['summary']['key_findings'])}")
        print(f"Reports saved to: results/")
        
        # Print key insights
        if analysis['summary']['key_findings']:
            print("\nKey Insights:")
            for finding in analysis['summary']['key_findings'][:3]:  # Top 3
                print(f"• {finding['insight']}")
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
