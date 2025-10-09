from pathlib import Path
from mcp.server.fastmcp import FastMCP
from fastapi import FastAPI
from fastapi.responses import FileResponse
from databricks.sdk.core import Config
from .prompts import load_prompts
from .services.sql_service import get_sql_service, QueryFormatter
from .services.query_repository import get_query_repository
from .services.waf_hierarchy_service import get_waf_hierarchy_service
import os
from dotenv import load_dotenv
from fastapi import Header
from typing import Optional, Annotated
from pydantic import Field


# Load environment variables from .env file
load_dotenv() 

cfg = Config()

user_token = Header(None, alias="X-Forwarded-Access-Token")

# Initialize services
print("🔧 Starting service initialization...")

try:
    print("🔍 Initializing SQL service...")
    sql_service = get_sql_service()
    print("✓ SQL service initialized successfully")
except Exception as e:
    print(f"✗ SQL service initialization failed: {e}")
    print(f"   Error type: {type(e).__name__}")
    sql_service = None

try:
    print("🔍 Initializing query repository...")
    query_repo = get_query_repository()
    print("✓ Query repository initialized successfully")
except Exception as e:
    print(f"✗ Query repository initialization failed: {e}")
    print(f"   Error type: {type(e).__name__}")
    query_repo = None

try:
    print("🔍 Initializing WAF service...")
    waf_service = get_waf_hierarchy_service()
    print("✓ WAF service initialized successfully")
    stats = waf_service.get_stats()
    print(f"  - Loaded {stats['total_measures']} measures, {stats['total_principles']} principles, {stats['total_pillars']} pillars, {stats['total_analyses']} analyses")
except Exception as e:
    print(f"✗ WAF service initialization failed: {e}")
    print(f"   Error type: {type(e).__name__}")
    import traceback
    print(f"   Traceback: {traceback.format_exc()}")
    waf_service = None

print("🏁 Service initialization complete")

STATIC_DIR = Path(__file__).parent / "static"

# Create an MCP server
mcp = FastMCP("Custom MCP Server on Databricks Apps for creating")

# Load prompts and tools
load_prompts(mcp)

# WAF (Well-Architected Framework) Tools

# @mcp.tool()
# def test_simple() -> str:
#     """
#     Simple test tool to verify MCP is working.
    
#     Returns:
#         A simple test message.
#     """
#     print("🧪 test_simple called")
#     return "Test successful - MCP framework is working!"



@mcp.tool(
    name="get-waf-pillar",
    description="Retrieve comprehensive information about a specific Databricks Well-Architected Framework pillar, including all its principles and measures. Use this to understand a pillar's scope and navigate to specific principles within it. Examples: 'CO' (Cost Optimization), 'DG' (Data & AI Governance), 'RE' (Reliability)."
)
def get_waf_pillar(
    pillar_id: Annotated[str, Field(description="The WAF pillar ID to retrieve. Examples: 'CO', 'DG', 'RE', 'SE', 'PE', 'OE', 'IU'")]
) -> str:
    """
    Get information about a specific WAF pillar by ID.
    
    Args:
        pillar_id: The WAF pillar ID (e.g., 'DG', 'CO', 'RE', 'SE', 'PE', 'SU', 'IU', 'OE')
    
    Returns:
        Pillar information with associated principles and measures count.
    """
    pillar = waf_service.get_pillar(pillar_id)
    
    if not pillar:
        available_pillars = [p.pillar_id for p in waf_service.get_all_pillars()]
        return f"WAF pillar '{pillar_id}' not found. Available pillar IDs: {', '.join(available_pillars)}"
    
    principles = waf_service.get_principles_by_pillar(pillar_id)
    measures = waf_service.get_measures_by_pillar(pillar_id)
    
    result = f"# WAF Pillar: {pillar.pillar_id}\n\n"
    result += f"**Name:** {pillar.pillar_name}\n\n"
    result += f"**Description:** {pillar.pillar_description}\n\n"
    result += f"**Principles:** {len(principles)}\n"
    result += f"**Measures:** {len(measures)}\n\n"
    
    if principles:
        result += "**Principles in this pillar:**\n"
        for principle in principles:
            principle_measures = waf_service.get_measures_by_principle(principle.principle_id)
            result += f"- **{principle.principle_id}**: {principle.principle_description} ({len(principle_measures)} measures)\n"
    
    result += f"\nUse **Get WAF Principle** to explore specific principles."
    
    return result


@mcp.tool(
    name="get-waf-principle",
    description="Get complete information about a specific WAF principle including all associated measures, their best practices, Databricks capabilities, implementation details, and available analyses. This is the primary tool for understanding what measures exist within a principle and which analyses can be run. Example IDs: 'CO-01' (Choose optimal resources), 'DG-01' (Manage data lifecycle), 'RE-01' (Design for reliability)."
)
def get_waf_principle(
    principle_id: Annotated[str, Field(description="The WAF principle ID to retrieve. Format: '{PILLAR}-{NUMBER}' (e.g., 'CO-01', 'DG-01', 'RE-01')")]
) -> str:
    """
    Get comprehensive information about a specific WAF principle including all measures and analyses.
    
    This provides the principle description, all associated measures with their best practices,
    capabilities, implementation details, and available analyses.
    
    Args:
        principle_id: The WAF principle ID (e.g., 'DG-01', 'CO-01', 'RE-01')
    
    Returns:
        Complete principle information with all measures and their available analyses.
    """
    principle = waf_service.get_principle(principle_id)
    
    if not principle:
        return f"WAF principle '{principle_id}' not found. Use **List WAF Principles** to see all available principles."
    
    measures = waf_service.get_measures_by_principle(principle_id)
    
    # Build result efficiently using a list
    parts = [
        f"# WAF Principle: {principle.principle_id}\n\n",
        f"**Pillar:** {principle.pillar_name}\n",
        f"**Description:** {principle.principle_description}\n\n",
        f"**Total Measures:** {len(measures)}\n\n"
    ]
    
    if measures:
        parts.append("## 📋 Measures\n\n")
        for measure in measures:
            parts.append(f"### {measure.measure_id}: {measure.best_practice}\n\n")
            
            if measure.databricks_capabilities:
                parts.append(f"**Databricks Capabilities:** {measure.databricks_capabilities}\n\n")
            
            parts.append(f"**Details:** {measure.details}\n\n")
            
            # Get analyses for this measure
            analyses = waf_service.get_analyses_for_measure(measure.measure_id)
            
            if analyses:
                parts.append(f"**Available Analyses ({len(analyses)}):**\n")
                for analysis in analyses:
                    parts.append(f"- **{analysis.analysis_id}**")
                    if analysis.sql_description:
                        parts.append(f": {analysis.sql_description}")
                    parts.append("\n")
                parts.append("\n")
            else:
                parts.append("*No automated analyses available for this measure.*\n\n")
    
    parts.append("Use **Run WAF Analysis** to execute any specific analysis.")
    
    return "".join(parts)


@mcp.tool(
    name="run-waf-analysis",
    description="Execute a specific WAF analysis to evaluate the current state of the Databricks workspace against a best practice measure. This queries system tables and returns actual workspace data (e.g., table formats, cluster configurations, costs). Each analysis has an ID like 'CO-01-01A' or 'CO-01-01B' and tests a specific aspect of a measure. Use get-waf-principle first to see available analysis IDs."
)
def run_waf_analysis(
    analysis_id: Annotated[str, Field(description="The WAF analysis ID to execute. Format: '{PILLAR}-{PRINCIPLE}-{MEASURE}{LETTER}' (e.g., 'CO-01-01A', 'CO-01-01B', 'DG-01-02')")]
) -> str:
    """
    Execute a specific WAF analysis query to evaluate your Databricks workspace.
    
    This runs the SQL analysis and returns the results with context about what is being measured
    and how to interpret the findings in relation to WAF best practices.
    
    Args:
        analysis_id: The WAF analysis ID (e.g., 'CO-01-01A', 'CO-01-01B', 'CO-03-02A')
    
    Returns:
        Analysis results with interpretation and context.
    """
    print(f"🚀 run_waf_analysis called with analysis_id: {analysis_id}")
    
    try:
        if waf_service is None:
            return "Error: WAF service failed to initialize."
        
        if sql_service is None:
            return "Error: SQL service failed to initialize."
        
        # Get the analysis
        analysis = waf_service.get_analysis(analysis_id)
        
        if not analysis:
            return f"WAF analysis '{analysis_id}' not found. Use **Get WAF Principle** to see available analyses for measures."
        
        # Build the response
        result = f"# Analysis {analysis.analysis_id}\n\n"
        
        if analysis.sql_description:
            result += f"**What This Measures:** {analysis.sql_description}\n\n"
        
        # Execute the SQL query and return results
        result += f"## Current Workspace Data\n\n"
        
        try:
            print(f"🔍 Executing analysis {analysis.analysis_id}...")
            query_result = sql_service.execute_query_with_formatting(analysis.sql_code)
            print(f"✅ Analysis executed successfully")
            
            result += query_result
            
        except Exception as e:
            print(f"❌ Analysis execution failed: {str(e)}")
            result += f"**Error executing analysis:** {str(e)}\n"
        
        return result
        
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
        return f"Error running analysis {analysis_id}: {str(e)}"


@mcp.tool(
    name="list-waf-pillars",
    description="List all seven Databricks Well-Architected Framework pillars with a count of their principles and measures. Use this as a starting point to explore the WAF framework structure. Pillars include: Cost Optimization, Data & AI Governance, Reliability, Security, Performance Efficiency, Operational Excellence, and Interoperability & Usability."
)
def list_waf_pillars() -> str:
    """
    List all WAF pillars with their principles and measures count.
    
    Returns:
        A comprehensive overview of all WAF pillars.
    """
    pillars = waf_service.get_all_pillars()
    stats = waf_service.get_stats()
    
    result = "**Databricks Well-Architected Framework Pillars:**\n\n"
    
    for pillar in pillars:
        principles = waf_service.get_principles_by_pillar(pillar.pillar_id)
        measures = waf_service.get_measures_by_pillar(pillar.pillar_id)
        
        result += f"**{pillar.pillar_id}** - {pillar.pillar_name}\n"
        # Truncate description to first 150 chars for overview
        desc_preview = pillar.pillar_description[:150] + "..." if len(pillar.pillar_description) > 150 else pillar.pillar_description
        result += f"  {desc_preview}\n"
        result += f"  *{len(principles)} principles, {len(measures)} measures*\n\n"
    
    result += f"**Total:** {stats['total_pillars']} pillars, {stats['total_principles']} principles, {stats['total_measures']} measures\n\n"
    result += "Use **Get WAF Pillar** to explore any pillar in detail."
    
    print("DEBUG: ", result)
    
    return result


@mcp.tool(
    name="list-waf-principles",
    description="List all WAF principles organized by pillar, showing their descriptions and measure counts. Use this to understand the architectural guidelines within each pillar and identify which principles to explore in detail. Each principle contains multiple specific measures and analyses."
)
def list_waf_principles() -> str:
    """
    List all WAF principles organized by pillar.
    
    Returns:
        A comprehensive list of all WAF principles grouped by pillar.
    """
    pillars = waf_service.get_all_pillars()
    
    result = "**WAF Principles by Pillar:**\n\n"
    
    for pillar in pillars:
        principles = waf_service.get_principles_by_pillar(pillar.pillar_id)
        
        result += f"**{pillar.pillar_name}:**\n"
        for principle in principles:
            measures = waf_service.get_measures_by_principle(principle.principle_id)
            result += f"  - **{principle.principle_id}**: {principle.principle_description} ({len(measures)} measures)\n"
        result += "\n"
    
    result += "Use **Get WAF Principle** to explore any principle in detail."
    
    return result


@mcp.tool(
    name="list-waf-measures-with-analyses",
    description="List all WAF measures that have automated SQL analyses available for workspace evaluation. Shows the specific analysis IDs (e.g., 'CO-01-01A', 'CO-01-01B') that can be executed with run-waf-analysis. Use this to discover which aspects of your workspace can be analyzed programmatically. Currently covers ~10 measures across Cost Optimization and other pillars."
)
def list_waf_measures_with_analyses() -> str:
    """
    List all WAF measures that have associated analyses for data evaluation.
    
    Returns:
        A list of measures that include automated analyses for assessing your Databricks environment.
    """
    measures_with_analyses = waf_service.get_measures_with_analyses()
    
    if not measures_with_analyses:
        return "No WAF measures found with automated analyses."
    
    result = f"**WAF Measures with Automated Analyses ({len(measures_with_analyses)} found):**\n\n"
    
    # Group by pillar for better organization
    by_pillar = {}
    for measure in measures_with_analyses:
        if measure.pillar_id not in by_pillar:
            by_pillar[measure.pillar_id] = []
        by_pillar[measure.pillar_id].append(measure)
    
    pillar_names = {
        'CO': 'Cost Optimization',
        'DG': 'Data & AI Governance', 
        'RE': 'Reliability',
        'SE': 'Security',
        'PE': 'Performance Efficiency',
        'OE': 'Operational Excellence',
        'IU': 'Interoperability & Usability'
    }
    
    for pillar_id, measures in sorted(by_pillar.items()):
        pillar_name = pillar_names.get(pillar_id, pillar_id)
        result += f"**{pillar_name} ({pillar_id}):**\n"
        
        for measure in sorted(measures, key=lambda m: m.measure_id):
            analyses = waf_service.get_analyses_for_measure(measure.measure_id)
            result += f"  - **{measure.measure_id}**: {measure.best_practice}\n"
            result += f"    Analyses: "
            result += ", ".join([f"`{a.analysis_id}`" for a in analyses])
            result += "\n"
        result += "\n"
    
    result += f"Use **Get WAF Principle** to see measure details, then **Run WAF Analysis** to execute specific analyses."
    
    return result


mcp_app = mcp.streamable_http_app()

app = FastAPI(
    lifespan=lambda _: mcp.session_manager.run(),
)


@app.get("/", include_in_schema=False)
async def serve_index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/", mcp_app)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "custom_server.app:app",
        host="0.0.0.0", 
        port=8000,
        reload=True
    )
