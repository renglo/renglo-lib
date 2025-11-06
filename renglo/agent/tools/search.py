from typing import Dict, Any, List
from pathlib import Path
from ..agent_tools import BaseTool, ToolResult, ToolParams

'''
## SEARCH TOOL

### ADD TO MANIFEST (In the source code)

To use this tool, add it to the manifest (path/to/manifest.json):
{
    "tools": [
        {
            "name": "search_documents",
            "module": "renglo_lib.app_agent.tools.search",
            "class": "SearchTool",
            "config": {
                "index_path": "/path/to/search/index",
                "max_results": 20
            }
        }
    ]
}


### REGISTRATION (During system initialization)

# Method 1: Register using manifest
registry = ToolRegistry()
register_all_tools(registry, manifest_path="path/to/manifest.json")

# Method 2: Register directly
search_tool = SearchTool(
    index_path="/path/to/search/index",
    max_results=20
)
registry.register_tool("search_documents", search_tool)


### USAGE (During runtime)

# Use the tool
result = registry.execute_tool("search_documents", {
    "query": "machine learning",
    "max_results": 5,
    "search_type": "semantic"
})

if result.success:
    for doc in result.result["results"]:
        print(f"Found: {doc['title']} (score: {doc['score']})")
else:
    print(f"Search failed: {result.error}")

'''


class SearchParams(ToolParams):
    """Parameters for the search tool"""
    query: str
    max_results: int = 10
    search_type: str = "semantic"  # semantic, exact, or fuzzy

class SearchTool(BaseTool):
    """A tool for searching through documents"""
    
    def __init__(self, index_path: str, max_results: int = 10):
        """Initialize the search tool
        
        Args:
            index_path: Path to the search index
            max_results: Maximum number of results to return
        """
        super().__init__()
        self.index_path = Path(index_path)
        self.max_results = max_results
        
        # Validate index path exists
        if not self.index_path.exists():
            raise ValueError(f"Index path does not exist: {index_path}")
    
    def execute(self, params: Dict[str, Any]) -> ToolResult:
        """Execute the search
        
        Args:
            params: Dictionary containing:
                - query: The search query
                - max_results: Maximum number of results (optional)
                - search_type: Type of search to perform (optional)
        
        Returns:
            ToolResult containing the search results
        """
        try:
            # Validate parameters
            search_params = SearchParams(**params)
            
            # Perform the search
            results = self._perform_search(
                query=search_params.query,
                max_results=min(search_params.max_results, self.max_results),
                search_type=search_params.search_type
            )
            
            return ToolResult(
                success=True,
                result={
                    "results": results,
                    "total": len(results),
                    "query": search_params.query
                }
            )
            
        except Exception as e:
            return ToolResult(
                success=False,
                result=None,
                error=str(e)
            )
    
    def validate_params(self, params: Dict[str, Any]) -> bool:
        """Validate the search parameters"""
        try:
            SearchParams(**params)
            return True
        except Exception:
            return False
    
    def _perform_search(self, query: str, max_results: int, search_type: str) -> List[Dict[str, Any]]:
        """Perform the actual search
        
        This is a placeholder implementation. In a real tool, this would:
        1. Load the search index
        2. Perform the search based on search_type
        3. Return the results
        
        Args:
            query: The search query
            max_results: Maximum number of results to return
            search_type: Type of search to perform
        
        Returns:
            List of search results
        """
        # Placeholder implementation
        return [
            {
                "title": f"Document {i}",
                "content": f"Content matching '{query}'",
                "score": 1.0 - (i * 0.1)
            }
            for i in range(min(max_results, 5))
        ] 