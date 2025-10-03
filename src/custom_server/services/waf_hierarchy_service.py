"""Comprehensive WAF (Well-Architected Framework) service for managing hierarchical WAF data."""

import csv
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional, Set
from dataclasses import dataclass


@dataclass
class WAFPillar:
    """Represents a WAF pillar."""
    pillar_id: str
    pillar_name: str


@dataclass
class WAFPrinciple:
    """Represents a WAF principle."""
    principle_id: str
    pillar_id: str
    pillar_name: str
    principle_description: str


@dataclass
class WAFMeasure:
    """Represents a WAF measure."""
    pillar_id: str
    principle_id: str
    measure_id: str
    best_practice: str
    databricks_capabilities: str
    details: str


class WAFHierarchyService:
    """Service for managing hierarchical WAF data (Pillars → Principles → Measures)."""
    
    def __init__(self):
        self._pillars: Dict[str, WAFPillar] = {}
        self._principles: Dict[str, WAFPrinciple] = {}
        self._measures: Dict[str, WAFMeasure] = {}
        self._load_waf_data()
    
    def _get_csv_path(self, filename: str) -> Path:
        """Get the path to a CSV file, handling both deployment and development environments."""
        # Deployment path (resources copied to same directory as app)
        csv_path = Path(f"resources/{filename}")
        
        if not csv_path.exists():
            # Development fallback
            csv_path = Path(__file__).parent.parent.parent.parent / "resources" / filename
            
        if not csv_path.exists():
            raise FileNotFoundError(f"WAF CSV file not found: {filename}")
            
        return csv_path
    
    def _load_waf_data(self):
        """Load all WAF data from CSV files."""
        try:
            # Load pillars
            pillars_path = self._get_csv_path("wafe-life-assessments - pillars.csv")
            with open(pillars_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                pillar_ids_seen = set()
                for row in reader:
                    pillar_id = row['pillar_id'].strip()
                    if pillar_id and pillar_id not in pillar_ids_seen:
                        pillar = WAFPillar(
                            pillar_id=pillar_id,
                            pillar_name=row['pillar_name'].strip()
                        )
                        self._pillars[pillar_id] = pillar
                        pillar_ids_seen.add(pillar_id)
            
            # Load principles
            principles_path = self._get_csv_path("wafe-life-assessments - principles.csv")
            with open(principles_path, 'r', encoding='utf-8') as file:
                # Skip the first empty line
                lines = file.readlines()
                if lines and lines[0].strip() == ',,,':
                    lines = lines[1:]  # Skip the empty first line
                
                # Create a new file-like object from the remaining lines
                from io import StringIO
                csv_content = StringIO(''.join(lines))
                reader = csv.DictReader(csv_content)
                
                for row in reader:
                    # Skip empty rows or rows without principle_id
                    if not row.get('principle_id') or not row['principle_id'].strip():
                        continue
                    principle_id = row['principle_id'].strip()
                    principle = WAFPrinciple(
                        principle_id=principle_id,
                        pillar_id=row['pillar_id'].strip(),
                        pillar_name=row['pillar_name'].strip(),
                        principle_description=row['principle_description'].strip()
                    )
                    self._principles[principle_id] = principle
            
            # Load measures
            measures_path = self._get_csv_path("wafe-life-assessments - measures.csv")
            with open(measures_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    # Skip empty rows or rows without measure_id
                    if not row.get('measure_id') or not row['measure_id'].strip():
                        continue
                    measure_id = row['measure_id'].strip()
                    measure = WAFMeasure(
                        pillar_id=row['pillar_id'].strip(),
                        principle_id=row['principle_id'].strip(),
                        measure_id=measure_id,
                        best_practice=row['best_practice'].strip(),
                        databricks_capabilities=row['measure_databricks_capabilities'].strip(),
                        details=row['measure_details'].strip()
                    )
                    self._measures[measure_id] = measure
                        
        except Exception as e:
            raise RuntimeError(f"Error loading WAF data: {str(e)}")
    
    # Pillar methods
    def get_all_pillars(self) -> List[WAFPillar]:
        """Get all WAF pillars."""
        return sorted(list(self._pillars.values()), key=lambda p: p.pillar_id)
    
    def get_pillar(self, pillar_id: str) -> Optional[WAFPillar]:
        """Get a specific pillar by ID."""
        return self._pillars.get(pillar_id.upper())
    
    # Principle methods
    def get_all_principles(self) -> List[WAFPrinciple]:
        """Get all WAF principles."""
        return sorted(list(self._principles.values()), key=lambda p: p.principle_id)
    
    def get_principle(self, principle_id: str) -> Optional[WAFPrinciple]:
        """Get a specific principle by ID."""
        return self._principles.get(principle_id.upper())
    
    def get_principles_by_pillar(self, pillar_id: str) -> List[WAFPrinciple]:
        """Get all principles for a specific pillar."""
        pillar_id = pillar_id.upper()
        return [p for p in self._principles.values() if p.pillar_id == pillar_id]
    
    # Measure methods
    def get_all_measures(self) -> List[WAFMeasure]:
        """Get all WAF measures."""
        return sorted(list(self._measures.values()), key=lambda m: m.measure_id)
    
    def get_measure(self, measure_id: str) -> Optional[WAFMeasure]:
        """Get a specific measure by ID."""
        return self._measures.get(measure_id.upper())
    
    def get_measures_by_pillar(self, pillar_id: str) -> List[WAFMeasure]:
        """Get all measures for a specific pillar."""
        pillar_id = pillar_id.upper()
        return [m for m in self._measures.values() if m.pillar_id == pillar_id]
    
    def get_measures_by_principle(self, principle_id: str) -> List[WAFMeasure]:
        """Get all measures for a specific principle."""
        principle_id = principle_id.upper()
        return [m for m in self._measures.values() if m.principle_id == principle_id]
    
    # Search methods
    def search_measures(self, search_term: str) -> List[WAFMeasure]:
        """Search for measures containing the search term."""
        search_term = search_term.lower()
        results = []
        
        for measure in self._measures.values():
            if (search_term in measure.measure_id.lower() or
                search_term in measure.best_practice.lower() or
                search_term in measure.databricks_capabilities.lower() or
                search_term in measure.details.lower()):
                results.append(measure)
        
        return sorted(results, key=lambda m: m.measure_id)
    
    def search_principles(self, search_term: str) -> List[WAFPrinciple]:
        """Search for principles containing the search term."""
        search_term = search_term.lower()
        results = []
        
        for principle in self._principles.values():
            if (search_term in principle.principle_id.lower() or
                search_term in principle.principle_description.lower()):
                results.append(principle)
        
        return sorted(results, key=lambda p: p.principle_id)
    
    # Statistics methods
    def get_stats(self) -> Dict[str, int]:
        """Get WAF hierarchy statistics."""
        return {
            'total_pillars': len(self._pillars),
            'total_principles': len(self._principles),
            'total_measures': len(self._measures)
        }


# Global instance
_waf_hierarchy_service = None


def get_waf_hierarchy_service() -> WAFHierarchyService:
    """Get the global WAF hierarchy service instance."""
    global _waf_hierarchy_service
    if _waf_hierarchy_service is None:
        _waf_hierarchy_service = WAFHierarchyService()
    return _waf_hierarchy_service
