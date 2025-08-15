#!/usr/bin/env python3
"""
DOTD (Deal of the Day) Data Collector - Single Snapshot Version
Fetches betting data from Real.vg API once and saves it
"""

import requests
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
import os
from pathlib import Path

class DOTDCollector:
    def __init__(self, api_url: str, output_dir: str = "dotd_data"):
        self.api_url = api_url
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
    def _save_data(self, snapshot_data: Dict[str, Any]):
        """Save snapshot to single master file with all snapshots"""
        try:
            single_filepath = self.output_dir / "dotd_all_snapshots.json"
            
            # Load existing data if file exists
            all_snapshots = []
            if single_filepath.exists():
                try:
                    with open(single_filepath, 'r') as f:
                        existing_data = json.load(f)
                        if isinstance(existing_data, list):
                            all_snapshots = existing_data
                        else:
                            # Convert old single snapshot to list
                            all_snapshots = [existing_data]
                except:
                    all_snapshots = []
            
            # Add new snapshot
            all_snapshots.append(snapshot_data)
            
            # Save updated file
            with open(single_filepath, 'w') as f:
                json.dump(all_snapshots, f, indent=2)
            print(f"✅ Updated: {single_filepath} (now contains {len(all_snapshots)} snapshots)")
                
        except Exception as e:
            print(f"❌ Error saving data: {e}")
    
    def _fetch_api_data(self) -> Dict[str, Any]:
        """Fetch data from the API"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json',
                'Accept-Language': 'en-US,en;q=0.9'
            }
            
            response = requests.get(self.api_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            return response.json()
        except Exception as e:
            print(f"❌ Error fetching API data: {e}")
            return None
    
    def _calculate_implied_probability(self, odds_str: str) -> float:
        """Convert American odds to implied probability"""
        try:
            odds = int(odds_str.replace('+', ''))
            if odds > 0:
                return 100 / (odds + 100)
            else:
                return abs(odds) / (abs(odds) + 100)
        except:
            return 0.0
    
    def _parse_lock_time(self, lock_time_str: str) -> datetime:
        """Parse the locksAt time string to datetime"""
        try:
            if '+' in lock_time_str or lock_time_str.endswith('Z'):
                return datetime.fromisoformat(lock_time_str.replace('Z', '+00:00'))
            else:
                return datetime.fromisoformat(lock_time_str)
        except:
            return None
    
    def _assign_ranks_by_votes(self, teams: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Assign ranks based on vote count with proper tie handling.
        Teams with higher vote counts get lower rank numbers (1 = best).
        """
        # Sort teams by vote count in descending order (highest votes first)
        teams_sorted = sorted(teams, key=lambda x: x['vote_count'], reverse=True)
        
        current_rank = 1
        for i, team in enumerate(teams_sorted):
            # Handle ties: if this team has same votes as previous, use same rank
            if i > 0 and team['vote_count'] == teams_sorted[i-1]['vote_count']:
                team['rank'] = teams_sorted[i-1]['rank']
            else:
                team['rank'] = current_rank
            
            # Calculate rank percentile (lower is better, so rank 1 = 0.0 percentile for best)
            team['rank_percentile'] = round((team['rank'] - 1) / len(teams_sorted), 4)
            
            # Update current_rank for next iteration (accounts for ties)
            current_rank = i + 2  # Next available rank after potential ties
        
        return teams_sorted
    
    def _process_snapshot(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process raw API data into structured snapshot"""
        if not raw_data:
            return None
        
        # Handle both nested and direct formats
        if 'poll' in raw_data:
            poll_data = raw_data.get('poll', {})
        else:
            poll_data = raw_data
        
        if not poll_data or 'options' not in poll_data:
            return None
            
        snapshot_time = datetime.now(timezone.utc)
        total_votes = sum(option.get('count', 0) for option in poll_data['options'])
        
        # Extract event-level info
        event_info = {
            'date': snapshot_time.date().isoformat(),
            'total_teams': len(poll_data['options']),
            'sport': poll_data.get('sport', 'unknown'),
            'snapshot_timestamp': snapshot_time.isoformat(),
            'poll_id': poll_data.get('id'),
            'is_daily_dog': poll_data.get('additionalInfo', {}).get('isDailyDog', False),
            'total_votes_at_snapshot': total_votes
        }
        
        # Find first game start time
        lock_times = []
        for option in poll_data['options']:
            lock_time = self._parse_lock_time(option.get('locksAt', ''))
            if lock_time:
                lock_times.append(lock_time)
        
        if lock_times:
            event_info['first_game_start_time'] = min(lock_times).isoformat()
        
        # Process team-level data
        teams = []
        for option in poll_data['options']:
            vote_count = option.get('count', 0)
            vote_percentage = (vote_count / total_votes * 100) if total_votes > 0 else 0
            
            team_data = {
                'team_id': option.get('id'),
                'team_identifier': option.get('label'),
                'american_odds': option.get('odds'),
                'current_vote_percentage': round(vote_percentage, 2),
                'vote_count': vote_count,
                'original_priority': option.get('priority', 0),  # Keep original for reference
                'game_id': option.get('additionalInfo', {}).get('gameId'),
                'team_db_id': option.get('additionalInfo', {}).get('teamId'),
                'multiplier': option.get('multiplier', 1.0),
                'is_locked': option.get('isLocked', False),
                'game_lock_time': option.get('locksAt')
            }
            
            # Calculate derived features
            implied_prob = self._calculate_implied_probability(option.get('odds', '+100'))
            team_data['implied_win_probability'] = round(implied_prob, 4)
            
            # Calculate hours until game starts
            if team_data['game_lock_time']:
                lock_time = self._parse_lock_time(team_data['game_lock_time'])
                if lock_time:
                    time_diff = lock_time - snapshot_time
                    hours_until_game = time_diff.total_seconds() / 3600
                    team_data['hours_until_game_starts'] = round(hours_until_game, 2)
            
            teams.append(team_data)
        
        # Assign ranks based on vote count (this also sorts the teams)
        teams = self._assign_ranks_by_votes(teams)
        
        # Calculate leader differentials (teams are now sorted by vote count, highest first)
        if teams:
            leader_votes = teams[0]['vote_count']  # Most votes (rank 1)
            leader_percentage = teams[0]['current_vote_percentage']
            
            for team in teams:
                team['votes_behind_leader'] = leader_votes - team['vote_count']
                team['percentage_behind_leader'] = round(leader_percentage - team['current_vote_percentage'], 2)
        
        return {
            'collection_info': {
                'api_url': self.api_url,
                'collection_time': snapshot_time.isoformat(),
                'data_format_version': '1.1'  # Incremented due to ranking fix
            },
            'event_info': event_info,
            'teams': teams  # teams is now sorted by vote count (highest to lowest) with proper ranks
        }
    
    def collect_snapshot(self) -> bool:
        """Collect a single data snapshot"""
        current_time = datetime.now()
        print(f"\n[{current_time.strftime('%Y-%m-%d %H:%M:%S')}] Collecting snapshot...")
        
        # Fetch and process data
        raw_data = self._fetch_api_data()
        if not raw_data:
            return False
        
        snapshot = self._process_snapshot(raw_data)
        if not snapshot:
            return False
        
        # Save data
        self._save_data(snapshot)
        
        # Print summary with ranking info
        total_votes = snapshot['event_info']['total_votes_at_snapshot']
        teams_count = snapshot['event_info']['total_teams']
        leader = snapshot['teams'][0] if snapshot['teams'] else None
        
        print(f"✅ Snapshot complete: {teams_count} teams, {total_votes} votes")
        if leader:
            print(f"   🏆 Leader (Rank {leader['rank']}): {leader['team_identifier']} ({leader['current_vote_percentage']:.1f}%, {leader['vote_count']} votes)")
        
        # Show top 3 for verification
        if len(snapshot['teams']) > 1:
            for i, team in enumerate(snapshot['teams'][:3]):
                print(f"   {i+1}. Rank {team['rank']}: {team['team_identifier']} ({team['vote_count']} votes, {team['current_vote_percentage']:.1f}%)")
        
        return True


def main():
    # Configuration - EASILY ADJUSTABLE
    API_URL = "https://api.real.vg/polls/270619"
    OUTPUT_DIR = "dotd_data"
    
    # Create collector and run single collection
    collector = DOTDCollector(API_URL, OUTPUT_DIR)
    
    print(f"🕐 DOTD Collector - Single Snapshot Mode")
    print(f"📁 Output directory: {OUTPUT_DIR}")
    print(f"🔗 API URL: {API_URL}\n")
    
    try:
        success = collector.collect_snapshot()
        if success:
            print(f"\n🎯 Collection complete!")
            print(f"📁 Data saved in: {OUTPUT_DIR}")
        else:
            print(f"\n❌ Collection failed!")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")


if __name__ == "__main__":
    main()
