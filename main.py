#!/usr/bin/env python3
"""
DOTD (Deal of the Day) Data Collector - Scheduled Loop Version
Fetches betting data from Real.vg API every 30 minutes within specified time window
"""

import requests
import json
import time
import signal
import sys
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
import os
from pathlib import Path
import pytz

class DOTDScheduler:
    def __init__(self, api_url: str, output_dir: str = "dotd_data"):
        self.api_url = api_url
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.running = True
        
        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        print(f"\n⚠️  Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    def _get_est_timezone(self):
        """Get EST timezone (handles EST/EDT automatically)"""
        return pytz.timezone('US/Eastern')
    
    def _parse_schedule_time(self, date_str: str, time_str: str) -> datetime:
        """Parse date and time strings into EST datetime
        
        Args:
            date_str: Date in format 'MM/DD' or 'M/D' (e.g., '8/11')
            time_str: Time in format 'H:MM AM/PM' (e.g., '6:00 PM', '3:00 AM')
        """
        est = self._get_est_timezone()
        
        # Parse date (assume current year)
        month, day = map(int, date_str.split('/'))
        current_year = datetime.now().year
        
        # Parse time
        time_str = time_str.upper().strip()
        if 'AM' in time_str or 'PM' in time_str:
            time_part = time_str.replace('AM', '').replace('PM', '').strip()
            hour, minute = map(int, time_part.split(':'))
            
            if 'PM' in time_str and hour != 12:
                hour += 12
            elif 'AM' in time_str and hour == 12:
                hour = 0
        else:
            # Assume 24-hour format if no AM/PM
            hour, minute = map(int, time_str.split(':'))
        
        # Create EST datetime
        dt = datetime(current_year, month, day, hour, minute)
        est_dt = est.localize(dt)
        
        return est_dt
    
    def _is_within_schedule(self, start_dt: datetime, end_dt: datetime) -> bool:
        """Check if current time is within the scheduled window"""
        est = self._get_est_timezone()
        current_time = datetime.now(est)
        
        # Handle overnight schedules (end time is next day)
        if end_dt < start_dt:
            # Schedule spans midnight
            return current_time >= start_dt or current_time <= end_dt
        else:
            # Normal schedule within same day
            return start_dt <= current_time <= end_dt
    
    def _get_next_run_time(self, interval_minutes: int = 30) -> datetime:
        """Calculate next run time (current time + interval)"""
        est = self._get_est_timezone()
        current_time = datetime.now(est)
        return current_time + timedelta(minutes=interval_minutes)
    
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
                'rank': option.get('priority', 0),
                'game_id': option.get('additionalInfo', {}).get('gameId'),
                'team_db_id': option.get('additionalInfo', {}).get('teamId'),
                'multiplier': option.get('multiplier', 1.0),
                'is_locked': option.get('isLocked', False),
                'game_lock_time': option.get('locksAt')
            }
            
            # Calculate derived features
            implied_prob = self._calculate_implied_probability(option.get('odds', '+100'))
            team_data['implied_win_probability'] = round(implied_prob, 4)
            team_data['rank_percentile'] = round(option.get('priority', 0) / len(poll_data['options']), 4)
            
            # Calculate hours until game starts
            if team_data['game_lock_time']:
                lock_time = self._parse_lock_time(team_data['game_lock_time'])
                if lock_time:
                    time_diff = lock_time - snapshot_time
                    hours_until_game = time_diff.total_seconds() / 3600
                    team_data['hours_until_game_starts'] = round(hours_until_game, 2)
            
            teams.append(team_data)
        
        # Sort teams by rank and calculate leader differentials
        teams.sort(key=lambda x: x['rank'])
        
        if teams:
            leader_votes = teams[0]['vote_count']
            leader_percentage = teams[0]['current_vote_percentage']
            
            for team in teams:
                team['votes_behind_leader'] = leader_votes - team['vote_count']
                team['percentage_behind_leader'] = round(leader_percentage - team['current_vote_percentage'], 2)
        
        return {
            'collection_info': {
                'api_url': self.api_url,
                'collection_time': snapshot_time.isoformat(),
                'data_format_version': '1.0'
            },
            'event_info': event_info,
            'teams': teams
        }
    
    def collect_single_snapshot(self) -> bool:
        """Collect a single data snapshot"""
        est = self._get_est_timezone()
        current_time = datetime.now(est)
        print(f"\n[{current_time.strftime('%Y-%m-%d %H:%M:%S EST')}] Collecting snapshot...")
        
        # Fetch and process data
        raw_data = self._fetch_api_data()
        if not raw_data:
            return False
        
        snapshot = self._process_snapshot(raw_data)
        if not snapshot:
            return False
        
        # Save data
        self._save_data(snapshot)
        
        # Print summary
        total_votes = snapshot['event_info']['total_votes_at_snapshot']
        teams_count = snapshot['event_info']['total_teams']
        leader = snapshot['teams'][0] if snapshot['teams'] else None
        
        print(f"✅ Snapshot complete: {teams_count} teams, {total_votes} votes")
        if leader:
            print(f"   🏆 Leader: {leader['team_identifier']} ({leader['current_vote_percentage']:.1f}%)")
        
        return True
    
    def run_scheduled_collection(self, start_date: str, start_time: str, 
                                end_date: str, end_time: str, 
                                interval_minutes: int = 30):
        """
        Run scheduled data collection
        
        Args:
            start_date: Start date in MM/DD format (e.g., '8/11')
            start_time: Start time in H:MM AM/PM format (e.g., '6:00 PM')
            end_date: End date in MM/DD format (e.g., '8/12')
            end_time: End time in H:MM AM/PM format (e.g., '3:00 AM')
            interval_minutes: Collection interval in minutes (default: 30)
        """
        est = self._get_est_timezone()
        
        # Parse start and end times
        start_dt = self._parse_schedule_time(start_date, start_time)
        end_dt = self._parse_schedule_time(end_date, end_time)
        
        print(f"🕐 DOTD Collector Scheduler Starting")
        print(f"📅 Schedule: {start_dt.strftime('%Y-%m-%d %H:%M:%S EST')} → {end_dt.strftime('%Y-%m-%d %H:%M:%S EST')}")
        print(f"⏱️  Collection interval: {interval_minutes} minutes")
        print(f"📁 Output directory: {self.output_dir}")
        print(f"🔗 API URL: {self.api_url}")
        print(f"⚠️  Press Ctrl+C to stop gracefully\n")
        
        collections_completed = 0
        
        while self.running:
            current_time = datetime.now(est)
            
            # Check if we're within the scheduled window
            if self._is_within_schedule(start_dt, end_dt):
                print(f"✅ Within schedule window - collecting data...")
                
                success = self.collect_single_snapshot()
                if success:
                    collections_completed += 1
                
                # Calculate next run time
                next_run = self._get_next_run_time(interval_minutes)
                
                # Check if next run would still be within schedule
                if self._is_within_schedule(start_dt, end_dt):
                    wait_seconds = (next_run - datetime.now(est)).total_seconds()
                    if wait_seconds > 0:
                        print(f"⏳ Next collection at {next_run.strftime('%H:%M:%S EST')} (waiting {wait_seconds:.0f} seconds)")
                        
                        # Sleep in small intervals to allow for graceful shutdown
                        while wait_seconds > 0 and self.running:
                            sleep_time = min(5, wait_seconds)  # Check every 5 seconds
                            time.sleep(sleep_time)
                            wait_seconds -= sleep_time
                    else:
                        print(f"⚡ Running immediately (next run time has passed)")
                else:
                    print(f"🏁 Schedule window will end before next collection - stopping")
                    break
                    
            else:
                # Outside schedule window
                if current_time < start_dt:
                    wait_seconds = (start_dt - current_time).total_seconds()
                    print(f"⏳ Waiting for schedule start: {start_dt.strftime('%Y-%m-%d %H:%M:%S EST')} ({wait_seconds:.0f} seconds)")
                    
                    # Sleep until start time
                    while wait_seconds > 0 and self.running and datetime.now(est) < start_dt:
                        sleep_time = min(60, wait_seconds)  # Check every minute
                        time.sleep(sleep_time)
                        wait_seconds = (start_dt - datetime.now(est)).total_seconds()
                        
                elif current_time > end_dt:
                    print(f"🏁 Schedule window ended at {end_dt.strftime('%Y-%m-%d %H:%M:%S EST')}")
                    break
                else:
                    # This shouldn't happen, but just in case
                    time.sleep(60)
        
        print(f"\n🎯 Collection session complete!")
        print(f"📊 Total snapshots collected: {collections_completed}")
        print(f"📁 Data saved in: {self.output_dir}")


def main():
    # Configuration - EASILY ADJUSTABLE
    API_URL = "https://api.real.vg/polls/270619"
    OUTPUT_DIR = "dotd_data"
    
    # Schedule Configuration (EASILY ADJUSTABLE)
    START_DATE = "8/11"    # MM/DD format
    START_TIME = "6:00 PM" # H:MM AM/PM format
    END_DATE = "8/12"      # MM/DD format  
    END_TIME = "3:00 AM"   # H:MM AM/PM format
    INTERVAL_MINUTES = 30  # Collection interval
    
    # Create scheduler and run
    scheduler = DOTDScheduler(API_URL, OUTPUT_DIR)
    
    try:
        scheduler.run_scheduled_collection(
            start_date=START_DATE,
            start_time=START_TIME, 
            end_date=END_DATE,
            end_time=END_TIME,
            interval_minutes=INTERVAL_MINUTES
        )
    except KeyboardInterrupt:
        print("\n👋 Gracefully shutting down...")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
    
    print("✅ Scheduler stopped.")


if __name__ == "__main__":
    main()
